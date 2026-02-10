//! Stress test: N weavers (P producers, rest receivers), M spools, run for T seconds then assert
//! all CRDT documents converge. Phases: setup → join → epoch sync → stress run → flush remaining
//! → drain → converge (all same text) → assert.

use std::time::{Duration, Instant};

use filament_core::Epoch;
use filament_testing::{
    TestAddressLookup, YrsCrdt, init_tracing, spawn_acceptor, test_endpoint, test_yrs_weaver_client,
};
use filament_weave::{Weaver, WeaverEvent};
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::task::coop;
use tokio_util::sync::CancellationToken;
use yrs::{GetString, Text, Transact};

const NUM_WEAVERS: usize = 10;
const NUM_PRODUCERS: usize = 5;
const NUM_SPOOLS: usize = 3;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGENCE_CONSECUTIVE_MATCHES: u32 = 3;
const DRAIN_PASSES_PER_ROUND: usize = 10;
const PER_WEAVER_WAIT_MS: u64 = 50;
const IDLE_ROUNDS_BEFORE_CONVERGE: u32 = 3;
const PROPAGATION_SLEEP_AFTER_FLUSH: Duration = Duration::from_secs(2);

type WeaverPair = (Weaver, YrsCrdt);

fn stress_duration() -> Duration {
    std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(2))
}

fn doc_text(crdt: &YrsCrdt) -> String {
    let text = crdt.doc().get_or_insert_text("doc");
    let txn = crdt.doc().transact();
    text.get_string(&txn)
}

fn random_edit(crdt: &YrsCrdt, rng: &mut SmallRng) {
    let text = crdt.doc().get_or_insert_text("doc");
    let len = {
        let txn = crdt.doc().transact();
        text.get_string(&txn).len() as u32
    };
    let insert = len == 0 || rng.random_ratio(3, 5);
    let mut txn = crdt.doc().transact_mut();
    if insert {
        let pos = if len == 0 {
            0
        } else {
            rng.random_range(0..=len)
        };
        let ch = rng.random_range(b'!'..=b'~') as char;
        text.insert(&mut txn, pos, &ch.to_string());
    } else {
        let pos = rng.random_range(0..len);
        text.remove_range(&mut txn, pos, 1);
    }
}

async fn drain_compaction(
    group: &mut Weaver,
    crdt: &mut YrsCrdt,
    events: &mut tokio::sync::broadcast::Receiver<WeaverEvent>,
) {
    while let Ok(WeaverEvent::CompactionNeeded { level, force }) = events.try_recv() {
        if force {
            group
                .force_compact(crdt, level)
                .await
                .expect("force_compact");
        } else {
            group.compact(crdt, level).await.expect("compact");
        }
    }
}

async fn drain_all(weavers: &mut [WeaverPair]) {
    for (group, crdt) in weavers {
        while group.sync(crdt) {
            coop::consume_budget().await;
        }
        coop::consume_budget().await;
    }
}

async fn drain_until_idle(weavers: &mut [WeaverPair], wait_ms: u64, idle_rounds: u32) {
    let mut rounds_idle = 0u32;
    while rounds_idle < idle_rounds {
        let mut any = false;
        for (group, crdt) in weavers.iter_mut() {
            let _ =
                tokio::time::timeout(Duration::from_millis(wait_ms), group.wait_for_update(crdt))
                    .await;
            while group.sync(crdt) {
                any = true;
                coop::consume_budget().await;
            }
        }
        rounds_idle = if any { 0 } else { rounds_idle + 1 };
    }
}

async fn wait_for_epoch(weavers: &mut [WeaverPair], target_epoch: Epoch, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    for (group, _) in weavers.iter_mut() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("epoch sync timeout");
        }
        if group.context().await.expect("context").epoch >= target_epoch {
            continue;
        }
        let mut events = group.subscribe();
        tokio::time::timeout(remaining, async {
            loop {
                match events.recv().await {
                    Ok(WeaverEvent::EpochAdvanced { epoch }) if epoch >= target_epoch.0 => return,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("event channel closed")
                    }
                }
            }
        })
        .await
        .expect("epoch sync timeout");
    }
}

async fn run_convergence_loop(weavers: &mut [WeaverPair]) -> Option<usize> {
    let mut consecutive_matched = 0u32;
    loop {
        for _ in 0..DRAIN_PASSES_PER_ROUND {
            for (group, crdt) in weavers.iter_mut() {
                let _ = tokio::time::timeout(
                    Duration::from_millis(PER_WEAVER_WAIT_MS),
                    group.wait_for_update(crdt),
                )
                .await;
                while group.sync(crdt) {
                    coop::consume_budget().await;
                }
            }
        }
        let texts: Vec<String> = weavers.iter().map(|(_, c)| doc_text(c)).collect();
        if texts.windows(2).all(|w| w[0] == w[1]) {
            consecutive_matched += 1;
            if consecutive_matched >= CONVERGENCE_CONSECUTIVE_MATCHES {
                return Some(texts[0].len());
            }
        } else {
            consecutive_matched = 0;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn stress_test() {
    init_tracing();
    let duration = stress_duration();
    eprintln!(
        "stress: {NUM_WEAVERS} weavers ({NUM_PRODUCERS} producers), {NUM_SPOOLS} spools, {duration:?}"
    );

    let discovery = TestAddressLookup::new();
    let mut acceptor_tasks = Vec::new();
    let mut acceptor_ids = Vec::new();
    let mut _dirs = Vec::new();
    for _ in 0..NUM_SPOOLS {
        let (task, id, dir) = spawn_acceptor(&discovery).await;
        acceptor_tasks.push(task);
        acceptor_ids.push(id);
        _dirs.push(dir);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client0 = test_yrs_weaver_client("weaver-0", test_endpoint(&discovery).await).await;
    let group0 = client0
        .create(&acceptor_ids, "yrs")
        .await
        .expect("create group");
    let crdt0 = YrsCrdt::new();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut weavers: Vec<WeaverPair> = vec![(group0, crdt0)];
    for i in 1..NUM_WEAVERS {
        let mut client =
            test_yrs_weaver_client(&format!("weaver-{i}"), test_endpoint(&discovery).await).await;
        let kp = client.generate_key_package().expect("key package");
        let (group0, _) = &mut weavers[0];
        group0.add_member(&kp).await.expect("add member");

        let welcome = client.recv_welcome().await.expect("welcome");
        let join_info = client.join(&welcome).await.expect("join");
        let crdt = YrsCrdt::new();

        weavers.push((join_info.group, crdt));
        eprintln!("  weaver {i} joined");
    }

    let target_epoch = weavers[0].0.context().await.expect("context").epoch;
    wait_for_epoch(&mut weavers, target_epoch, Duration::from_secs(60)).await;
    eprintln!("epoch {target_epoch:?}, starting run");

    let token = CancellationToken::new();
    let mut send_drain_hist = Histogram::<u64>::new(2).unwrap().into_sync();
    let mut tasks = Vec::new();

    for (i, (mut group, mut crdt)) in weavers.drain(..NUM_PRODUCERS).enumerate() {
        let token = token.clone();
        let mut recorder = send_drain_hist.recorder();
        tasks.push(tokio::spawn(async move {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let mut events = group.subscribe();
            while !token.is_cancelled() {
                group.sync(&mut crdt);
                random_edit(&crdt, &mut rng);
                let t0 = Instant::now();
                group.send_update(&mut crdt).await.expect("send_update");
                drain_compaction(&mut group, &mut crdt, &mut events).await;
                recorder += t0.elapsed().as_nanos() as u64;
                tokio::task::yield_now().await;
            }
            (group, crdt)
        }));
    }
    for (mut group, mut crdt) in weavers.drain(..) {
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            let mut events = group.subscribe();
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    up = group.wait_for_update(&mut crdt) => {
                        if up.is_none() {
                            break;
                        }
                    }
                }
                drain_compaction(&mut group, &mut crdt, &mut events).await;
            }
            (group, crdt)
        }));
    }

    let start = Instant::now();
    tokio::time::sleep(duration).await;
    token.cancel();
    let elapsed = start.elapsed();

    let mut all_weavers: Vec<WeaverPair> = Vec::with_capacity(NUM_WEAVERS);
    for task in tasks {
        all_weavers.push(task.await.expect("task panic"));
    }

    send_drain_hist.refresh();
    if !send_drain_hist.is_empty() {
        eprintln!("update stats:");
        for iv in send_drain_hist.iter_quantiles(1) {
            let d = Duration::from_nanos(iv.value_iterated_to());
            eprintln!("  p{:.3}={d:.3?}", iv.percentile());
        }
        eprintln!(
            "  count={} ({:.0} events/s)",
            send_drain_hist.len(),
            send_drain_hist.len() as f64 / elapsed.as_secs_f64()
        );
    }

    eprintln!("convergence (timeout {CONVERGENCE_TIMEOUT:?})...");
    for (group, crdt) in &mut all_weavers {
        group.flush_remaining(crdt).await.expect("flush_remaining");
    }
    tokio::time::sleep(PROPAGATION_SLEEP_AFTER_FLUSH).await;
    drain_all(&mut all_weavers).await;
    drain_until_idle(&mut all_weavers, 100, IDLE_ROUNDS_BEFORE_CONVERGE).await;

    let converged =
        tokio::time::timeout(CONVERGENCE_TIMEOUT, run_convergence_loop(&mut all_weavers)).await;

    let texts: Vec<String> = all_weavers.iter().map(|(_, c)| doc_text(c)).collect();
    match converged {
        Ok(Some(len)) => eprintln!("converged, {len} chars"),
        Ok(None) => unreachable!(),
        Err(_) => {
            let lengths: Vec<usize> = texts.iter().map(String::len).collect();
            panic!("convergence timeout, lengths: {lengths:?}");
        }
    }

    let base = &texts[0];
    for (i, t) in texts.iter().enumerate().skip(1) {
        assert_eq!(t, base, "weaver {i} diverged");
    }
    eprintln!("all {NUM_WEAVERS} converged");

    for (group, _) in all_weavers {
        group.shutdown().await;
    }
    for task in acceptor_tasks {
        task.abort();
    }
}
