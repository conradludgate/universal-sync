//! Prometheus metrics for the acceptor server.

use std::sync::Arc;

use measured::label::{LabelGroupVisitor, LabelName, LabelValue, LabelVisitor};
use measured::metric::MetricEncoding;
use measured::metric::gauge::GaugeState;
use measured::metric::group::Encoding;
use measured::metric::name::MetricName;
use measured::text::BufferedTextEncoder;
use measured::{Counter, MetricGroup};
use tokio::sync::Mutex;

use crate::state_store::SharedFjallStateStore;

#[derive(MetricGroup)]
#[metric(new(state_store: SharedFjallStateStore))]
pub struct AcceptorMetrics {
    #[metric(namespace = "tokio")]
    #[metric(init = measured_tokio::RuntimeCollector::current())]
    tokio: measured_tokio::RuntimeCollector,

    pub connections_opened_total: Counter,
    pub connections_closed_total: Counter,

    pub weavers_total: Counter,

    pub data_sent_bytes_total: Counter,
    pub data_received_bytes_total: Counter,

    #[metric(namespace = "storage")]
    #[metric(init = StorageCollector::new(state_store))]
    storage: StorageCollector,
}

pub struct StorageCollector {
    state_store: SharedFjallStateStore,
}

impl StorageCollector {
    #[must_use]
    pub fn new(state_store: SharedFjallStateStore) -> Self {
        Self { state_store }
    }
}

#[derive(Copy, Clone)]
enum KeyspaceKind {
    Accepted,
    Messages,
    Snapshots,
}

impl KeyspaceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Messages => "messages",
            Self::Snapshots => "snapshots",
        }
    }
}

impl LabelValue for KeyspaceKind {
    fn visit<V: LabelVisitor>(&self, v: V) -> V::Output {
        v.write_str(self.as_str())
    }
}

impl measured::label::LabelGroup for KeyspaceKind {
    fn visit_values(&self, v: &mut impl LabelGroupVisitor) {
        const NAME: &LabelName = LabelName::from_str("keyspace");
        v.write_value(NAME, self);
    }
}

impl<Enc: Encoding> MetricGroup<Enc> for StorageCollector
where
    GaugeState: MetricEncoding<Enc>,
{
    fn collect_group_into(&self, enc: &mut Enc) -> Result<(), Enc::Err> {
        const NAME: &MetricName = MetricName::from_str("disk_bytes");

        enc.write_help(NAME, "disk space used by storage keyspaces")?;

        let db = self.state_store.database();
        let names = db.list_keyspace_names();

        let mut totals = [0u64; 3];

        for name in &names {
            let name = name.as_ref();
            let kind = if name.ends_with(".accepted") {
                Some(KeyspaceKind::Accepted)
            } else if name.ends_with(".messages") {
                Some(KeyspaceKind::Messages)
            } else if name.ends_with(".snapshots") {
                Some(KeyspaceKind::Snapshots)
            } else {
                None
            };

            if let Some(kind) = kind
                && let Ok(ks) = db.keyspace(name, fjall::KeyspaceCreateOptions::default)
            {
                totals[kind as usize] += ks.disk_space();
            }
        }

        for (i, kind) in [
            KeyspaceKind::Accepted,
            KeyspaceKind::Messages,
            KeyspaceKind::Snapshots,
        ]
        .iter()
        .enumerate()
        {
            #[allow(clippy::cast_possible_wrap)]
            measured::metric::gauge::write_gauge(enc, NAME, *kind, totals[i] as i64)?;
        }

        Ok(())
    }
}

pub struct MetricsEncoder {
    encoder: Mutex<BufferedTextEncoder>,
    pub metrics: AcceptorMetrics,
}

impl MetricsEncoder {
    pub fn new(metrics: AcceptorMetrics) -> Self {
        Self {
            encoder: Mutex::default(),
            metrics,
        }
    }

    /// # Panics
    ///
    /// Panics if metric collection fails.
    pub async fn encode(&self) -> Vec<u8> {
        let mut encoder = self.encoder.lock().await;
        self.metrics.collect_group_into(&mut *encoder).unwrap();
        encoder.finish().to_vec()
    }
}

pub type SharedMetrics = Arc<MetricsEncoder>;
