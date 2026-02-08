//! Filament Editor — a collaborative text editor built on Filament.
//! Actor-based Tauri v2 app: no mutexes, all state mediated by channels.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use filament_core::PAXOS_ALPN;
use filament_editor::actor::CoordinatorActor;
use filament_editor::types::{AppState, CoordinatorRequest};
use filament_weave::WeaverClient;
use iroh::{Endpoint, SecretKey};
use tokio::sync::mpsc;
use tracing::info;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let (coordinator_tx, coordinator_rx) = mpsc::channel::<CoordinatorRequest>(64);

    tauri::Builder::default()
        .manage(AppState { coordinator_tx })
        .setup(move |app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(e) = setup_coordinator(coordinator_rx, handle).await {
                    tracing::error!(?e, "coordinator setup failed");
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            filament_editor::commands::create_document,
            filament_editor::commands::get_key_package,
            filament_editor::commands::recv_welcome,
            filament_editor::commands::join_document_bytes,
            filament_editor::commands::apply_delta,
            filament_editor::commands::get_document_text,
            filament_editor::commands::add_member,
            filament_editor::commands::add_spool,
            filament_editor::commands::list_spools,
            filament_editor::commands::list_peers,
            filament_editor::commands::add_peer,
            filament_editor::commands::remove_member,
            filament_editor::commands::remove_spool,
            filament_editor::commands::get_group_state,
            filament_editor::commands::update_keys,
            filament_editor::commands::update_cursor,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

async fn setup_coordinator(
    coordinator_rx: mpsc::Receiver<CoordinatorRequest>,
    app_handle: tauri::AppHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let iroh_key = SecretKey::generate(&mut rand::rng());
    info!(public_key = %iroh_key.public(), "iroh key generated");

    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()))
        .build();

    let endpoint = Endpoint::builder()
        .transport_config(transport_config)
        .secret_key(iroh_key.clone())
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind()
        .await?;

    info!(addr = ?endpoint.addr(), "iroh endpoint ready");

    let identity = iroh_key.public().as_bytes().to_vec();
    let group_client = WeaverClient::new(identity, endpoint);

    let coordinator = CoordinatorActor::new(group_client, coordinator_rx, app_handle);
    coordinator.run().await;

    Ok(())
}
