//! Filament Editor — a collaborative text editor built on Filament.
//! Actor-based Tauri v2 app: no mutexes, all state mediated by channels.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

pub mod commands;

use filament_core::ALPN;
use filament_editor::CoordinatorActor;
use filament_editor::types::{AppState, CoordinatorRequest};
use filament_weave::{FjallGroupStateStorage, WeaverClient};
use iroh::address_lookup::{DnsAddressLookup, MdnsAddressLookup, PkarrPublisher};
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
            commands::create_document,
            commands::get_key_package,
            commands::recv_welcome,
            commands::join_document_bytes,
            commands::join_external,
            commands::generate_external_invite,
            commands::apply_delta,
            commands::get_document_text,
            commands::add_member,
            commands::add_spool,
            commands::list_spools,
            commands::list_peers,
            commands::add_peer,
            commands::remove_member,
            commands::remove_spool,
            commands::get_group_state,
            commands::update_keys,
            commands::update_cursor,
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
        .alpns(vec![ALPN.to_vec()])
        .address_lookup(PkarrPublisher::n0_dns())
        .address_lookup(DnsAddressLookup::n0_dns())
        .address_lookup(MdnsAddressLookup::builder())
        .bind()
        .await?;

    info!(addr = ?endpoint.addr(), "iroh endpoint ready");

    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("filament-editor");
    std::fs::create_dir_all(&data_dir)?;
    let storage = FjallGroupStateStorage::open(data_dir.join("weaver_state")).await?;

    let identity = iroh_key.public().as_bytes().to_vec();
    let group_client = WeaverClient::new(identity, endpoint.clone(), storage);

    let coordinator = CoordinatorActor::new(group_client, coordinator_rx, app_handle);
    coordinator.run().await;

    Ok(())
}
