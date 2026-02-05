//! Sync Editor - Federated E2EE collaborative text editor
//!
//! A Tauri-based collaborative text editor using the universal-sync framework.
//!
//! ## Running
//!
//! Without Tauri (tests only):
//! ```bash
//! cargo test -p sync-editor
//! ```
//!
//! With Tauri (full app):
//! ```bash
//! cargo run -p sync-editor --features tauri
//! ```

#![cfg_attr(all(not(debug_assertions), feature = "tauri"), windows_subsystem = "windows")]

#[cfg(feature = "tauri")]
fn main() {
    use sync_editor::tauri_app;
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    tracing::info!("Starting Sync Editor...");

    // Create tokio runtime for async initialization
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    // Initialize app state
    let app_state = runtime.block_on(tauri_app::init_app_state());

    // Build and run Tauri app
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(app_state)
        .invoke_handler(tauri::generate_handler![
            tauri_app::create_document,
            tauri_app::recv_welcome,
            tauri_app::join_document_bytes,
            tauri_app::get_document_text,
            tauri_app::apply_delta,
            tauri_app::get_key_package,
            tauri_app::add_member,
            tauri_app::add_acceptor,
            tauri_app::list_documents,
        ])
        .setup(|_app| {
            tracing::info!("Tauri app setup complete");
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(not(feature = "tauri"))]
fn main() {
    println!("Sync Editor - Federated E2EE Collaborative Text Editor");
    println!();
    println!("This is the non-Tauri build. To run the full GUI application:");
    println!("  cargo run -p sync-editor --features tauri");
    println!();
    println!("To run tests:");
    println!("  cargo test -p sync-editor");
}
