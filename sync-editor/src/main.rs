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
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    tracing::info!("Starting Sync Editor...");

    // Build and run Tauri app
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
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
