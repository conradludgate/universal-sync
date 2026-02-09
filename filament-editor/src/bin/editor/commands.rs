//! Tauri command handlers — thin wrappers that forward to the coordinator actor.

use filament_core::GroupId;
use filament_editor::types::{
    AppState, CoordinatorRequest, Delta, DocRequest, DocumentInfo, GroupStatePayload, PeerEntry,
};
use tokio::sync::oneshot;

async fn coord_request<T>(
    state: &AppState,
    make_request: impl FnOnce(oneshot::Sender<Result<T, String>>) -> CoordinatorRequest,
) -> Result<T, String> {
    let (tx, rx) = oneshot::channel();
    state
        .coordinator_tx
        .send(make_request(tx))
        .await
        .map_err(|_| "coordinator actor closed".to_string())?;
    rx.await
        .map_err(|_| "coordinator dropped reply".to_string())?
}

async fn doc_request<T>(
    state: &AppState,
    group_id_b58: &str,
    make_request: impl FnOnce(oneshot::Sender<Result<T, String>>) -> DocRequest,
) -> Result<T, String> {
    let group_id = parse_group_id(group_id_b58)?;
    let (tx, rx) = oneshot::channel();
    state
        .coordinator_tx
        .send(CoordinatorRequest::ForDoc {
            group_id,
            request: make_request(tx),
        })
        .await
        .map_err(|_| "coordinator actor closed".to_string())?;
    rx.await
        .map_err(|_| "document not found or actor closed".to_string())?
}

fn parse_group_id(b58: &str) -> Result<GroupId, String> {
    let bytes = bs58::decode(b58)
        .into_vec()
        .map_err(|e| format!("invalid group ID: {e}"))?;
    Ok(GroupId::from_slice(&bytes))
}

#[tauri::command]
pub async fn create_document(state: tauri::State<'_, AppState>) -> Result<DocumentInfo, String> {
    coord_request(&state, |reply| CoordinatorRequest::CreateDocument { reply }).await
}

#[tauri::command]
pub async fn get_key_package(state: tauri::State<'_, AppState>) -> Result<String, String> {
    coord_request(&state, |reply| CoordinatorRequest::GetKeyPackage { reply }).await
}

#[tauri::command]
pub async fn recv_welcome(state: tauri::State<'_, AppState>) -> Result<DocumentInfo, String> {
    coord_request(&state, |reply| CoordinatorRequest::RecvWelcome { reply }).await
}

#[tauri::command]
pub async fn join_document_bytes(
    state: tauri::State<'_, AppState>,
    welcome_b58: String,
) -> Result<DocumentInfo, String> {
    coord_request(&state, |reply| CoordinatorRequest::JoinDocumentBytes {
        welcome_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn join_external(
    state: tauri::State<'_, AppState>,
    invite_b58: String,
) -> Result<DocumentInfo, String> {
    coord_request(&state, |reply| CoordinatorRequest::JoinExternal {
        invite_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn generate_external_invite(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<String, String> {
    doc_request(&state, &group_id, |reply| {
        DocRequest::GenerateExternalInvite { reply }
    })
    .await
}

#[tauri::command]
pub async fn apply_delta(
    state: tauri::State<'_, AppState>,
    group_id: String,
    delta: Delta,
    anchor: u32,
    head: u32,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::ApplyDelta {
        delta,
        anchor,
        head,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn get_document_text(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<String, String> {
    doc_request(&state, &group_id, |reply| DocRequest::GetText { reply }).await
}

#[tauri::command]
pub async fn add_member(
    state: tauri::State<'_, AppState>,
    group_id: String,
    key_package_b58: String,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::AddMember {
        key_package_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn add_spool(
    state: tauri::State<'_, AppState>,
    group_id: String,
    addr_b58: String,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::AddSpool {
        addr_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn list_spools(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<Vec<String>, String> {
    doc_request(&state, &group_id, |reply| DocRequest::ListSpools { reply }).await
}

#[tauri::command]
pub async fn list_peers(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<Vec<PeerEntry>, String> {
    doc_request(&state, &group_id, |reply| DocRequest::ListPeers { reply }).await
}

#[tauri::command]
pub async fn add_peer(
    state: tauri::State<'_, AppState>,
    group_id: String,
    input_b58: String,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::AddPeer {
        input_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn remove_member(
    state: tauri::State<'_, AppState>,
    group_id: String,
    member_index: u32,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::RemoveMember {
        member_index,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn remove_spool(
    state: tauri::State<'_, AppState>,
    group_id: String,
    spool_id_b58: String,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::RemoveSpool {
        spool_id_b58,
        reply,
    })
    .await
}

#[tauri::command]
pub async fn get_group_state(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<GroupStatePayload, String> {
    doc_request(&state, &group_id, |reply| DocRequest::GetGroupState {
        reply,
    })
    .await
}

#[tauri::command]
pub async fn update_keys(
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<(), String> {
    doc_request(&state, &group_id, |reply| DocRequest::UpdateKeys { reply }).await
}

#[tauri::command]
pub async fn update_cursor(
    state: tauri::State<'_, AppState>,
    group_id: String,
    anchor: u32,
    head: u32,
) -> Result<(), String> {
    let group_id = parse_group_id(&group_id)?;
    state
        .coordinator_tx
        .send(CoordinatorRequest::ForDoc {
            group_id,
            request: DocRequest::UpdateCursor { anchor, head },
        })
        .await
        .map_err(|_| "coordinator actor closed".to_string())?;
    Ok(())
}
