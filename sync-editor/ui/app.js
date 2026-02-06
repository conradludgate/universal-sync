/**
 * Sync Editor — Frontend application
 *
 * Sidebar-driven collaborative text editor using Tauri + universal-sync.
 */

// ============================================================================
// Tauri bridge
// ============================================================================

const isTauri = '__TAURI__' in window;

const invoke = isTauri
    ? window.__TAURI__.core.invoke
    : async (cmd, args) => {
        console.log(`[Mock] invoke: ${cmd}`, args);
        if (cmd === 'create_document') {
            return { group_id: 'mock-' + Date.now(), text: '', member_count: 1 };
        }
        if (cmd === 'get_document_text') return '';
        return null;
    };

// ============================================================================
// State
// ============================================================================

const state = {
    /** @type {{ group_id: string, text: string }|null} */
    currentDocument: null,
    /** @type {Map<string, { group_id: string, text: string }>} */
    documents: new Map(),
    sidebarOpen: true,
};

// ============================================================================
// DOM references
// ============================================================================

const el = {
    // Layout
    sidebar: document.getElementById('sidebar'),
    btnToggleSidebar: document.getElementById('btn-toggle-sidebar'),

    // Doc list
    docList: document.getElementById('doc-list'),
    docListEmpty: document.getElementById('doc-list-empty'),
    btnNewDoc: document.getElementById('btn-new-doc'),

    // Join inline (sidebar)
    joinIdle: document.getElementById('join-idle'),
    joinActive: document.getElementById('join-active'),
    btnJoinStart: document.getElementById('btn-join-start'),
    myInviteCode: document.getElementById('my-invite-code'),
    btnCopyInviteCode: document.getElementById('btn-copy-invite-code'),
    joinStatus: document.getElementById('join-status'),
    btnJoinCancel: document.getElementById('btn-join-cancel'),

    // Editor
    emptyState: document.getElementById('empty-state'),
    editorContainer: document.getElementById('editor-container'),
    editor: document.getElementById('editor'),
    docId: document.getElementById('doc-id'),
    btnCopyId: document.getElementById('btn-copy-id'),
    syncStatus: document.getElementById('sync-status'),

    // Share modal
    btnShare: document.getElementById('btn-share'),
    shareModal: document.getElementById('share-modal'),
    shareModalClose: document.getElementById('share-modal-close'),
    btnShareDone: document.getElementById('btn-share-done'),
    groupEpoch: document.getElementById('group-epoch'),
    groupHash: document.getElementById('group-hash'),
    groupMembers: document.getElementById('group-members'),
    btnUpdateKeys: document.getElementById('btn-update-keys'),
    peerList: document.getElementById('peer-list'),
    peerEmpty: document.getElementById('peer-empty'),
    peerInput: document.getElementById('peer-input'),
    btnAddPeer: document.getElementById('btn-add-peer'),

    toastContainer: document.getElementById('toast-container'),
};

// ============================================================================
// Toast
// ============================================================================

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    el.toastContainer.appendChild(toast);
    setTimeout(() => {
        toast.classList.add('leaving');
        setTimeout(() => toast.remove(), 200);
    }, 3000);
}

// ============================================================================
// Modal helpers
// ============================================================================

function showModal(m) { m.classList.remove('hidden'); }
function hideModal(m) { m.classList.add('hidden'); }

// ============================================================================
// Sidebar toggle
// ============================================================================

function toggleSidebar() {
    state.sidebarOpen = !state.sidebarOpen;
    el.sidebar.classList.toggle('collapsed', !state.sidebarOpen);
}

// ============================================================================
// Document list (sidebar)
// ============================================================================

function renderDocList() {
    // Remove old items
    el.docList.querySelectorAll('.doc-item').forEach(e => e.remove());

    if (state.documents.size === 0) {
        el.docListEmpty.classList.remove('hidden');
        return;
    }
    el.docListEmpty.classList.add('hidden');

    for (const [gid, doc] of state.documents) {
        const item = document.createElement('div');
        item.className = 'doc-item';
        if (state.currentDocument && state.currentDocument.group_id === gid) {
            item.classList.add('active');
        }
        const label = gid.length > 12 ? gid.slice(0, 6) + '…' + gid.slice(-6) : gid;
        item.innerHTML = `<span class="doc-item-label" title="${escapeHtml(gid)}">${escapeHtml(label)}</span>`;
        item.addEventListener('click', () => switchToDocument(gid));
        el.docList.appendChild(item);
    }
}

function switchToDocument(gid) {
    const doc = state.documents.get(gid);
    if (!doc) return;

    // Save current editor text back into the current doc state
    if (state.currentDocument) {
        state.currentDocument.text = el.editor.value;
    }

    state.currentDocument = doc;
    el.docId.textContent = gid.length > 16 ? gid.slice(0, 8) + '…' + gid.slice(-8) : gid;
    el.docId.title = gid;
    el.editor.value = doc.text;
    lastSentText = doc.text;
    lastText = doc.text;

    showEditor();
    renderDocList();
    fetchGroupState();
}

// ============================================================================
// Editor management
// ============================================================================

function showEditor() {
    el.emptyState.classList.add('hidden');
    el.editorContainer.classList.remove('hidden');
    el.editor.focus();
}

function updateSyncStatus(status) {
    const indicator = el.syncStatus.querySelector('.sync-indicator');
    switch (status) {
        case 'synced':
            indicator.className = 'sync-indicator synced';
            el.syncStatus.lastChild.textContent = 'Synced';
            break;
        case 'syncing':
            indicator.className = 'sync-indicator syncing';
            el.syncStatus.lastChild.textContent = 'Syncing…';
            break;
        case 'error':
            indicator.className = 'sync-indicator error';
            el.syncStatus.lastChild.textContent = 'Error';
            break;
    }
}

// ============================================================================
// Document operations
// ============================================================================

async function createDocument() {
    try {
        updateSyncStatus('syncing');
        const doc = await invoke('create_document');

        state.documents.set(doc.group_id, doc);
        state.currentDocument = doc;

        el.docId.textContent = doc.group_id.slice(0, 8) + '…' + doc.group_id.slice(-8);
        el.docId.title = doc.group_id;
        el.editor.value = doc.text;
        lastSentText = doc.text;
        lastText = doc.text;

        showEditor();
        updateSyncStatus('synced');
        renderDocList();
        fetchGroupState();
        showToast('Document created!', 'success');
    } catch (error) {
        console.error('Failed to create document:', error);
        showToast(`Failed to create document: ${error}`, 'error');
        updateSyncStatus('error');
    }
}

function openDocument(doc) {
    state.documents.set(doc.group_id, doc);
    state.currentDocument = doc;

    el.docId.textContent = doc.group_id.slice(0, 8) + '…' + doc.group_id.slice(-8);
    el.docId.title = doc.group_id;
    el.editor.value = doc.text;
    lastSentText = doc.text;
    lastText = doc.text;

    showEditor();
    updateSyncStatus('synced');
    renderDocList();
    fetchGroupState();
}

// ============================================================================
// Text editing
// ============================================================================

let lastSentText = '';
let lastText = '';
let debounceTimer = null;

async function handleEditorInput() {
    if (!state.currentDocument) return;
    lastText = el.editor.value;

    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(async () => {
        const currentText = el.editor.value;
        const delta = computeDelta(lastSentText, currentText);
        if (!delta) return;

        lastSentText = currentText;

        try {
            await invoke('apply_delta', {
                groupId: state.currentDocument.group_id,
                delta,
            });
            updateSyncStatus('synced');
        } catch (error) {
            console.error('Failed to apply delta:', error);
            updateSyncStatus('error');
        }
    }, 50);
}

function computeDelta(oldText, newText) {
    let prefixLen = 0;
    while (prefixLen < oldText.length &&
           prefixLen < newText.length &&
           oldText[prefixLen] === newText[prefixLen]) {
        prefixLen++;
    }

    let oldSuffixStart = oldText.length;
    let newSuffixStart = newText.length;
    while (oldSuffixStart > prefixLen &&
           newSuffixStart > prefixLen &&
           oldText[oldSuffixStart - 1] === newText[newSuffixStart - 1]) {
        oldSuffixStart--;
        newSuffixStart--;
    }

    const deleteLen = oldSuffixStart - prefixLen;
    const insertText = newText.slice(prefixLen, newSuffixStart);

    if (deleteLen === 0 && insertText.length === 0) return null;
    if (deleteLen > 0 && insertText.length > 0) {
        return { type: 'Replace', position: prefixLen, length: deleteLen, text: insertText };
    }
    if (deleteLen > 0) {
        return { type: 'Delete', position: prefixLen, length: deleteLen };
    }
    if (insertText.length > 0) {
        return { type: 'Insert', position: prefixLen, text: insertText };
    }
    return null;
}

// ============================================================================
// Join flow (sidebar inline)
// ============================================================================

let welcomeListenerActive = false;

async function startJoinFlow() {
    el.joinIdle.classList.add('hidden');
    el.joinActive.classList.remove('hidden');
    el.myInviteCode.value = 'Generating…';

    try {
        const code = await invoke('get_key_package');
        el.myInviteCode.value = code;

        el.joinStatus.innerHTML = `
            <span class="join-status-icon">⏳</span>
            <span class="join-status-text">Waiting to be added…</span>
        `;
        el.joinStatus.classList.remove('success');

        startWelcomeListener();
    } catch (error) {
        el.myInviteCode.value = 'Error';
        showToast('Failed to generate invite code', 'error');
    }
}

async function startWelcomeListener() {
    if (welcomeListenerActive) return;
    welcomeListenerActive = true;

    try {
        const doc = await invoke('recv_welcome');
        if (doc && welcomeListenerActive) {
            el.joinStatus.innerHTML = `
                <span class="join-status-icon">✓</span>
                <span class="join-status-text">Joined!</span>
            `;
            el.joinStatus.classList.add('success');

            openDocument(doc);
            showToast('Joined document!', 'success');

            // Reset join UI after a moment
            setTimeout(cancelJoinFlow, 1500);
        }
    } catch (error) {
        if (welcomeListenerActive) {
            console.error('Welcome listener error:', error);
            showToast(`Failed to join: ${error}`, 'error');
        }
    } finally {
        welcomeListenerActive = false;
    }
}

function cancelJoinFlow() {
    welcomeListenerActive = false;
    el.joinActive.classList.add('hidden');
    el.joinIdle.classList.remove('hidden');
}

// ============================================================================
// Group state (inside Share modal)
// ============================================================================

function updateGroupStateDisplay(payload) {
    el.groupEpoch.textContent = payload.epoch;
    const shortHash = payload.transcript_hash.slice(0, 16) + '…';
    el.groupHash.textContent = shortHash;
    el.groupHash.title = `${payload.transcript_hash}\n(click to copy)`;
    el.groupHash.dataset.fullHash = payload.transcript_hash;
    el.groupMembers.textContent = payload.member_count;
}

function resetGroupStateDisplay() {
    el.groupEpoch.textContent = '—';
    el.groupHash.textContent = '—';
    el.groupHash.title = '';
    el.groupMembers.textContent = '—';
}

async function fetchGroupState() {
    if (!state.currentDocument) return;
    try {
        const gs = await invoke('get_group_state', {
            groupId: state.currentDocument.group_id,
        });
        updateGroupStateDisplay(gs);
    } catch (error) {
        console.error('Failed to fetch group state:', error);
    }
}

async function updateKeys() {
    if (!state.currentDocument) return;
    try {
        el.btnUpdateKeys.disabled = true;
        el.btnUpdateKeys.textContent = '⏳ Updating…';
        await invoke('update_keys', {
            groupId: state.currentDocument.group_id,
        });
        showToast('Keys updated! Epoch advanced.', 'success');
    } catch (error) {
        console.error('Failed to update keys:', error);
        showToast(`Failed to update keys: ${error}`, 'error');
    } finally {
        el.btnUpdateKeys.disabled = false;
        el.btnUpdateKeys.innerHTML = '🔑 Update Keys';
    }
}

// ============================================================================
// Peers (inside Share modal)
// ============================================================================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function shortId(id) {
    return id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-8)}` : id;
}

function renderPeerList(peers) {
    el.peerList.querySelectorAll('.peer-item, .peer-section-label').forEach(e => e.remove());

    if (peers.length === 0) {
        el.peerEmpty.classList.remove('hidden');
        return;
    }
    el.peerEmpty.classList.add('hidden');

    const members = peers.filter(p => p.kind === 'Member');
    const acceptors = peers.filter(p => p.kind === 'Acceptor');

    if (members.length > 0) {
        const label = document.createElement('div');
        label.className = 'peer-section-label';
        label.textContent = 'Members';
        el.peerList.appendChild(label);

        members.forEach(m => {
            const item = document.createElement('div');
            item.className = 'peer-item';
            const selfTag = m.is_self ? ' <span class="peer-item-tag self-tag">(you)</span>' : '';
            item.innerHTML = `
                <div class="peer-item-info">
                    <span class="peer-item-id" title="${escapeHtml(m.identity)}">${escapeHtml(shortId(m.identity))}${selfTag}</span>
                </div>
                ${!m.is_self ? `<button class="peer-item-remove" data-action="remove-member" data-index="${m.index}" title="Remove">✕</button>` : ''}
            `;
            el.peerList.appendChild(item);
        });
    }

    if (acceptors.length > 0) {
        const label = document.createElement('div');
        label.className = 'peer-section-label';
        label.textContent = 'Acceptors';
        el.peerList.appendChild(label);

        acceptors.forEach(a => {
            const item = document.createElement('div');
            item.className = 'peer-item';
            item.innerHTML = `
                <div class="peer-item-info">
                    <span class="peer-item-id" title="${escapeHtml(a.id)}">${escapeHtml(shortId(a.id))}</span>
                </div>
                <button class="peer-item-remove" data-action="remove-acceptor" data-id="${escapeHtml(a.id)}" title="Remove">✕</button>
            `;
            el.peerList.appendChild(item);
        });
    }
}

async function fetchPeers() {
    if (!state.currentDocument) return;
    try {
        const peers = await invoke('list_peers', {
            groupId: state.currentDocument.group_id,
        });
        renderPeerList(peers);
    } catch (error) {
        console.error('Failed to fetch peers:', error);
    }
}

async function addPeer(input) {
    if (!state.currentDocument) return;
    try {
        await invoke('add_peer', {
            groupId: state.currentDocument.group_id,
            inputB58: input,
        });
        showToast('Peer added!', 'success');
        await fetchPeers();
    } catch (error) {
        console.error('Failed to add peer:', error);
        showToast(`Failed to add peer: ${error}`, 'error');
    }
}

async function removeMember(idx) {
    if (!state.currentDocument) return;
    try {
        await invoke('remove_member', {
            groupId: state.currentDocument.group_id,
            memberIndex: idx,
        });
        showToast('Member removed', 'success');
        await fetchPeers();
    } catch (error) {
        console.error('Failed to remove member:', error);
        showToast(`Failed to remove member: ${error}`, 'error');
    }
}

async function removeAcceptor(id) {
    if (!state.currentDocument) return;
    try {
        await invoke('remove_acceptor', {
            groupId: state.currentDocument.group_id,
            acceptorIdB58: id,
        });
        showToast('Acceptor removed', 'success');
        await fetchPeers();
    } catch (error) {
        console.error('Failed to remove acceptor:', error);
        showToast(`Failed to remove acceptor: ${error}`, 'error');
    }
}

// ============================================================================
// Event listeners
// ============================================================================

function setupEventListeners() {
    // Sidebar toggle
    el.btnToggleSidebar.addEventListener('click', toggleSidebar);

    // New document
    el.btnNewDoc.addEventListener('click', createDocument);

    // Join flow
    el.btnJoinStart.addEventListener('click', startJoinFlow);
    el.btnJoinCancel.addEventListener('click', cancelJoinFlow);
    el.btnCopyInviteCode.addEventListener('click', async () => {
        const code = el.myInviteCode.value;
        if (!code || code === 'Generating…' || code === 'Error') return;
        try {
            await navigator.clipboard.writeText(code);
            showToast('Invite code copied!', 'success');
        } catch (_) {
            showToast('Failed to copy', 'error');
        }
    });

    // Copy document ID
    el.btnCopyId.addEventListener('click', async () => {
        if (!state.currentDocument) return;
        try {
            await navigator.clipboard.writeText(state.currentDocument.group_id);
            showToast('Document ID copied!', 'success');
        } catch (_) {
            showToast('Failed to copy', 'error');
        }
    });

    // Share modal
    el.btnShare.addEventListener('click', () => {
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        el.peerInput.value = '';
        fetchPeers();
        fetchGroupState();
        showModal(el.shareModal);
    });

    const closeShareModal = () => hideModal(el.shareModal);
    el.btnShareDone.addEventListener('click', closeShareModal);
    el.shareModalClose.addEventListener('click', closeShareModal);
    el.shareModal.querySelector('.modal-backdrop').addEventListener('click', closeShareModal);

    // Update keys
    el.btnUpdateKeys.addEventListener('click', updateKeys);

    // Copy transcript hash
    el.groupHash.addEventListener('click', async () => {
        const hash = el.groupHash.dataset.fullHash;
        if (!hash) return;
        try {
            await navigator.clipboard.writeText(hash);
            showToast('Transcript hash copied!', 'success');
        } catch (_) {
            showToast('Failed to copy', 'error');
        }
    });

    // Add peer
    el.btnAddPeer.addEventListener('click', async () => {
        const input = el.peerInput.value.trim();
        if (!input) {
            showToast('Paste an invite code or acceptor address', 'error');
            return;
        }
        await addPeer(input);
        el.peerInput.value = '';
    });

    // Remove peer delegation
    el.peerList.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-action]');
        if (!btn) return;
        if (btn.dataset.action === 'remove-member') {
            removeMember(parseInt(btn.dataset.index, 10));
        } else if (btn.dataset.action === 'remove-acceptor') {
            removeAcceptor(btn.dataset.id);
        }
    });

    // Editor input
    el.editor.addEventListener('input', handleEditorInput);

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'n') {
            e.preventDefault();
            createDocument();
        }
        if (e.key === 'Escape') {
            hideModal(el.shareModal);
        }
        if ((e.metaKey || e.ctrlKey) && e.key === 'b') {
            e.preventDefault();
            toggleSidebar();
        }
    });
}

// ============================================================================
// Tauri event listeners
// ============================================================================

async function setupTauriEvents() {
    if (!isTauri) return;
    const { listen } = window.__TAURI__.event;

    await listen('document-updated', (event) => {
        const { group_id, text } = event.payload;

        // Update stored doc
        const doc = state.documents.get(group_id);
        if (doc) doc.text = text;

        if (state.currentDocument && state.currentDocument.group_id === group_id) {
            lastSentText = text;
            if (text === lastText) return;

            const cursorPos = el.editor.selectionStart;
            el.editor.value = text;
            el.editor.selectionStart = Math.min(cursorPos, text.length);
            el.editor.selectionEnd = el.editor.selectionStart;
            lastText = text;
            updateSyncStatus('synced');
        }
    });

    await listen('group-state-changed', (event) => {
        const payload = event.payload;
        if (state.currentDocument && state.currentDocument.group_id === payload.group_id) {
            updateGroupStateDisplay(payload);
            if (!el.shareModal.classList.contains('hidden')) {
                fetchPeers();
            }
        }
    });

    await listen('acceptors-updated', (event) => {
        const { group_id } = event.payload;
        if (state.currentDocument && state.currentDocument.group_id === group_id) {
            if (!el.shareModal.classList.contains('hidden')) {
                fetchPeers();
            }
        }
    });

    await listen('connection-status', (event) => {
        updateSyncStatus(event.payload.connected ? 'synced' : 'error');
    });
}

// ============================================================================
// Init
// ============================================================================

async function init() {
    setupEventListeners();
    try {
        await setupTauriEvents();
    } catch (e) {
        console.warn('Tauri events not available:', e.message || e);
    }
}

init();
