/**
 * Sync Editor - Frontend application
 * 
 * A collaborative text editor using Tauri + universal-sync
 */

// Check if we're running in Tauri
const isTauri = '__TAURI__' in window;

// Tauri API bridge (or mock for development)
const invoke = isTauri 
    ? window.__TAURI__.core.invoke 
    : async (cmd, args) => {
        console.log(`[Mock] invoke: ${cmd}`, args);
        // Mock responses for development
        if (cmd === 'create_document') {
            return { group_id: 'mock-' + Date.now(), text: '', member_count: 1 };
        }
        if (cmd === 'get_document_text') {
            return 'Mock document text';
        }
        return null;
    };

// State
const state = {
    currentDocument: null,
    isConnected: false,
    pendingChanges: [],
};

// DOM Elements
const elements = {
    emptyState: document.getElementById('empty-state'),
    editorContainer: document.getElementById('editor-container'),
    editor: document.getElementById('editor'),
    docId: document.getElementById('doc-id'),
    syncStatus: document.getElementById('sync-status'),
    collaborators: document.getElementById('collaborators'),
    toastContainer: document.getElementById('toast-container'),
    
    // Buttons
    btnNewDoc: document.getElementById('btn-new-doc'),
    btnNewDocEmpty: document.getElementById('btn-new-doc-empty'),
    btnJoinDoc: document.getElementById('btn-join-doc'),
    btnCopyId: document.getElementById('btn-copy-id'),
    btnPeers: document.getElementById('btn-peers'),
    
    // Join modal
    joinModal: document.getElementById('join-modal'),
    myInviteCode: document.getElementById('my-invite-code'),
    btnCopyInviteCode: document.getElementById('btn-copy-invite-code'),
    joinStatus: document.getElementById('join-status'),
    btnJoinCancel: document.getElementById('btn-join-cancel'),
    joinModalClose: document.getElementById('join-modal-close'),
    
    // Peers modal (unified members + acceptors)
    peersModal: document.getElementById('peers-modal'),
    peerList: document.getElementById('peer-list'),
    peerEmpty: document.getElementById('peer-empty'),
    peerInput: document.getElementById('peer-input'),
    btnAddPeer: document.getElementById('btn-add-peer'),
    btnPeersDone: document.getElementById('btn-peers-done'),
    peersModalClose: document.getElementById('peers-modal-close'),
};

// ============================================================================
// Toast Notifications
// ============================================================================

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    elements.toastContainer.appendChild(toast);
    
    // Auto-remove after 3 seconds
    setTimeout(() => {
        toast.classList.add('leaving');
        setTimeout(() => toast.remove(), 200);
    }, 3000);
}

// ============================================================================
// Modal Management
// ============================================================================

function showModal(modal) {
    modal.classList.remove('hidden');
}

function hideModal(modal) {
    modal.classList.add('hidden');
}

// ============================================================================
// Editor Management
// ============================================================================

function showEditor() {
    elements.emptyState.classList.add('hidden');
    elements.editorContainer.classList.remove('hidden');
    elements.editor.focus();
}

function hideEditor() {
    elements.editorContainer.classList.add('hidden');
    elements.emptyState.classList.remove('hidden');
}

function updateSyncStatus(status) {
    const indicator = elements.syncStatus.querySelector('.sync-indicator');
    
    switch (status) {
        case 'synced':
            indicator.className = 'sync-indicator synced';
            elements.syncStatus.lastChild.textContent = 'Synced';
            break;
        case 'syncing':
            indicator.className = 'sync-indicator syncing';
            elements.syncStatus.lastChild.textContent = 'Syncing...';
            break;
        case 'error':
            indicator.className = 'sync-indicator error';
            elements.syncStatus.lastChild.textContent = 'Error';
            break;
    }
}

// ============================================================================
// Peer Management (members + acceptors)
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
    // Remove existing items (keep the empty placeholder)
    elements.peerList.querySelectorAll('.peer-item, .peer-section-label').forEach(el => el.remove());

    if (peers.length === 0) {
        elements.peerEmpty.classList.remove('hidden');
        return;
    }
    elements.peerEmpty.classList.add('hidden');

    const members = peers.filter(p => p.kind === 'Member');
    const acceptors = peers.filter(p => p.kind === 'Acceptor');

    if (members.length > 0) {
        const label = document.createElement('div');
        label.className = 'peer-section-label';
        label.textContent = 'Members';
        elements.peerList.appendChild(label);

        members.forEach(m => {
            const item = document.createElement('div');
            item.className = 'peer-item';
            const selfTag = m.is_self ? ' <span class="peer-item-tag self-tag">(you)</span>' : '';
            item.innerHTML = `
                <div class="peer-item-info">
                    <span class="peer-item-id" title="${escapeHtml(m.identity)}">${escapeHtml(shortId(m.identity))}${selfTag}</span>
                </div>
                ${!m.is_self ? `<button class="peer-item-remove" data-action="remove-member" data-index="${m.index}" title="Remove member">✕</button>` : ''}
            `;
            elements.peerList.appendChild(item);
        });
    }

    if (acceptors.length > 0) {
        const label = document.createElement('div');
        label.className = 'peer-section-label';
        label.textContent = 'Acceptors';
        elements.peerList.appendChild(label);

        acceptors.forEach(a => {
            const item = document.createElement('div');
            item.className = 'peer-item';
            item.innerHTML = `
                <div class="peer-item-info">
                    <span class="peer-item-id" title="${escapeHtml(a.id)}">${escapeHtml(shortId(a.id))}</span>
                </div>
                <button class="peer-item-remove" data-action="remove-acceptor" data-id="${escapeHtml(a.id)}" title="Remove acceptor">✕</button>
            `;
            elements.peerList.appendChild(item);
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
    if (!state.currentDocument) {
        showToast('No document open', 'error');
        return;
    }
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

async function removeMember(memberIndex) {
    if (!state.currentDocument) return;
    try {
        await invoke('remove_member', {
            groupId: state.currentDocument.group_id,
            memberIndex: memberIndex,
        });
        showToast('Member removed', 'success');
        await fetchPeers();
    } catch (error) {
        console.error('Failed to remove member:', error);
        showToast(`Failed to remove member: ${error}`, 'error');
    }
}

async function removeAcceptor(acceptorIdB58) {
    if (!state.currentDocument) return;
    try {
        await invoke('remove_acceptor', {
            groupId: state.currentDocument.group_id,
            acceptorIdB58: acceptorIdB58,
        });
        showToast('Acceptor removed', 'success');
        await fetchPeers();
    } catch (error) {
        console.error('Failed to remove acceptor:', error);
        showToast(`Failed to remove acceptor: ${error}`, 'error');
    }
}


// ============================================================================
// Invite Code (Key Package)
// ============================================================================

async function generateInviteCode() {
    try {
        const keyPackage = await invoke('get_key_package');
        return keyPackage;
    } catch (error) {
        console.error('Failed to generate invite code:', error);
        throw error;
    }
}

async function showJoinModal() {
    showModal(elements.joinModal);
    elements.myInviteCode.value = 'Generating...';
    elements.joinStatus.classList.add('hidden');
    
    try {
        const inviteCode = await generateInviteCode();
        elements.myInviteCode.value = inviteCode;
        
        // Show waiting status
        elements.joinStatus.classList.remove('hidden');
        elements.joinStatus.innerHTML = `
            <span class="join-status-icon">⏳</span>
            <span class="join-status-text">Waiting to be added...</span>
        `;
        
        // Start listening for welcome message
        startWelcomeListener();
        
    } catch (error) {
        elements.myInviteCode.value = 'Error generating invite code';
        showToast('Failed to generate invite code', 'error');
    }
}

// Welcome listener state
let welcomeListenerActive = false;

async function startWelcomeListener() {
    if (welcomeListenerActive) return;
    welcomeListenerActive = true;
    
    try {
        // recv_welcome blocks until a welcome arrives, then auto-joins
        // and returns DocumentInfo directly.
        const doc = await invoke('recv_welcome');
        
        if (doc && welcomeListenerActive) {
            elements.joinStatus.innerHTML = `
                <span class="join-status-icon">✓</span>
                <span class="join-status-text">Joined!</span>
            `;
            elements.joinStatus.classList.add('success');
            
            // Apply the returned document info
            state.currentDocument = doc;
            elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
            elements.docId.title = doc.group_id;
            elements.editor.value = doc.text;
            lastSentText = doc.text;
            lastText = doc.text;
            
            showEditor();
            updateSyncStatus('synced');
            showToast('Joined document!', 'success');
            
            hideModal(elements.joinModal);
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

function stopWelcomeListener() {
    welcomeListenerActive = false;
}

async function joinDocumentWithWelcome(welcomeBytes) {
    try {
        updateSyncStatus('syncing');
        const doc = await invoke('join_document_bytes', { welcome: welcomeBytes });
        
        state.currentDocument = doc;
        elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
        elements.docId.title = doc.group_id;
        elements.editor.value = doc.text;
        lastSentText = doc.text;
        lastText = doc.text;
        
        showEditor();
        updateSyncStatus('synced');
        showToast('Joined document!', 'success');
        
    } catch (error) {
        console.error('Failed to join document:', error);
        showToast(`Failed to join: ${error}`, 'error');
        updateSyncStatus('error');
    }
}

// ============================================================================
// Document Operations
// ============================================================================

async function createDocument() {
    try {
        updateSyncStatus('syncing');
        const doc = await invoke('create_document');
        
        state.currentDocument = doc;
        elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
        elements.docId.title = doc.group_id;
        elements.editor.value = doc.text;
        lastSentText = doc.text;
        lastText = doc.text;
        
        showEditor();
        updateSyncStatus('synced');
        showToast('Document created!', 'success');
        
    } catch (error) {
        console.error('Failed to create document:', error);
        showToast(`Failed to create document: ${error}`, 'error');
        updateSyncStatus('error');
    }
}


async function refreshText() {
    if (!state.currentDocument) return;
    
    try {
        const text = await invoke('get_document_text', { 
            groupId: state.currentDocument.group_id 
        });
        
        // Preserve cursor position if possible
        const cursorPos = elements.editor.selectionStart;
        elements.editor.value = text;
        elements.editor.selectionStart = Math.min(cursorPos, text.length);
        elements.editor.selectionEnd = elements.editor.selectionStart;
        
    } catch (error) {
        console.error('Failed to refresh text:', error);
    }
}

// ============================================================================
// Text Editing
// ============================================================================

// `lastSentText` tracks the text we have already computed a delta for.
// Updated *before* the async send so that overlapping debounce callbacks
// always diff against the most recently captured state.
let lastSentText = '';
let lastText = '';
let debounceTimer = null;

async function handleEditorInput() {
    if (!state.currentDocument) return;

    lastText = elements.editor.value;

    // Debounce rapid changes.  When the timer fires we compute the
    // delta against `lastSentText` (the last state we sent / accounted
    // for), so the diff captures ALL intermediate keystrokes.
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(async () => {
        const currentText = elements.editor.value;
        const delta = computeDelta(lastSentText, currentText);
        if (!delta) return;

        // Update baseline BEFORE the async call.  This ensures that if
        // the user keeps editing while `invoke` is in flight, the next
        // debounced delta will diff against `currentText` — not the old
        // baseline — so no changes are silently dropped.
        lastSentText = currentText;

        try {
            await invoke('apply_delta', {
                groupId: state.currentDocument.group_id,
                delta: delta,
            });
            updateSyncStatus('synced');
        } catch (error) {
            console.error('Failed to apply delta:', error);
            updateSyncStatus('error');
        }
    }, 50);
}

function computeDelta(oldText, newText) {
    // Find common prefix
    let prefixLen = 0;
    while (prefixLen < oldText.length && 
           prefixLen < newText.length && 
           oldText[prefixLen] === newText[prefixLen]) {
        prefixLen++;
    }
    
    // Find common suffix
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
    
    if (deleteLen === 0 && insertText.length === 0) {
        return null; // No change
    }
    
    if (deleteLen > 0 && insertText.length > 0) {
        return { type: 'Replace', position: prefixLen, length: deleteLen, text: insertText };
    } else if (deleteLen > 0) {
        return { type: 'Delete', position: prefixLen, length: deleteLen };
    } else if (insertText.length > 0) {
        return { type: 'Insert', position: prefixLen, text: insertText };
    }
    
    return null;
}

// ============================================================================
// Event Listeners
// ============================================================================

function setupEventListeners() {
    // New document buttons
    elements.btnNewDoc.addEventListener('click', createDocument);
    elements.btnNewDocEmpty.addEventListener('click', createDocument);
    
    // Join document
    elements.btnJoinDoc.addEventListener('click', () => showJoinModal());
    
    const closeJoinModal = () => {
        stopWelcomeListener();
        hideModal(elements.joinModal);
    };
    elements.btnJoinCancel.addEventListener('click', closeJoinModal);
    elements.joinModalClose.addEventListener('click', closeJoinModal);
    elements.joinModal.querySelector('.modal-backdrop').addEventListener('click', closeJoinModal);
    
    // Copy invite code
    elements.btnCopyInviteCode.addEventListener('click', async () => {
        const inviteCode = elements.myInviteCode.value;
        if (!inviteCode || inviteCode === 'Generating...' || inviteCode.startsWith('Error')) {
            showToast('No invite code to copy', 'error');
            return;
        }
        
        try {
            await navigator.clipboard.writeText(inviteCode);
            showToast('Invite code copied!', 'success');
        } catch (error) {
            showToast('Failed to copy', 'error');
        }
    });
    
    // Copy document ID
    elements.btnCopyId.addEventListener('click', async () => {
        if (!state.currentDocument) return;
        
        try {
            await navigator.clipboard.writeText(state.currentDocument.group_id);
            showToast('Copied document ID!', 'success');
        } catch (error) {
            showToast('Failed to copy', 'error');
        }
    });
    
    // Peers modal
    elements.btnPeers.addEventListener('click', () => {
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        elements.peerInput.value = '';
        fetchPeers();
        showModal(elements.peersModal);
    });

    const closePeersModal = () => hideModal(elements.peersModal);
    elements.btnPeersDone.addEventListener('click', closePeersModal);
    elements.peersModalClose.addEventListener('click', closePeersModal);
    elements.peersModal.querySelector('.modal-backdrop').addEventListener('click', closePeersModal);

    elements.btnAddPeer.addEventListener('click', async () => {
        const input = elements.peerInput.value.trim();
        if (!input) {
            showToast('Please paste an invite code or acceptor address', 'error');
            return;
        }
        await addPeer(input);
        elements.peerInput.value = '';
    });

    // Delegate remove clicks inside the peer list
    elements.peerList.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-action]');
        if (!btn) return;
        const action = btn.dataset.action;
        if (action === 'remove-member') {
            removeMember(parseInt(btn.dataset.index, 10));
        } else if (action === 'remove-acceptor') {
            removeAcceptor(btn.dataset.id);
        }
    });

    // Editor input
    elements.editor.addEventListener('input', handleEditorInput);

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        // Cmd/Ctrl + N = New document
        if ((e.metaKey || e.ctrlKey) && e.key === 'n') {
            e.preventDefault();
            createDocument();
        }
        
        // Escape = Close modals
        if (e.key === 'Escape') {
            hideModal(elements.joinModal);
            hideModal(elements.peersModal);
        }
    });
}

// ============================================================================
// Tauri Event Listeners
// ============================================================================

async function setupTauriEvents() {
    if (!isTauri) return;
    
    const { listen } = window.__TAURI__.event;
    
    // Listen for document updates from other clients
    await listen('document-updated', (event) => {
        const { group_id, text } = event.payload;
        
        if (state.currentDocument && state.currentDocument.group_id === group_id) {
            // The backend CRDT now contains `text`, so update our
            // baseline so the next delta is computed against it.
            lastSentText = text;

            // Skip no-op updates (e.g. echo of own edits)
            if (text === lastText) return;
            
            // Update the editor if it's the current document
            const cursorPos = elements.editor.selectionStart;
            elements.editor.value = text;
            elements.editor.selectionStart = Math.min(cursorPos, text.length);
            elements.editor.selectionEnd = elements.editor.selectionStart;
            lastText = text;
            
            updateSyncStatus('synced');
        }
    });
    
    // Refresh the peers modal if it's open when acceptors change
    await listen('acceptors-updated', (event) => {
        const { group_id } = event.payload;
        if (state.currentDocument && state.currentDocument.group_id === group_id) {
            if (!elements.peersModal.classList.contains('hidden')) {
                fetchPeers();
            }
        }
    });
    
    // Listen for connection status changes
    await listen('connection-status', (event) => {
        state.isConnected = event.payload.connected;
        updateSyncStatus(state.isConnected ? 'synced' : 'error');
    });
}

// ============================================================================
// Initialization
// ============================================================================

async function init() {
    setupEventListeners();
    
    try {
        await setupTauriEvents();
    } catch (e) {
        // Non-fatal - real-time sync events won't work
        console.warn('Tauri events not available:', e.message || e);
    }
}

// Start the app
init();
