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
    docAcceptors: [], // acceptor IDs for current document
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
    btnAddMember: document.getElementById('btn-add-member'),
    
    // Join modal
    joinModal: document.getElementById('join-modal'),
    myInviteCode: document.getElementById('my-invite-code'),
    btnCopyInviteCode: document.getElementById('btn-copy-invite-code'),
    joinStatus: document.getElementById('join-status'),
    btnJoinCancel: document.getElementById('btn-join-cancel'),
    joinModalClose: document.getElementById('join-modal-close'),
    
    // Invite modal (for document owner to add members)
    inviteModal: document.getElementById('invite-modal'),
    inviteKp: document.getElementById('invite-kp'),
    btnInviteConfirm: document.getElementById('btn-invite-confirm'),
    btnInviteCancel: document.getElementById('btn-invite-cancel'),
    inviteModalClose: document.getElementById('invite-modal-close'),
    
    // Document acceptor modal
    btnDocAcceptors: document.getElementById('btn-doc-acceptors'),
    acceptorModal: document.getElementById('acceptor-modal'),
    acceptorList: document.getElementById('acceptor-list'),
    acceptorEmpty: document.getElementById('acceptor-empty'),
    acceptorAddr: document.getElementById('acceptor-addr'),
    btnAddAcceptorConfirm: document.getElementById('btn-add-acceptor-confirm'),
    btnAcceptorDone: document.getElementById('btn-acceptor-done'),
    acceptorModalClose: document.getElementById('acceptor-modal-close'),
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
// Acceptor Management
// ============================================================================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function renderDocAcceptorList() {
    const items = elements.acceptorList.querySelectorAll('.acceptor-item');
    items.forEach(item => item.remove());
    
    if (state.docAcceptors.length === 0) {
        elements.acceptorEmpty.classList.remove('hidden');
    } else {
        elements.acceptorEmpty.classList.add('hidden');
        
        state.docAcceptors.forEach((acceptorId) => {
            const item = document.createElement('div');
            item.className = 'acceptor-item';
            // Display first/last parts of the ID for readability
            const shortId = acceptorId.length > 16 
                ? `${acceptorId.slice(0, 8)}...${acceptorId.slice(-8)}`
                : acceptorId;
            item.innerHTML = `
                <div class="acceptor-item-info">
                    <span class="acceptor-item-id" title="${escapeHtml(acceptorId)}">${escapeHtml(shortId)}</span>
                </div>
            `;
            elements.acceptorList.appendChild(item);
        });
    }
}

async function addDocAcceptor(addr) {
    if (!state.currentDocument) {
        showToast('No document open', 'error');
        return;
    }
    
    try {
        await invoke('add_acceptor', { 
            groupId: state.currentDocument.group_id,
            addrB58: addr 
        });
        // The acceptor list will be updated via the acceptor-added event
        showToast('Acceptor added!', 'success');
    } catch (error) {
        console.error('Failed to add acceptor:', error);
        showToast(`Failed to add acceptor: ${error}`, 'error');
    }
}

async function fetchDocAcceptors() {
    if (!state.currentDocument) return;
    
    try {
        const acceptors = await invoke('list_acceptors', {
            groupId: state.currentDocument.group_id
        });
        state.docAcceptors = acceptors;
        renderDocAcceptorList();
    } catch (error) {
        console.error('Failed to fetch acceptors:', error);
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
            await fetchDocAcceptors();
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
        await fetchDocAcceptors();
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
        await fetchDocAcceptors();
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

// `lastSentText` tracks the text that the backend CRDT is known to have.
// We only update it after a successful send or when a remote update arrives.
let lastSentText = '';
let lastText = '';
let debounceTimer = null;

async function handleEditorInput() {
    if (!state.currentDocument) return;

    lastText = elements.editor.value;

    // Debounce rapid changes.  When the timer fires we compute the
    // delta against `lastSentText` (the last state the backend knows
    // about), so the diff captures ALL intermediate keystrokes — not
    // just the most recent one.
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(async () => {
        const currentText = elements.editor.value;
        const delta = computeDelta(lastSentText, currentText);
        if (!delta) return;

        try {
            await invoke('apply_delta', {
                groupId: state.currentDocument.group_id,
                delta: delta,
            });
            lastSentText = currentText;
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
    
    // Add member button
    elements.btnAddMember.addEventListener('click', () => {
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        elements.inviteKp.value = '';
        showModal(elements.inviteModal);
    });
    
    // Invite modal (add member)
    elements.btnInviteCancel.addEventListener('click', () => hideModal(elements.inviteModal));
    elements.inviteModalClose.addEventListener('click', () => hideModal(elements.inviteModal));
    elements.inviteModal.querySelector('.modal-backdrop').addEventListener('click', () => hideModal(elements.inviteModal));
    
    elements.btnInviteConfirm.addEventListener('click', async () => {
        const inviteCode = elements.inviteKp.value.trim();
        if (!inviteCode) {
            showToast('Please enter an invite code', 'error');
            return;
        }
        
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        
        try {
            await invoke('add_member', {
                groupId: state.currentDocument.group_id,
                keyPackageB58: inviteCode,
            });
            hideModal(elements.inviteModal);
            showToast('Member added! Share the welcome message with them.', 'success');
            // TODO: Show the welcome message to copy
        } catch (error) {
            console.error('Failed to add member:', error);
            showToast(`Failed to add member: ${error}`, 'error');
        }
    });
    
    // Editor input
    elements.editor.addEventListener('input', handleEditorInput);
    
    // Document acceptor modal
    elements.btnDocAcceptors.addEventListener('click', () => {
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        renderDocAcceptorList();
        showModal(elements.acceptorModal);
    });
    
    const closeDocAcceptorModal = () => hideModal(elements.acceptorModal);
    elements.btnAcceptorDone.addEventListener('click', closeDocAcceptorModal);
    elements.acceptorModalClose.addEventListener('click', closeDocAcceptorModal);
    elements.acceptorModal.querySelector('.modal-backdrop').addEventListener('click', closeDocAcceptorModal);
    
    elements.btnAddAcceptorConfirm.addEventListener('click', async () => {
        const addr = elements.acceptorAddr.value.trim();
        
        if (!addr) {
            showToast('Please enter the acceptor address', 'error');
            return;
        }
        
        await addDocAcceptor(addr);
        elements.acceptorAddr.value = '';
    });

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
            hideModal(elements.inviteModal);
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
    
    // Listen for acceptor list changes
    await listen('acceptors-updated', (event) => {
        const { group_id, acceptors } = event.payload;
        
        if (state.currentDocument && state.currentDocument.group_id === group_id) {
            state.docAcceptors = acceptors;
            renderDocAcceptorList();
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
