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
    acceptors: [], // { name, addr }
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
    
    // Acceptor modal (per-document)
    btnDocAcceptors: document.getElementById('btn-doc-acceptors'),
    acceptorModal: document.getElementById('acceptor-modal'),
    acceptorList: document.getElementById('acceptor-list'),
    acceptorEmpty: document.getElementById('acceptor-empty'),
    acceptorName: document.getElementById('acceptor-name'),
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

function renderAcceptorList() {
    // Clear existing items (except empty message)
    const items = elements.acceptorList.querySelectorAll('.acceptor-item');
    items.forEach(item => item.remove());
    
    if (state.acceptors.length === 0) {
        elements.acceptorEmpty.classList.remove('hidden');
    } else {
        elements.acceptorEmpty.classList.add('hidden');
        
        state.acceptors.forEach((acceptor, index) => {
            const item = document.createElement('div');
            item.className = 'acceptor-item';
            item.innerHTML = `
                <div class="acceptor-item-info">
                    <span class="acceptor-item-name">${escapeHtml(acceptor.name)}</span>
                    <span class="acceptor-item-addr" title="${escapeHtml(acceptor.addr)}">${acceptor.addr.slice(0, 32)}...</span>
                </div>
                <button class="acceptor-item-remove" data-index="${index}" title="Remove">✕</button>
            `;
            elements.acceptorList.appendChild(item);
        });
        
        // Add remove handlers
        elements.acceptorList.querySelectorAll('.acceptor-item-remove').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const index = parseInt(e.target.dataset.index, 10);
                removeAcceptor(index);
            });
        });
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function addAcceptor(name, addr) {
    if (!state.currentDocument) {
        showToast('No document open', 'error');
        return;
    }
    
    try {
        await invoke('add_acceptor', { 
            groupId: state.currentDocument.group_id,
            addrB58: addr 
        });
        state.acceptors.push({ name, addr });
        renderAcceptorList();
        showToast(`Added acceptor: ${name}`, 'success');
    } catch (error) {
        console.error('Failed to add acceptor:', error);
        showToast(`Failed to add acceptor: ${error}`, 'error');
    }
}

function removeAcceptor(index) {
    // Note: Backend doesn't support removal yet, just remove from UI state
    const acceptor = state.acceptors[index];
    state.acceptors.splice(index, 1);
    renderAcceptorList();
    showToast(`Removed acceptor: ${acceptor.name}`, 'info');
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
        // Poll for welcome message (backend will receive it via p2p)
        const welcome = await invoke('recv_welcome');
        
        if (welcome && welcomeListenerActive) {
            // Update status
            elements.joinStatus.innerHTML = `
                <span class="join-status-icon">✓</span>
                <span class="join-status-text">Added! Joining document...</span>
            `;
            elements.joinStatus.classList.add('success');
            
            // Join the document
            await joinDocumentWithWelcome(welcome);
            
            // Close modal
            hideModal(elements.joinModal);
        }
    } catch (error) {
        if (welcomeListenerActive) {
            console.error('Welcome listener error:', error);
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
        state.acceptors = []; // Reset acceptors for joined document
        elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
        elements.docId.title = doc.group_id;
        elements.editor.value = doc.text;
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
        state.acceptors = []; // Reset acceptors for new document
        elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
        elements.docId.title = doc.group_id;
        elements.editor.value = doc.text;
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

let lastText = '';
let debounceTimer = null;

async function handleEditorInput() {
    const newText = elements.editor.value;
    
    if (!state.currentDocument) return;
    
    // Find the diff between old and new text
    // This is a simple implementation - a real editor would use OT/CRDT-aware diffing
    const delta = computeDelta(lastText, newText);
    
    if (delta) {
        // Debounce rapid changes
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(async () => {
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
    
    lastText = newText;
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
    
    // Return as a sequence of operations
    // For simplicity, we'll return the most significant operation
    if (deleteLen > 0) {
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
    
    // Acceptor modal (per-document)
    elements.btnDocAcceptors.addEventListener('click', () => {
        if (!state.currentDocument) {
            showToast('No document open', 'error');
            return;
        }
        renderAcceptorList();
        showModal(elements.acceptorModal);
    });
    
    const closeAcceptorModal = () => hideModal(elements.acceptorModal);
    elements.btnAcceptorDone.addEventListener('click', closeAcceptorModal);
    elements.acceptorModalClose.addEventListener('click', closeAcceptorModal);
    elements.acceptorModal.querySelector('.modal-backdrop').addEventListener('click', closeAcceptorModal);
    
    elements.btnAddAcceptorConfirm.addEventListener('click', async () => {
        const name = elements.acceptorName.value.trim();
        const addr = elements.acceptorAddr.value.trim();
        
        if (!name) {
            showToast('Please enter a name for the acceptor', 'error');
            return;
        }
        if (!addr) {
            showToast('Please enter the acceptor address', 'error');
            return;
        }
        
        await addAcceptor(name, addr);
        elements.acceptorName.value = '';
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
            // Update the editor if it's the current document
            const cursorPos = elements.editor.selectionStart;
            elements.editor.value = text;
            elements.editor.selectionStart = Math.min(cursorPos, text.length);
            elements.editor.selectionEnd = elements.editor.selectionStart;
            lastText = text;
            
            updateSyncStatus('synced');
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
    console.log('Sync Editor initializing...');
    console.log('Running in Tauri:', isTauri);
    
    setupEventListeners();
    await setupTauriEvents();
    
    console.log('Sync Editor ready!');
}

// Start the app
init();
