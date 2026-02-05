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
    btnAddMember: document.getElementById('btn-add-member'),
    
    // Join modal
    joinModal: document.getElementById('join-modal'),
    myInviteCode: document.getElementById('my-invite-code'),
    btnCopyInviteCode: document.getElementById('btn-copy-invite-code'),
    joinWelcome: document.getElementById('join-welcome'),
    btnJoinConfirm: document.getElementById('btn-join-confirm'),
    btnJoinCancel: document.getElementById('btn-join-cancel'),
    joinModalClose: document.getElementById('join-modal-close'),
    
    // Invite modal (for document owner to add members)
    inviteModal: document.getElementById('invite-modal'),
    inviteKp: document.getElementById('invite-kp'),
    btnInviteConfirm: document.getElementById('btn-invite-confirm'),
    btnInviteCancel: document.getElementById('btn-invite-cancel'),
    inviteModalClose: document.getElementById('invite-modal-close'),
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
    elements.joinWelcome.value = '';
    
    try {
        const inviteCode = await generateInviteCode();
        elements.myInviteCode.value = inviteCode;
    } catch (error) {
        elements.myInviteCode.value = 'Error generating invite code';
        showToast('Failed to generate invite code', 'error');
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
        
        showEditor();
        updateSyncStatus('synced');
        showToast('Document created!', 'success');
        
    } catch (error) {
        console.error('Failed to create document:', error);
        showToast(`Failed to create document: ${error}`, 'error');
        updateSyncStatus('error');
    }
}

async function joinDocument(welcomeMessage) {
    try {
        updateSyncStatus('syncing');
        const doc = await invoke('join_document', { welcomeB58: welcomeMessage });
        
        state.currentDocument = doc;
        elements.docId.textContent = doc.group_id.slice(0, 12) + '...';
        elements.docId.title = doc.group_id;
        elements.editor.value = doc.text;
        
        showEditor();
        updateSyncStatus('synced');
        showToast('Joined document!', 'success');
        
    } catch (error) {
        console.error('Failed to join document:', error);
        showToast(`Failed to join: ${error}`, 'error');
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
    elements.btnJoinCancel.addEventListener('click', () => hideModal(elements.joinModal));
    elements.joinModalClose.addEventListener('click', () => hideModal(elements.joinModal));
    elements.joinModal.querySelector('.modal-backdrop').addEventListener('click', () => hideModal(elements.joinModal));
    
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
    
    elements.btnJoinConfirm.addEventListener('click', async () => {
        const welcome = elements.joinWelcome.value.trim();
        if (!welcome) {
            showToast('Please enter the welcome message', 'error');
            return;
        }
        hideModal(elements.joinModal);
        await joinDocument(welcome);
        elements.joinWelcome.value = '';
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
