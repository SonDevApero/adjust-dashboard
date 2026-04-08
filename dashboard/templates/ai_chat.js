// Terabot AI Chat — multi-conversation with history
const chatArea = document.getElementById('chatArea');
const chatInput = document.getElementById('chatInput');
const sendBtn = document.getElementById('sendBtn');
const welcome = document.getElementById('welcome');
const convList = document.getElementById('convList');
const convEmpty = document.getElementById('convEmpty');

const CONVS_KEY = 'terabot_conversations';
let conversations = []; // [{id, title, messages:[], createdAt, updatedAt}]
let activeConvId = null;
let sending = false;

// ─── Storage ───
function saveAll() {
  try { localStorage.setItem(CONVS_KEY, JSON.stringify(conversations)); } catch {}
}
function loadAll() {
  try {
    const raw = localStorage.getItem(CONVS_KEY);
    if (raw) conversations = JSON.parse(raw) || [];
  } catch { conversations = []; }
}

// ─── Conversation management ───
function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}

function getActiveConv() {
  return conversations.find(c => c.id === activeConvId);
}

function newChat() {
  // Save current before switching
  saveAll();
  activeConvId = null;
  renderChatArea();
  renderConvList();
  chatInput.focus();
  closeSidebarMobile();
}

function switchConv(id) {
  saveAll();
  activeConvId = id;
  renderChatArea();
  renderConvList();
  chatInput.focus();
  closeSidebarMobile();
}

function deleteConv(id, e) {
  e.stopPropagation();
  conversations = conversations.filter(c => c.id !== id);
  if (activeConvId === id) {
    activeConvId = null;
    renderChatArea();
  }
  saveAll();
  renderConvList();
}

function ensureActiveConv() {
  if (!activeConvId) {
    const conv = {
      id: genId(),
      title: 'New Chat',
      messages: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    conversations.unshift(conv);
    activeConvId = conv.id;
  }
}

function autoTitle(conv) {
  // Use first user message as title (truncated)
  const first = conv.messages.find(m => m.role === 'user');
  if (first) {
    conv.title = first.content.substring(0, 50) + (first.content.length > 50 ? '...' : '');
  }
}

// ─── Rendering ───
function renderConvList() {
  const items = conversations.map(c => {
    const active = c.id === activeConvId ? ' active' : '';
    const msgCount = c.messages.filter(m => m.role === 'user').length;
    const date = new Date(c.updatedAt).toLocaleDateString();
    return `<div class="conv-item${active}" onclick="switchConv('${c.id}')">
      <div class="conv-title">${escapeHtml(c.title)}</div>
      <div class="conv-meta"><span>${msgCount} msg</span><span>${date}</span></div>
      <button class="conv-delete" onclick="deleteConv('${c.id}',event)" title="Delete">&times;</button>
    </div>`;
  }).join('');

  convList.innerHTML = items || '<div class="sidebar-empty">No conversations yet</div>';
}

function renderChatArea() {
  // Clear messages
  chatArea.querySelectorAll('.msg,.typing').forEach(el => el.remove());

  const conv = getActiveConv();
  if (!conv || conv.messages.length === 0) {
    if (welcome) welcome.style.display = '';
    return;
  }
  if (welcome) welcome.style.display = 'none';
  for (const msg of conv.messages) {
    appendMsgDom(msg.role, msg.content, true);
  }
}

function appendMsgDom(role, content, isRestore) {
  if (welcome) welcome.style.display = 'none';
  const div = document.createElement('div');
  div.className = `msg ${role}`;
  let html = role === 'assistant' ? formatMarkdown(content) : escapeHtml(content);
  div.innerHTML = html + `<div class="msg-time">${isRestore ? '' : formatTime()}</div>`;
  chatArea.appendChild(div);
  chatArea.scrollTop = chatArea.scrollHeight;
}

// ─── Helpers ───
function escapeHtml(text) {
  const d = document.createElement('div');
  d.textContent = text;
  return d.innerHTML;
}
function formatTime() {
  return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}
function formatMarkdown(text) {
  let s = escapeHtml(text);
  s = s.replace(/```(\w*)\n?([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
  s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
  s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  return s;
}

function showTyping() {
  const div = document.createElement('div');
  div.className = 'typing'; div.id = 'typingIndicator';
  div.innerHTML = '<span></span><span></span><span></span>';
  chatArea.appendChild(div);
  chatArea.scrollTop = chatArea.scrollHeight;
}
function removeTyping() {
  const el = document.getElementById('typingIndicator');
  if (el) el.remove();
}

// ─── Sidebar toggle (mobile) ───
function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('sidebarOverlay').classList.toggle('open');
}
function closeSidebarMobile() {
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebarOverlay').classList.remove('open');
}

// ─── Send ───
async function sendMessage() {
  const text = chatInput.value.trim();
  if (!text || sending) return;
  sending = true;

  ensureActiveConv();
  const conv = getActiveConv();

  // Add user message
  conv.messages.push({ role: 'user', content: text });
  conv.updatedAt = new Date().toISOString();
  autoTitle(conv);
  saveAll();
  renderConvList();

  appendMsgDom('user', text);
  chatInput.value = '';
  chatInput.style.height = 'auto';
  sendBtn.disabled = true;
  showTyping();

  try {
    const resp = await fetch('/api/ai_chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: conv.messages }),
    });

    if (resp.status === 401) { window.location.href = '/login'; return; }

    const data = await resp.json();
    removeTyping();

    if (data.error) {
      appendMsgDom('assistant', 'Error: ' + data.error);
    } else {
      const reply = data.reply || 'No response.';
      conv.messages.push({ role: 'assistant', content: reply });
      conv.updatedAt = new Date().toISOString();
      saveAll();
      appendMsgDom('assistant', reply);
    }
  } catch (e) {
    removeTyping();
    appendMsgDom('assistant', 'Connection error: ' + e.message);
  } finally {
    sending = false;
    sendBtn.disabled = false;
    chatInput.focus();
    renderConvList();
  }
}

// ─── Textarea auto-resize & Enter to send ───
chatInput.addEventListener('input', () => {
  chatInput.style.height = 'auto';
  chatInput.style.height = Math.min(chatInput.scrollHeight, 150) + 'px';
});
chatInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});

// ─── Migrate old single-chat storage ───
function migrateOldStorage() {
  const old = localStorage.getItem('terabot_chat_history');
  if (!old) return;
  try {
    const msgs = JSON.parse(old);
    if (msgs && msgs.length > 0) {
      const conv = {
        id: genId(),
        title: (msgs.find(m => m.role === 'user')?.content || 'Old Chat').substring(0, 50),
        messages: msgs,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      conversations.unshift(conv);
      saveAll();
    }
    localStorage.removeItem('terabot_chat_history');
  } catch {}
}

// ─── Init ───
loadAll();
migrateOldStorage();
// Auto-select most recent conversation if any
if (conversations.length > 0) {
  activeConvId = conversations[0].id;
}
renderConvList();
renderChatArea();
chatInput.focus();
