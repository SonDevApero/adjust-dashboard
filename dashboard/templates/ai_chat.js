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

function clearAllConvs() {
  if (conversations.length === 0) return;
  conversations = [];
  activeConvId = null;
  saveAll();
  renderConvList();
  renderChatArea();
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
    const preview = c.messages.find(m => m.role === 'assistant');
    const sub = preview ? preview.content.substring(0, 40).replace(/\n/g, ' ') : `${msgCount} messages`;
    return `<div class="conv-item${active}" onclick="switchConv('${c.id}')">
      <div class="conv-dot"></div>
      <div class="conv-info">
        <div class="conv-title">${escapeHtml(c.title)}</div>
        <div class="conv-meta">${escapeHtml(sub)}</div>
      </div>
      <span class="conv-arrow">&#8250;</span>
      <button class="conv-delete" onclick="deleteConv('${c.id}',event)" title="Delete">&times;</button>
    </div>`;
  }).join('');

  convList.innerHTML = items || '<div class="sidebar-empty">No conversations yet.<br>Start chatting to see history here.</div>';
}

function renderChatArea() {
  // Clear messages
  chatArea.querySelectorAll('.msg,.msg-row,.typing').forEach(el => el.remove());

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
  const msgDiv = document.createElement('div');
  msgDiv.className = `msg ${role}`;
  let html = role === 'assistant' ? formatMarkdown(content) : escapeHtml(content);

  // Actions bar for assistant
  let actions = '';
  if (role === 'assistant' && !content.startsWith('Error:') && !content.startsWith('Connection error:')) {
    const encoded = content.replace(/'/g, "\\'").replace(/\n/g, "\\n");
    actions = `<div class="msg-actions"><button onclick="copyMsg(this, '${encoded}')" title="Copy">&#x1F4CB;<span>Copy</span></button></div>`;
  }

  msgDiv.innerHTML = html + actions + `<div class="msg-time">${isRestore ? '' : formatTime()}</div>`;

  if (role === 'assistant') {
    const row = document.createElement('div');
    row.className = 'msg-row';
    row.innerHTML = '<div class="bot-avatar">\uD83D\uDC0A</div>';
    row.appendChild(msgDiv);
    chatArea.appendChild(row);
  } else {
    chatArea.appendChild(msgDiv);
  }
  chatArea.scrollTop = chatArea.scrollHeight;
}

function copyMsg(btn, text) {
  const decoded = text.replace(/\\n/g, '\n').replace(/\\'/g, "'");
  navigator.clipboard.writeText(decoded).then(() => {
    btn.innerHTML = '&#x2705;<span>Copied</span>';
    setTimeout(() => { btn.innerHTML = '&#x1F4CB;<span>Copy</span>'; }, 1500);
  }).catch(() => {});
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
  // Code blocks
  s = s.replace(/```(\w*)\n?([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
  // Inline code
  s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
  // Bold
  s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  // Markdown tables
  s = s.replace(/((?:^\|.+\|$\n?)+)/gm, (match) => {
    const rows = match.trim().split('\n').filter(r => r.trim());
    if (rows.length < 2) return match;
    // Check for separator row (|---|---|)
    const sepIdx = rows.findIndex(r => /^\|[\s\-:|]+\|$/.test(r.trim()));
    let html = '<table>';
    rows.forEach((row, i) => {
      if (i === sepIdx) return; // skip separator
      const cells = row.split('|').filter((c, ci, arr) => ci > 0 && ci < arr.length - 1);
      const tag = (sepIdx > 0 && i < sepIdx) ? 'th' : 'td';
      html += '<tr>' + cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('') + '</tr>';
    });
    html += '</table>';
    return html;
  });
  return s;
}

function showTyping() {
  const row = document.createElement('div');
  row.className = 'msg-row';
  row.id = 'typingIndicator';
  row.innerHTML = '<div class="bot-avatar">\uD83D\uDC0A</div><div class="typing"><span></span><span></span><span></span></div>';
  chatArea.appendChild(row);
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
const isMobile = window.matchMedia('(max-width:768px)').matches;
const maxTextareaH = isMobile ? 100 : 150;
chatInput.addEventListener('input', () => {
  chatInput.style.height = 'auto';
  chatInput.style.height = Math.min(chatInput.scrollHeight, maxTextareaH) + 'px';
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

// ─── Quick ask from welcome hints ───
function quickAsk(text) {
  chatInput.value = text;
  sendMessage();
}

// ─── Mobile: prevent body scroll, handle keyboard ───
if (isMobile) {
  // Prevent any touch scroll on body/html — only chat-area & sidebar-list scroll
  document.body.addEventListener('touchmove', (e) => {
    let el = e.target;
    while (el && el !== document.body) {
      const style = window.getComputedStyle(el);
      if (style.overflowY === 'auto' || style.overflowY === 'scroll') {
        // Allow scroll inside scrollable containers
        if (el.scrollHeight > el.clientHeight) return;
      }
      el = el.parentElement;
    }
    e.preventDefault();
  }, { passive: false });

  // Handle iOS keyboard: scroll chat to bottom when keyboard opens
  if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', () => {
      chatArea.scrollTop = chatArea.scrollHeight;
    });
  }
}

// ─── Init ───
loadAll();
migrateOldStorage();
if (conversations.length > 0) {
  activeConvId = conversations[0].id;
}
renderConvList();
renderChatArea();
chatInput.focus();
