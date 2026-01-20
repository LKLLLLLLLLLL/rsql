import { ref } from 'vue'

const connected = ref(false)
let socket = null
let reconnectTimer = null
let isConnecting = false

const messageListeners = new Set()
const openListeners = new Set()
const closeListeners = new Set()
const errorListeners = new Set()

function buildWebSocketUrl(overrideUrl) {
  if (overrideUrl) return overrideUrl

  let username = null
  let password = null
  try {
    username = typeof window !== 'undefined' ? localStorage.getItem('username') : null
    password = typeof window !== 'undefined' ? localStorage.getItem('password') : null
  } catch (e) {
    // ignore
  }

  if (!username || !password) return null

  // In non-browser (SSR/test) environment, default to backend address
  if (typeof window === 'undefined') {
    return `ws://127.0.0.1:4456/ws?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
  }

  // During local development, some setups may not correctly proxy websocket traffic.
  // Prefer connecting directly to the backend WebSocket server to avoid dev-proxy issues.
  // Vite exposes `import.meta.env.DEV` to detect development mode.
  try {
    if (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.DEV) {
      return `ws://127.0.0.1:4456/ws?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
    }
  } catch (e) {
    // ignore if import.meta access is not available
  }

  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${protocol}://${window.location.host}/ws?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
}

function connect(overrideUrl) {
  const url = buildWebSocketUrl(overrideUrl)
  if (!url) {
    console.warn('wsService: missing credentials, will not connect')
    return false
  }

  // already connected or connecting
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    connected.value = socket.readyState === WebSocket.OPEN
    return true
  }

  try {
    isConnecting = true
    console.debug('wsService: creating WebSocket', url)
    socket = new WebSocket(url)
  } catch (e) {
    console.warn('wsService: create socket failed', e)
    isConnecting = false
    return false
  }

  socket.onopen = () => {
    connected.value = true
    openListeners.forEach(cb => { try { cb() } catch (e) {} })
    // clear reconnect timer
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    isConnecting = false
  }

  socket.onclose = (ev) => {
    connected.value = false
    closeListeners.forEach(cb => { try { cb(ev) } catch (e) {} })
    isConnecting = false
    // try reconnect after delay
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        // attempt reconnect
        try {
          connect()
        } catch (e) {}
      }, 3000)
    }
  }

  socket.onerror = (err) => {
    connected.value = false
    console.error('wsService: socket error', err)
    errorListeners.forEach(cb => { try { cb(err) } catch (e) {} })
  }

  socket.onmessage = (ev) => {
    try {
      const data = JSON.parse(ev.data)
      messageListeners.forEach(cb => {
        try { cb(data) } catch (e) {}
      })
    } catch (e) {
      // ignore malformed
    }
  }

  return true
}

function send(str) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    throw new Error('WebSocket is not open')
  }
  socket.send(str)
}

function close() {
  if (socket) {
    try { socket.close() } catch (e) {}
    socket = null
  }
  connected.value = false
  if (reconnectTimer) {
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
}

function isOpen() {
  return !!(socket && socket.readyState === WebSocket.OPEN)
}

function addMessageListener(cb) { messageListeners.add(cb) }
function removeMessageListener(cb) { messageListeners.delete(cb) }
function addOpenListener(cb) { openListeners.add(cb) }
function removeOpenListener(cb) { openListeners.delete(cb) }
function addCloseListener(cb) { closeListeners.add(cb) }
function removeCloseListener(cb) { closeListeners.delete(cb) }
function addErrorListener(cb) { errorListeners.add(cb) }
function removeErrorListener(cb) { errorListeners.delete(cb) }

export {
  connected,
  connect,
  send,
  close,
  isOpen,
  addMessageListener,
  removeMessageListener,
  addOpenListener,
  removeOpenListener,
  addCloseListener,
  removeCloseListener,
  addErrorListener,
  removeErrorListener,
}

export default {
  connected,
  connect,
  send,
  close,
  isOpen,
  addMessageListener,
  removeMessageListener,
}
