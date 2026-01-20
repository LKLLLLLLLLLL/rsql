<template>
  <div class="terminal-operation">
    <div class="page-header">
      <div class="header-content">
        <h2><Icon :path="mdiConsole" size="20" /> Terminal</h2>
      </div>
    </div>
    
    <div class="terminal-panel">
      <div class="code-area">
        <SqlEditor
          v-model="codeInput"
          :placeholder="connected ? '输入 SQL 后提交 (例如: SELECT * FROM users)' : '正在连接到 WebSocket...'"
          :disabled="!connected"
        />
      </div>
      <div class="terminal-actions">
        <button
          class="codeArea-submit"
          type="button"
          :disabled="!connected || !codeInput.trim()"
          @click="submitSql"
        >
          {{ connected ? 'Execute SQL' : 'Connecting' }}
        </button>
        <button v-if="codeResults.length > 0" class="clear-results" @click="codeResults = []">Clear Results</button>
      </div>
      <div class="codeArea-result">
        <div class="result-header">
          <h4>Execution Results</h4>
        </div>
        <div v-if="!connected" class="empty-state">
          <Icon :path="mdiLanDisconnect" size="48" />
          <p>WebSocket 未连接，请稍等或检查后端是否运行。</p>
        </div>
        <div v-else-if="codeResults.length === 0" class="empty-state">
          <Icon :path="mdiConsole" size="48" />
          <p>暂无执行结果，请输入 SQL 并执行</p>
        </div>
        <div v-else class="results-list">
          <div v-for="(item, idx) in codeResults" :key="idx" class="codeArea-result-item">
            <div class="result-header-item">
              <span class="result-time">{{ formatTime(item.timestamp) }}</span>
              <span class="result-status" :class="{ success: item.success }">
                {{ item.success ? '✓ Success' : '✗ Error' }}
              </span>
            </div>
            <div class="result-content">
              <pre v-if="item.success">{{ formatResultContent(item) }}</pre>
              <pre v-else class="error">{{ item.rayon_response.error || '未知错误' }}</pre>
            </div>
            <div class="result-footer">
              <span>Connection: {{ item.connection_id }}</span>
              <span class="execution-time">耗时: {{ item.rayon_response.execution_time }} ms</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import SqlEditor from './SqlEditor.vue'
import Icon from './Icon.vue'
import { mdiLanDisconnect, mdiConsole } from '@mdi/js'

const props = defineProps({
  wsUrl: { type: String, required: true }
})

const emit = defineEmits(['sql-executed'])

const connected = ref(false)
const codeInput = ref('')
const codeResults = ref([])
let wsRef = null

function connectWebSocket() {
  const socket = new WebSocket(props.wsUrl)
  wsRef = socket

  socket.onopen = () => {
    connected.value = true
  }

  socket.onclose = () => {
    connected.value = false
  }

  socket.onerror = (err) => {
    console.warn('WebSocket error', err)
    connected.value = false
  }

  socket.onmessage = (ev) => {
    try {
      const data = JSON.parse(ev.data)
      codeResults.value.push(data)
      emit('sql-executed', data)
    } catch (e) {
      console.warn('Parse WebSocket message failed', e, ev.data)
    }
  }
}

function submitSql() {
  if (!wsRef || wsRef.readyState !== WebSocket.OPEN) {
    alert('WebSocket 未连接，请稍后重试')
    return
  }

  const sql = codeInput.value.trim()
  if (!sql) {
    alert('请输入 SQL 再提交')
    return
  }

  const payload = {
    username: (() => {
      try {
        const u = typeof window !== 'undefined' ? localStorage.getItem('username') : null
        return u || 'guest'
      } catch {
        return 'guest'
      }
    })(),
    userid: 0,
    request_content: sql,
  }
  wsRef.send(JSON.stringify(payload))
}

function ensureWsReady() {
  if (!wsRef || wsRef.readyState !== WebSocket.OPEN) {
    alert('WebSocket 未连接，请先启动后端或等待连接成功')
    return false
  }
  return true
}

function sendSqlStatement(sql, actionLabel = 'SQL') {
  const trimmed = (sql || '').trim()
  if (!trimmed) {
    alert(`${actionLabel} 为空，未发送`)
    return
  }
  if (!ensureWsReady()) return
  const payload = {
    username: (() => {
      try {
        const u = typeof window !== 'undefined' ? localStorage.getItem('username') : null
        return u || 'guest'
      } catch {
        return 'guest'
      }
    })(),
    userid: 0,
    request_content: trimmed,
  }
  wsRef.send(JSON.stringify(payload))
  codeInput.value = trimmed
}

function formatTime(timestamp) {
  const date = new Date(timestamp * 1000)
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  const seconds = String(date.getSeconds()).padStart(2, '0')
  return `${hours}:${minutes}:${seconds}`
}

function formatResultContent(item) {
  // 特殊处理：WebSocket连接成功
  if (item.rayon_response.error === 'Websocket Connection Established' && 
      Array.isArray(item.rayon_response.response_content) && 
      item.rayon_response.response_content.length === 0) {
    return 'WebSocket连接成功'
  }
  if (item.rayon_response.error === 'Checkpoint Success' && 
      Array.isArray(item.rayon_response.response_content) && 
      item.rayon_response.response_content.length === 0) {
    return 'WebSocket连接正常'
  }
  
  // 正常错误处理
  if (!item.success) {
    return item.rayon_response.error || '未知错误'
  }
  
  // 处理响应内容
  const content = item.rayon_response.response_content
  if (Array.isArray(content) && content.length === 0) {
    return '(empty result)'
  }
  
  return content || '(no output)'
}

onMounted(() => {
  connectWebSocket()
})

onBeforeUnmount(() => {
  if (wsRef) {
    wsRef.close()
  }
})

defineExpose({
  sendSqlStatement,
  ensureWsReady
})
</script>

<style scoped>
.terminal-operation {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #ffffff;
  border-radius: 12px;
  border: 1px solid #e3e8ef;
}

.page-header {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  background: #f8fafc;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-content {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.page-header h2 {
  font-size: 1.1rem;
  color: #1a1f36;
  margin: 0;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 12px;
}

.header-subtitle {
  font-size: 0.9rem;
  color: #6b7280;
  margin: 0;
}

.header-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: #9ca3af;
  padding: 8px 16px;
  background: #ffffff;
  border-radius: 20px;
  border: 1px solid #e5e7eb;
}

.header-status.connected {
  color: #10b981;
  background: #d1fae5;
  border-color: #a7f3d0;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #9ca3af;
}

.header-status.connected .status-dot {
  background: #10b981;
}

.terminal-panel {
  display: flex;
  flex-direction: column;
  flex: 1;
  overflow: hidden;
}

.code-area {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  min-height: 200px;
  max-height: 45vh; /* 限制最大高度 */
  overflow-y: auto;
}

.terminal-actions {
  padding: 0 24px;
  margin: 20px 0;
  display: flex;
  gap: 12px;
}

.codeArea-submit {
  padding: 12px 24px;
  background: #3b82f6;
  color: #ffffff;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.9rem;
  transition: all 0.2s ease;
  min-width: 120px;
}

.codeArea-submit:hover:not(:disabled) {
  background: #2563eb;
  transform: translateY(-1px);
}

.codeArea-submit:disabled {
  background: #e5e7eb;
  color: #9ca3af;
  cursor: not-allowed;
  transform: none;
}

.clear-results {
  padding: 12px 24px;
  background: #f3f4f6;
  color: #6b7280;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.9rem;
  transition: all 0.2s ease;
}

.clear-results:hover {
  background: #e5e7eb;
  color: #4b5563;
}

.codeArea-result {
  flex: 1;
  padding: 0 24px 24px;
  overflow-y: auto;
}

.result-header h4 {
  margin: 0 0 16px 0;
  color: #1a1f36;
  font-size: 0.95rem;
  font-weight: 600;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: #9ca3af;
  text-align: center;
  gap: 16px;
}

.empty-state p {
  margin: 0;
  color: #6b7280;
  max-width: 300px;
}

.results-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.codeArea-result-item {
  background: #f9fafb;
  border-radius: 8px;
  padding: 16px;
  border: 1px solid #e5e7eb;
  transition: all 0.2s ease;
}

.codeArea-result-item:hover {
  border-color: #d1d5db;
}

.result-header-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin: 0 0 12px 0;
}

.result-time {
  font-size: 0.85rem;
  color: #6b7280;
  font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', Monaco, Consolas, monospace;
}

.result-status {
  font-size: 0.85rem;
  font-weight: 500;
  padding: 4px 8px;
  border-radius: 12px;
  background: #fee2e2;
  color: #991b1b;
}

.result-status.success {
  background: #d1fae5;
  color: #065f46;
}

.result-content {
  margin: 12px 0;
}

.result-content pre {
  margin: 0;
  font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', Monaco, Consolas, monospace;
  font-size: 13px;
  line-height: 1.5;
  color: #1a1f36;
  white-space: pre-wrap;
  word-break: break-word;
}

.result-content pre.error {
  color: #dc2626;
}

.result-footer {
  display: flex;
  justify-content: space-between;
  font-size: 0.85rem;
  color: #6b7280;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid #e5e7eb;
}

.execution-time {
  color: #10b981;
  font-weight: 500;
}

.codeArea-result::-webkit-scrollbar {
  width: 6px;
}

.codeArea-result::-webkit-scrollbar-track {
  background: #f3f4f6;
  border-radius: 3px;
}

.codeArea-result::-webkit-scrollbar-thumb {
  background: #d1d5db;
  border-radius: 3px;
}

.codeArea-result::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}
</style>