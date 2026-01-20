<!-- Terminal.vue -->
<template>
  <div class="terminal-operation">
    <div class="page-header">
      <div class="header-content">
        <h2><Icon :path="mdiConsole" size="20" /> Terminal</h2>
        <p class="header-subtitle">Execute SQL commands directly</p>
      </div>
      <div class="header-status" :class="{ connected: connected }">
        <span class="status-dot"></span>
        {{ connected ? 'Connected' : 'Connecting...' }}
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
      <div class="codeArea-result" ref="resultContainer">
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
              <pre v-else class="error">执行的SQL语句：{{ item.rayon_response.request_content || codeInput || '(unknown)' }}

错误信息：{{ item.rayon_response.error || '未知错误' }}</pre>
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
import { ref, onMounted, onBeforeUnmount, nextTick } from 'vue'
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
const resultContainer = ref(null)
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
      // 自动滚动到底部
      scrollToBottom()
    } catch (e) {
      console.warn('Parse WebSocket message failed', e, ev.data)
    }
  }
}

function scrollToBottom() {
  nextTick(() => {
    if (resultContainer.value) {
      resultContainer.value.scrollTop = resultContainer.value.scrollHeight
    }
  })
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
  
  // 提取执行的SQL语句
  const sql = item.rayon_response.request_content || codeInput.value || '(unknown SQL)'
  let output = `执行的SQL语句：${sql}\n\n`
  
  // 处理响应内容 - 尝试解析查询结果
  const responseContent = item.rayon_response.response_content
  
  // 检查是否是查询结果（包含uniform_result）
  if (item.rayon_response.uniform_result && 
      Array.isArray(item.rayon_response.uniform_result) && 
      item.rayon_response.uniform_result.length > 0) {
    
    const result = item.rayon_response.uniform_result[0]
    if (result.data && result.data.rows && result.data.columns) {
      const rows = result.data.rows
      const columns = result.data.columns
      
      if (rows.length === 0) {
        output += '(没有查询结果)\n'
        return output
      }
      
      // 格式化表格
      output += formatTable(columns, rows)
      output += `\n总记录数: ${rows.length}`
      return output
    }
  }
  
  // 检查是否是旧格式的Query结果
  if (responseContent && Array.isArray(responseContent)) {
    for (const item of responseContent) {
      if (item.Query && item.Query.rows && item.Query.columns) {
        const rows = item.Query.rows
        const columns = item.Query.columns
        
        if (rows.length === 0) {
          output += '(没有查询结果)\n'
          return output
        }
        
        output += formatTable(columns, rows)
        output += `\n总记录数: ${rows.length}`
        return output
      }
    }
  }
  
  // 非查询语句（如INSERT、UPDATE、DELETE、CREATE等）
  if (responseContent && Array.isArray(responseContent)) {
    if (responseContent.length === 0) {
      output += '执行成功'
      return output
    }
    // 显示受影响的行数或其他信息
    output += JSON.stringify(responseContent, null, 2)
    return output
  }
  
  output += '执行成功'
  return output
}

function formatTable(columns, rows) {
  // 计算每列的最大宽度
  const columnWidths = columns.map((col, idx) => {
    const colName = String(col || `col${idx}`)
    let maxWidth = colName.length
    
    rows.forEach(row => {
      const value = formatCellValue(row[idx])
      maxWidth = Math.max(maxWidth, value.length)
    })
    
    return Math.min(maxWidth, 50) // 限制最大宽度为50
  })
  
  let output = ''
  
  // 输出列名
  const headerLine = columns.map((col, idx) => {
    const colName = String(col || `col${idx}`)
    return colName.padEnd(columnWidths[idx], ' ')
  }).join(' | ')
  
  output += headerLine + '\n'
  
  // 输出分隔线
  const separatorLine = columnWidths.map(width => '-'.repeat(width)).join('-+-')
  output += separatorLine + '\n'
  
  // 输出数据行
  rows.forEach(row => {
    const rowLine = row.map((cell, idx) => {
      const value = formatCellValue(cell)
      return value.padEnd(columnWidths[idx], ' ')
    }).join(' | ')
    output += rowLine + '\n'
  })
  
  return output
}

function formatCellValue(cell) {
  if (cell === null || cell === undefined) {
    return 'NULL'
  }
  
  // 处理对象类型（如 {Integer: 1}, {Chars: {value: "text"}}）
  if (typeof cell === 'object') {
    if (cell.Integer !== undefined) return String(cell.Integer)
    if (cell.Float !== undefined) return String(cell.Float)
    if (cell.Chars && cell.Chars.value !== undefined) return String(cell.Chars.value)
    if (cell.Boolean !== undefined) return String(cell.Boolean)
    return JSON.stringify(cell)
  }
  
  const str = String(cell)
  // 截断过长的字符串
  return str.length > 50 ? str.substring(0, 47) + '...' : str
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
  line-height: 1.6;
  color: #1a1f36;
  white-space: pre;
  overflow-x: auto;
  padding: 12px;
  background: #ffffff;
  border-radius: 6px;
  border: 1px solid #e5e7eb;
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