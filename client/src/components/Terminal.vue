<template>
  <div class="terminal-operation">
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
              <span class="execution-time">耗时: {{ item.rayon_response.execution_time }} ms</span>
            </div>
            <div class="result-content">
              <pre v-if="!item.success" class="error">执行的SQL语句：{{ item.rayon_response.request_content || codeInput || '(unknown)' }}

错误信息：{{ item.rayon_response.error || '未知错误' }}</pre>
              <DataTable
                v-else-if="item.parsedTable"
                :headers="item.parsedTable.headers"
                :rows="item.parsedTable.rows"
                mode="view"
                :max-height="Math.min(((item.parsedTable.rows?.length || 1) * 52) + 56, 600)"
                :column-metadata="item.parsedTable.columnMetadata"
                compact
                class="result-table"
              />
              <pre v-else-if="item.textContent">{{ item.textContent }}</pre>
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
import DataTable from './DataTable.vue'
import { mdiLanDisconnect, mdiConsole } from '@mdi/js'

const props = defineProps({
  wsUrl: { type: String, required: false }
})

const emit = defineEmits(['sql-executed'])

import { connected as wsConnected, send as wsSend, addMessageListener, removeMessageListener } from '../services/wsService'

const connected = wsConnected
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
      const parsedTable = parseTableFromResponse(data)
      const textContent = formatResultContent(data)
      if (!parsedTable && !textContent) {
        return
      }

      codeResults.value.push({ ...data, parsedTable, textContent })
      emit('sql-executed', data)
      // 自动滚动到底部
      scrollToBottom()
    } catch (e) {
      console.warn('Parse WebSocket message failed', e, ev.data)
    }
  }
}

function parseTableFromResponse(payload) {
  const uniform = payload?.rayon_response?.uniform_result
  if (Array.isArray(uniform) && uniform[0]?.data?.rows && uniform[0]?.data?.columns) {
    const data = uniform[0].data
    const columnTypes = data.column_types || []
    const columnMetadata = data.columns.map((colName, idx) => ({
      name: String(colName || ''),
      type: columnTypes[idx] || 'UNKNOWN'
    }))

    return {
      headers: data.columns.map(col => String(col || '')),
      rows: data.rows,
      columnMetadata
    }
  }

  const query = payload?.Query
  if (query?.rows && (query?.columns || query?.cols?.[0])) {
    const cols = query.columns || query.cols?.[0] || []
    return {
      headers: cols.map(col => String(col || '')),
      rows: query.rows
    }
  }

  const responseContent = payload?.rayon_response?.response_content
  if (Array.isArray(responseContent)) {
    for (const item of responseContent) {
      if (item?.Query?.rows && (item.Query.columns || item.Query.cols?.[0])) {
        const cols = item.Query.columns || item.Query.cols?.[0] || []
        return {
          headers: cols.map(col => String(col || '')),
          rows: item.Query.rows
        }
      }
    }
  }

  return null
}

function scrollToBottom() {
  nextTick(() => {
    if (resultContainer.value) {
      resultContainer.value.scrollTop = resultContainer.value.scrollHeight
    }
  })
}

function submitSql() {
  if (!connected || !connected.value) {
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
        return u || ''
      } catch {
        return ''
      }
    })(),
    userid: 0,
    request_content: sql,
  }
  try { wsSend(JSON.stringify(payload)) } catch (e) { console.warn('ws send failed', e) }
}

function ensureWsReady() {
  if (!connected || !connected.value) {
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
        return u || ''
      } catch {
        return ''
      }
    })(),
    userid: 0,
    request_content: trimmed,
  }
  try { wsSend(JSON.stringify(payload)) } catch (e) { console.warn('ws send failed', e) }
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
    return ''
  }
  if (item.rayon_response.error === 'Checkpoint Success' &&
      Array.isArray(item.rayon_response.response_content) &&
      item.rayon_response.response_content.length === 0) {
    return ''
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
      output += '✓ 执行成功'
      return output
    }

    // 尝试提取操作反馈信息
    let feedback = []
    for (const item of responseContent) {
      if (typeof item === 'string') {
        feedback.push(item)
      } else if (item.Insert) {
        feedback.push(`✓ 插入成功`)
      } else if (item.Update) {
        feedback.push(`✓ 更新成功`)
      } else if (item.Delete) {
        feedback.push(`✓ 删除成功`)
      } else if (item.CreateTable || item.Ddl) {
        // DDL 操作，尝试从 SQL 语句中识别具体操作
        const sqlUpper = sql.toUpperCase()
        if (sqlUpper.includes('CREATE TABLE')) {
          feedback.push(`✓ 创建表成功`)
        } else if (sqlUpper.includes('DROP TABLE')) {
          feedback.push(`✓ 删除表成功`)
        } else if (sqlUpper.includes('ALTER TABLE')) {
          feedback.push(`✓ 修改表成功`)
        } else {
          feedback.push(`✓ DDL 操作成功`)
        }
      } else if (item.DropTable) {
        feedback.push(`✓ 删除表成功`)
      } else {
        // 其他情况，显示简化的信息
        const keys = Object.keys(item)
        if (keys.length > 0) {
          feedback.push(`✓ ${keys[0]} 操作成功`)
        }
      }
    }

    if (feedback.length > 0) {
      output += feedback.join('\n')
    } else {
      output += '✓ 执行成功'
    }
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

function onServiceMessage(payload) {
  try {
    const parsedTable = parseTableFromResponse(payload)
    const textContent = formatResultContent(payload)
    if (!parsedTable && !textContent) return

    // ensure timestamp and success fields for the UI
    const entry = {
      ...payload,
      parsedTable,
      textContent,
      timestamp: payload?.rayon_response?.timestamp || Math.floor(Date.now() / 1000),
      success: payload?.success !== undefined ? payload.success : true,
    }

    codeResults.value.push(entry)
    emit('sql-executed', payload)
    scrollToBottom()
  } catch (e) {
    console.warn('onServiceMessage parse failed', e, payload)
  }
}

onMounted(() => {
  addMessageListener(onServiceMessage)
})

onBeforeUnmount(() => {
  try { removeMessageListener(onServiceMessage) } catch (e) {}
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
  /* remove outer framed container: let parent/inner panels control visuals */
  background: transparent;
  border: none;
  border-radius: 0;
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
  justify-content: flex-end;
  align-items: center;
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

.result-table {
  margin: 12px 0;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: hidden;
}

.result-table.table-container {
  min-height: auto;
  height: auto;
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
  flex: 1;
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

.execution-time {
  color: #10b981;
  font-weight: 500;
  margin-left: 20px;
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
