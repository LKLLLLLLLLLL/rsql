<!-- Terminal.vue -->
<template>
  <div class="terminal-operation">
    <h1>Terminal</h1>
    <div class="terminal-panel">
      <div class="code-area">
        <SqlEditor
          v-model="codeInput"
          :placeholder="connected ? '输入 SQL 后提交' : '正在连接到 WebSocket...'"
          :disabled="!connected"
        />
      </div>
      <button
        class="codeArea-submit"
        type="button"
        :disabled="!connected"
        @click="submitSql"
      >
        {{ connected ? 'Submit' : 'Connecting' }}
      </button>
      <div class="codeArea-result">
        <div v-if="!connected">WebSocket 未连接，请稍等或检查后端是否运行。</div>
        <div v-else-if="codeResults.length === 0">暂无响应</div>
        <div v-else>
          <div v-for="(item, idx) in codeResults" :key="idx" class="codeArea-result-item">
            <div class="result-line">
              <span class="result-prompt">{{ formatTime(item.timestamp) }} USER占位 % </span>
              <span class="result-content" :class="{ 'result-error': !item.success }">
                {{ formatResultContent(item) }}
              </span>
            </div>
            <div class="result-timing">耗时: {{ item.rayon_response.execution_time }} ms</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import SqlEditor from './SqlEditor.vue'

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
    username: 'guest',
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
    username: 'guest',
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
  display: block;
}

.terminal-panel {
  display: flex; 
  flex-direction: column; 
  height: 800px;
  min-height: 320px; 
  background: #f8f8f8; 
  border-radius: 8px; 
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}

.code-area {
  flex: 0 0 30%; 
  position: relative; 
  padding: 0;
  background: white; 
  border-bottom: 1px solid #ddd;
  border-radius: 4px 4px 0 0;
  overflow: hidden;
}

.codeArea-text {
  width: 100%; 
  height: 100%; 
  resize: none; 
  font-family: monospace; 
  font-size: 24px; 
  border-radius: 4px; 
  border: 1px solid #ccc; 
  padding: 8px;
}

.codeArea-submit {
  padding: 10px 24px; 
  background: #4caf50; 
  color: #fff;
  border: none; 
  border-radius: 4px; 
  cursor: pointer; 
  font-weight: 600; 
  font-size: 15px;
  flex: 0 0 auto;
  align-self: center;
  margin: 12px 0;
  margin-left: auto
}

.codeArea-result {
  flex: 1 1 60%; 
  padding: 16px; 
  background: #fff; 
  min-height: 120px;
  overflow-y: auto;
}

.codeArea-result-item {
  margin-bottom: 12px;
  padding: 8px;
  border-bottom: 1px solid #eee;
  font-family: 'Courier New', 'Courier', 'Microsoft YaHei', monospace;
  font-weight: bold;
}

.result-line {
  display: flex;
  align-items: baseline;
  word-break: break-all;
}

.result-prompt {
  color: #666;
  flex-shrink: 0;
  margin-right: 8px;
}

.result-content {
  color: #333;
  flex: 1;
}

.result-content.result-error {
  color: #d32f2f;
}

.result-timing {
  color: #999;
  font-size: 12px;
  margin-top: 4px;
}
</style>