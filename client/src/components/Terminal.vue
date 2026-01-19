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
            <div>时间: {{ new Date(item.timestamp * 1000).toLocaleString() }} | Conn: {{ item.connection_id }}</div>
            <div v-if="item.success">✅ {{ item.rayon_response.response_content }}</div>
            <div v-else>❌ {{ item.rayon_response.error || '未知错误' }}</div>
            <div>耗时: {{ item.rayon_response.execution_time }} ms</div>
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
}
</style>