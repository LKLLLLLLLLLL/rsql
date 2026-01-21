<template>
<div class="sidebar">
  <div class="sidebar-header">
    <div class="header-container">
      <div class="header-icon">
        <Icon :path="mdiDatabase" size="32" />
      </div>
      <div class="header-content">
        <h1>RSQL</h1>
        <h3>Dashboard</h3>
      </div>
    </div>
    <!-- <div>
      <p class="header-subtitle">A simple relational database system written in Rust.</p>
    </div> -->
  </div>

  <div class="functions-section">
    <div class="section-header">
      <h3>FUNCTIONS</h3>
    </div>
    <div class="tables-buttons">
      <div class="tables-btn terminal" :class="{ active: activeButton === 'terminal' }" @click="handleButtonClick('terminal')">
        <div class="btn-icon">
          <Icon :path="mdiConsoleLine" size="18" />
        </div>
        <span>Terminal</span>
      </div>
      <div class="tables-btn create" :class="{ active: activeButton === 'create' }" @click="handleButtonClick('create')">
        <div class="btn-icon">
          <Icon :path="mdiTablePlus" size="18" />
        </div>
        <span>Create New Table</span>
      </div>
      <div class="tables-btn rename" :class="{ active: activeButton === 'rename' }" @click="handleButtonClick('rename')">
        <div class="btn-icon">
          <Icon :path="mdiTableEdit" size="18" />
        </div>
        <span>Rename Table</span>
      </div>
      <div class="tables-btn drop" :class="{ active: activeButton === 'drop' }" @click="handleButtonClick('drop')">
        <div class="btn-icon">
          <Icon :path="mdiTableRemove" size="18" />
        </div>
        <span>Drop Table</span>
      </div>
    </div>
  </div>

  <div class="tables-list">
    <div class="list-header">
      <h3>TABLE LIST</h3>
      <span class="table-count">{{ tables.length }} tables</span>
    </div>
    <div v-for="table in tables" :key="table.tableName || table.tableId" class="table-item" :class="{ active: currentTable === table.tableName }" @click="handleTableSelect(table)">
      <div class="table-content">
        <div class="table-icon">
          <Icon :path="mdiTable" size="16" />
        </div>
        <span class="table-name">{{ table.tableName }}</span>
        <button v-if="isDropMode" class="table-delete-btn" @click.stop="emit('delete-table', table.tableName)">
          <Icon :path="mdiTrashCanOutline" size="14" />
        </button>
      </div>
    </div>

    <!-- <div class="sidebar-footer">
      <div class="footer-content">
        <Icon :path="mdiInformationOutline" size="16" />
        <span>Click table name to view data</span>
      </div>
    </div> -->
  </div>

  <div style="display: flex; flex-direction: column; gap: 6px; width: 100%;">
    <div class="username-row" style="font-size: 0.85rem; color: #a0aec0; font-weight: 500; padding-bottom: 2px;margin-left: 30px;">
      Username: <span style="color: #f1f5f9; font-weight: 600;">{{ username }}</span>
    </div>
  </div>

  <!-- Information Panel -->
  <div class="info-panel">
    <div class="info-content">
      <div class="connection-status">
        <div class="status-indicator" :class="connectionStatus">
          <div class="status-dot"></div>
          <span class="status-text">{{ connectionStatusText }}</span>
        </div>
      </div>
      <button class="logout-btn" @click="handleLogout" title="Logout">
        <Icon :path="mdiPower" size="18" />
      </button>
    </div>
  </div>
</div>
</template>

<script setup>
import { defineProps, defineEmits, computed, onMounted, onBeforeUnmount, watch } from 'vue'
import { ref } from 'vue'

// 获取本地存储的用户名
const username = ref(localStorage.getItem('username') || '')

// 监听本地存储变化（如有需要，可扩展为响应式）
window.addEventListener('storage', (e) => {
  if (e.key === 'username') {
    username.value = e.newValue || ''
  }
})
import { useRouter } from 'vue-router'
import Icon from './Icon.vue'
import {
  mdiDatabase,
  mdiConsoleLine,
  mdiTableEdit,
  mdiTablePlus,
  mdiTableRemove,
  mdiTable,
  mdiInformationOutline,
  mdiTrashCanOutline,
  mdiPower
} from '@mdi/js'

import { connected, connect, close } from '../services/wsService'

const router = useRouter()

const props = defineProps({
  tables: { type: Array, default: () => [] },
  currentTable: { type: String, default: '' },
  activeButton: { type: String, default: '' },
  isDropMode: { type: Boolean, default: false },
  wsUrl: { type: String, default: '' }
})

const emit = defineEmits(['create', 'rename', 'drop', 'terminal', 'select-table', 'delete-table', 'clear-selection', 'logout'])

// 计算连接状态（使用单例 wsService 的连接状态）
const connectionStatus = computed(() => (connected.value ? 'connected' : 'disconnected'))
const connectionStatusText = computed(() => (connected.value ? 'Connected' : 'Disconnected'))

function handleButtonClick(button) {
  // 点击功能按钮时，清除表的选中状态
  emit('clear-selection')

  if (button === 'create') {
    emit('create')
  } else if (button === 'rename') {
    emit('rename')
  } else if (button === 'drop') {
    emit('drop')
  } else if (button === 'terminal') {
    emit('terminal')
  }
}

function handleTableSelect(table) {
  emit('select-table', table)
}
// 处理退出登录
function handleLogout() {
  // 清除登录信息
  localStorage.removeItem('username')
  localStorage.removeItem('password')
  // 关闭 全局 WebSocket 连接
  try { close() } catch (e) {}
  // 触发 logout 事件
  emit('logout')

  // 使用 replace 而不是 push，这样用户无法通过浏览器回退返回到工作页面
  router.replace('/')
}

// 组件挂载时尝试连接（使用单例 wsService），若 props 提供 wsUrl 则使用之
onMounted(() => {
  try {
    if (props.wsUrl) connect(props.wsUrl)
    else connect()
  } catch (e) {
    // ignore
  }
})

// 组件卸载时不再关闭全局连接（让单例管理生命周期）
onBeforeUnmount(() => {
  // no-op
})

// 监听 wsUrl 变化，重新连接
watch(() => props.wsUrl, (nv) => {
  try {
    close()
  } catch (e) {}
  try { connect(nv) } catch (e) {}
})
</script>

<style scoped>
/* 使用系统字体栈，保证字体一致性 */
.sidebar {
  width: 280px;
  background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
  color: #cbd5e1;
  display: flex;
  flex-direction: column;
  height: 100vh;
  position: sticky;
  top: 0;
  z-index: 10;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
  box-sizing: border-box;
  box-shadow: 0 0 20px rgba(0, 0, 0, 0.2);
}

.sidebar-header {
  padding: 22px 20px 20px 20px;
  display: flex;
  flex-direction: column;
  background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
  color: #f1f5f9;
  gap: 10px;
  position: relative;
  overflow: hidden;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.sidebar-header::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 20px;
  right: 20px;
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.3), transparent);
}

.header-icon {
  padding: 12px;
  background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(99, 102, 241, 0.3) 100%);
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 60px;
  height: 60px;
  border: 1px solid rgba(99, 102, 241, 0.2);
  box-shadow: 0 6px 16px rgba(99, 102, 241, 0.15);
}

.header-content {
  flex: 1;
  border: 0;
}

.sidebar-header h1 {
  font-size: 1.8rem;
  font-weight: 650;
  margin: 4px 0 0 0;
  line-height: 1.05;
  color: #f8fafc;
  letter-spacing: 0.04em;
  background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.sidebar-header h3 {
  font-size: 1.2rem;
  font-weight: 500;
  padding: 0 0 4px 0;
  margin: 0;
  line-height: 1.1;
  color: #d4dce7;
  letter-spacing: 0.03em;
}

.header-container {
  display: flex;
  align-items: center;
  gap: 10px;
}

.header-subtitle {
  font-size: 0.85rem;
  color: #a0aec0;
  margin: 0;
  font-weight: 400;
  letter-spacing: 0.02em;
  line-height: 1.3;
}

.functions-section {
  padding: 16px 0;
  background: #0f172a;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.section-header {
  padding: 0 20px 10px 30px;
  margin-bottom: 4px;
}

.section-header h3 {
  font-size: 0.75rem;
  font-weight: 700;
  color: #a0aec0;
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.tables-buttons {
  padding: 0 16px;
  user-select: none;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.tables-btn {
  padding: 6px 16px;
  cursor: pointer;
  border-radius: 8px;
  font-weight: 500;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  display: flex;
  align-items: center;
  gap: 12px;
  color: #cbd5e1;
  background: transparent;
  border: 1px solid transparent;
  position: relative;
  overflow: hidden;
  font-size: 0.95rem;
  min-height: 42px;
}

.tables-btn:hover {
  background: rgba(51, 65, 85, 0.6);
  color: #f1f5f9;
  border-color: rgba(71, 85, 105, 0.5);
}

.tables-btn.active {
  background: linear-gradient(135deg, rgba(51, 65, 85, 0.8) 0%, rgba(30, 41, 59, 0.9) 100%);
  color: #ffffff;
  border: 1px solid rgba(99, 102, 241, 0.5);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2), inset 0 1px 1px rgba(255, 255, 255, 0.1);
}

.tables-btn.active:hover {
  background: linear-gradient(135deg, rgba(71, 85, 105, 0.9) 0%, rgba(51, 65, 85, 0.9) 100%);
  border-color: rgba(99, 102, 241, 0.7);
}

.btn-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border-radius: 6px;
  background: rgba(148, 163, 184, 0.1);
  color: #cbd5e1;
  transition: all 0.2s ease;
}

.tables-btn:hover .btn-icon,
.tables-btn.active .btn-icon {
  background: rgba(99, 102, 241, 0.2);
  color: #ffffff;
}

.tables-list {
  padding: 16px 20px;
  overflow-y: auto;
  flex-grow: 1;
  background: #0f172a;
  border-bottom: 1px solid rgba(51, 65, 85, 0.5);
}

.list-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 14px;
  padding: 0 6px;
}

.list-header h3 {
  font-size: 0.75rem;
  font-weight: 700;
  color: #a0aec0;
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.table-count {
  font-size: 0.75rem;
  font-weight: 600;
  color: #a0aec0;
  background: rgba(30, 41, 59, 0.8);
  padding: 4px 10px;
  border-radius: 20px;
  border: 1px solid rgba(100, 116, 139, 0.3);
}

.table-item {
  padding: 0;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  display: flex;
  align-items: center;
  user-select: none;
  position: relative;
  background: transparent;
  color: #cbd5e1;
  border-radius: 8px;
  margin-bottom: 4px;
  border: 1px solid transparent;
  font-size: 0.95rem;
  font-weight: 500;
  min-height: 42px;
}

.table-item:hover {
  background: rgba(51, 65, 85, 0.6);
  color: #f1f5f9;
  border-color: rgba(71, 85, 105, 0.5);
}

.table-item.active {
  background: linear-gradient(135deg, rgba(51, 65, 85, 0.8) 0%, rgba(30, 41, 59, 0.9) 100%);
  color: #ffffff;
  font-weight: 600;
  border: 1px solid rgba(99, 102, 241, 0.5);
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2), inset 0 1px 1px rgba(255, 255, 255, 0.1);
}

.table-item.active:hover {
  background: linear-gradient(135deg, rgba(71, 85, 105, 0.9) 0%, rgba(51, 65, 85, 0.9) 100%);
  border-color: rgba(99, 102, 241, 0.7);
}

.table-content {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
  height: 100%;
  padding: 6px 16px;
}

.table-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border-radius: 6px;
  background: rgba(148, 163, 184, 0.1);
  color: #cbd5e1;
  transition: all 0.2s ease;
}

.table-item:hover .table-icon,
.table-item.active .table-icon {
  background: rgba(99, 102, 241, 0.2);
  color: #ffffff;
}

.table-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.95rem;
  font-weight: inherit;
}

.table-delete-btn {
  background: rgba(63, 63, 70, 0.8);
  color: #f43f5e;
  border: 1px solid rgba(82, 82, 91, 0.6);
  border-radius: 6px;
  width: 30px;
  height: 30px;
  min-width: 30px;
  min-height: 30px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
  margin-left: 8px;
  padding: 0;
}

.table-delete-btn:hover {
  background: rgba(82, 82, 91, 0.9);
  color: #fb7185;
  border-color: rgba(113, 113, 122, 0.8);
}

.sidebar-footer {
  padding: 16px 20px;
  border-top: 1px solid rgba(51, 65, 85, 0.5);
  background: rgba(15, 23, 42, 0.8);
  backdrop-filter: blur(10px);
}

.footer-content {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
  color: #a0aec0;
}

/* Information Panel */
.info-panel {
  padding: 16px 20px;
  height: 65px;
  background: rgba(15, 23, 42, 0.9);
  display: flex;
  align-items: center;
  border-top: 1px solid rgba(51, 65, 85, 0.5);
  backdrop-filter: blur(10px);
}

.info-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.connection-status {
  display: flex;
  align-items: center;
}

.status-indicator {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-radius: 20px;
  font-size: 0.8rem;
  font-weight: 500;
  transition: all 0.2s ease;
}

.status-indicator.connected {
  background: rgba(16, 185, 129, 0.1);
  color: #10b981;
  border: 1px solid rgba(16, 185, 129, 0.3);
}

.status-indicator.disconnected {
  background: rgba(239, 68, 68, 0.1);
  color: #ef4444;
  border: 1px solid rgba(239, 68, 68, 0.3);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  transition: all 0.2s ease;
}

.status-indicator.connected .status-dot {
  background-color: #10b981;
  box-shadow: 0 0 8px rgba(16, 185, 129, 0.5);
}

.status-indicator.disconnected .status-dot {
  background-color: #ef4444;
  box-shadow: 0 0 8px rgba(239, 68, 68, 0.5);
}

.status-text {
  font-size: 0.8rem;
  font-weight: 500;
}

.logout-btn {
  background: rgba(71, 85, 105, 0.4);
  color: #cbd5e1;
  border: 1px solid rgba(100, 116, 139, 0.3);
  border-radius: 8px;
  width: 36px;
  height: 36px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.logout-btn:hover {
  background: rgba(239, 68, 68, 0.2);
  color: #ef4444;
  border-color: rgba(239, 68, 68, 0.4);
  transform: translateY(-1px);
}

/* 滚动条样式 */
.tables-list::-webkit-scrollbar {
  width: 6px;
}

.tables-list::-webkit-scrollbar-track {
  background: rgba(15, 23, 42, 0.5);
  border-radius: 3px;
}

.tables-list::-webkit-scrollbar-thumb {
  background: rgba(51, 65, 85, 0.8);
  border-radius: 3px;
}

.tables-list::-webkit-scrollbar-thumb:hover {
  background: rgba(71, 85, 105, 0.9);
}
</style>
