<!-- Sidebar.vue -->
<template>
  <div class="sidebar">
    <div class="sidebar-header">
      <div class="header-icon">
        <Icon :path="mdiDatabase" size="24" />
      </div>
      <div class="header-content">
        <h2>Database Management</h2>
        <p class="header-subtitle">Manage your RSQL databases</p>
      </div>
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
        <div class="tables-btn list" :class="{ active: showTableList }" @click="toggleTableList">
          <div class="btn-icon">
            <Icon :path="mdiFormatListBulleted" size="18" />
          </div>
          <span>Table List</span>
        </div>
      </div>
    </div>

    <div class="tables-list" :class="{ collapsed: !showTableList }">
      <div class="list-header">
        <h3>TABLE LIST</h3>
        <span class="table-count">{{ tables.length }} tables</span>
      </div>
      <div v-for="table in tables" :key="table" class="table-item" :class="{ active: currentTable === table }" @click="handleTableSelect(table)">
        <div class="table-content">
          <div class="table-icon">
            <Icon :path="mdiTable" size="16" />
          </div>
          <span class="table-name">{{ table }}</span>
          <button v-if="isDropMode" class="table-delete-btn" @click.stop="emit('delete-table', table)">
            <Icon :path="mdiTrashCanOutline" size="14" />
          </button>
        </div>
      </div>

      <div class="sidebar-footer">
        <div class="footer-content">
          <Icon :path="mdiInformationOutline" size="16" />
          <span>Click table name to view data</span>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiDatabase,
  mdiConsoleLine,
  mdiTableEdit,
  mdiTablePlus,
  mdiTableRemove,
  mdiFormatListBulleted,
  mdiTable,
  mdiInformationOutline,
  mdiTrashCanOutline,
} from '@mdi/js'

const props = defineProps({
  tables: { type: Array, default: () => [] },
  currentTable: { type: String, default: '' },
  activeButton: { type: String, default: '' },
  isDropMode: { type: Boolean, default: false }
})

const emit = defineEmits(['create', 'rename', 'drop', 'terminal', 'select-table', 'list-toggle', 'delete-table', 'clear-selection'])

const showTableList = ref(false)

function toggleTableList() {
  showTableList.value = !showTableList.value
  emit('list-toggle', showTableList.value)
  // 当展开表列表时，清空当前选择
  if (showTableList.value) {
    emit('clear-selection')
  }
}

function handleButtonClick(button) {
  // 当点击操作按钮时，折叠表列表
  if (showTableList.value) {
    showTableList.value = false
    emit('list-toggle', false)
  }
  
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
  // 选择表时保持表列表展开
  emit('select-table', table)
}
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
  background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
  color: #f1f5f9;
  display: flex;
  align-items: center;
  gap: 14px;
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
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(99, 102, 241, 0.2);
  box-shadow: 0 4px 12px rgba(99, 102, 241, 0.1);
}

.header-content {
  flex: 1;
}

.sidebar-header h2 {
  font-size: 1.3rem;
  font-weight: 700;
  margin: 0 0 4px 0;
  color: #f8fafc;
  letter-spacing: -0.01em;
  background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.header-subtitle {
  font-size: 0.85rem;
  color: #a0aec0;
  margin: 0;
  font-weight: 400;
  letter-spacing: 0.02em;
}

.functions-section {
  padding: 20px 0;
  background: #0f172a;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.section-header {
  padding: 0 20px 10px 30px;
  margin-bottom: 6px;
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
  gap: 6px;
}

.tables-btn {
  padding: 12px 16px;
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
}

.tables-btn:hover {
  background: rgba(51, 65, 85, 0.6);
  color: #f1f5f9;
  border-color: rgba(71, 85, 105, 0.5);
  transform: translateY(-1px);
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
  transform: translateY(-1px);
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
  padding: 20px;
  overflow-y: auto;
  flex-grow: 1;
  background: #0f172a;
  transition: all 0.3s ease;
}

.tables-list.collapsed {
  padding: 0;
  flex-grow: 0;
  display: none;
}

.list-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  padding: 0 10px;
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
  height: 46px;
  min-height: 46px;
  padding: 0 12px;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  display: flex;
  align-items: center;
  user-select: none;
  position: relative;
  background: transparent;
  color: #cbd5e1;
  border-radius: 8px;
  margin-bottom: 6px;
  border: 1px solid transparent;
}

.table-item:hover {
  background: rgba(51, 65, 85, 0.6);
  color: #f1f5f9;
  border-color: rgba(71, 85, 105, 0.5);
  transform: translateY(-1px);
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
  transform: translateY(-1px);
}

.table-content {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  height: 100%;
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
  transform: scale(1.05);
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