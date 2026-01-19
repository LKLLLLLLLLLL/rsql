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
        <div class="tables-btn terminal" :class="{ active: activeButton === 'terminal' }" @click="handleButtonClick('terminal')">
          <div class="btn-icon">
            <Icon :path="mdiConsoleLine" size="18" />
          </div>
          <span>Open Terminal</span>
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
        <h3>Table List</h3>
        <span class="table-count">{{ tables.length }} tables</span>
      </div>
      <div v-for="table in tables" :key="table" class="table-item" :class="{ active: !showTableList && currentTable === table }" @click="handleTableSelect(table)">
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
  background: #1e293b;
  color: #cbd5e1;
  display: flex;
  flex-direction: column;
  border-right: 1px solid #334155;
  height: 100vh;
  position: sticky;
  top: 0;
  z-index: 10;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
  box-sizing: border-box;
}

.sidebar-header {
  padding: 24px 20px;
  background: #0f172a;
  color: #f1f5f9;
  display: flex;
  align-items: center;
  gap: 12px;
  position: relative;
  overflow: hidden;
  border-bottom: 1px solid #334155;
}

.header-icon {
  padding: 10px;
  background: rgba(99, 102, 241, 0.1);
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.header-content {
  flex: 1;
}

.sidebar-header h2 {
  font-size: 1.25rem;
  font-weight: 600;
  margin: 0 0 4px 0;
  color: #e2e8f0;
  letter-spacing: -0.025em;
}

.header-subtitle {
  font-size: 0.85rem;
  color: #94a3b8;
  margin: 0;
  font-weight: 400;
}

.functions-section {
  padding: 20px 0;
  background: #0f172a;
  border-bottom: 1px solid #334155;
  border-top: 1px solid #334155;
}

.section-header {
  padding: 0 20px 12px;
  margin-bottom: 4px;
}

.section-header h3 {
  font-size: 0.75rem;
  font-weight: 600;
  color: #64748b;
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.tables-buttons {
  padding: 0 16px;
  user-select: none;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.tables-btn {
  padding: 14px 16px;
  cursor: pointer;
  border-radius: 6px;
  font-weight: 500;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 12px;
  color: #94a3b8;
  background: transparent;
  border: 1px solid transparent;
  position: relative;
  overflow: hidden;
}

.tables-btn:hover {
  background: #334155;
  color: #e2e8f0;
  border-color: #475569;
}

.tables-btn.active {
  background: #334155;
  color: #f8fafc;
  border-color: #64748b;
  box-shadow: inset 0 2px 4px 0 rgba(0, 0, 0, 0.3);
}

.tables-btn.active:hover {
  background: #475569;
  border-color: #94a3b8;
}

.btn-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border-radius: 4px;
  background: rgba(148, 163, 184, 0.1);
  color: #94a3b8;
  transition: all 0.2s ease;
}

.tables-btn:hover .btn-icon,
.tables-btn.active .btn-icon {
  background: #64748b;
  color: #f8fafc;
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
  padding: 0 4px;
}

.list-header h3 {
  font-size: 0.875rem;
  font-weight: 600;
  color: #64748b;
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.table-count {
  font-size: 0.75rem;
  font-weight: 500;
  color: #64748b;
  background: #1e293b;
  padding: 4px 10px;
  border-radius: 20px;
}

.table-item {
  height: 48px;
  min-height: 48px;
  padding: 0 12px;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  user-select: none;
  position: relative;
  background: transparent;
  color: #cbd5e1;
  border-radius: 6px;
  margin-bottom: 6px;
  border: 1px solid transparent;
}

.table-item:hover {
  background: #334155;
  color: #e2e8f0;
  border-color: #475569;
}

.table-item.active {
  background: #334155;
  color: #f8fafc;
  font-weight: 600;
  border: 1px solid #64748b;
  box-shadow: inset 0 2px 4px 0 rgba(0, 0, 0, 0.3);
}

.table-item.active:hover {
  background: #475569;
  border-color: #94a3b8;
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
  border-radius: 4px;
  background: rgba(148, 163, 184, 0.1);
  color: #94a3b8;
  transition: all 0.2s ease;
}

.table-item:hover .table-icon,
.table-item.active .table-icon {
  background: #64748b;
  color: #f8fafc;
}

.table-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.95rem;
}

.table-delete-btn {
  background: #3f3f46;
  color: #f43f5e;
  border: 1px solid #52525b;
  border-radius: 4px;
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
  background: #52525b;
  color: #fb7185;
  border-color: #71717a;
}

.sidebar-footer {
  padding: 16px 20px;
  border-top: 1px solid #334155;
  background: #0f172a;
}

.footer-content {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
  color: #64748b;
}

/* 滚动条样式 */
.tables-list::-webkit-scrollbar {
  width: 6px;
}

.tables-list::-webkit-scrollbar-track {
  background: #0f172a;
  border-radius: 3px;
}

.tables-list::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 3px;
}

.tables-list::-webkit-scrollbar-thumb:hover {
  background: #475569;
}
</style>