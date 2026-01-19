<!-- Sidebar.vue -->
<template>
  <div class="sidebar">
    <div class="sidebar-header">
      <h2>Database Management</h2>
    </div>
    <div class="tables-buttons">
      <div class="tables-btn create" :class="{ active: activeButton === 'create' }" @click="handleButtonClick('create')">
        <Icon :path="mdiTablePlus" size="18" />
        <span>Create New Table</span>
      </div>
      <div class="tables-btn rename" :class="{ active: activeButton === 'rename' }" @click="handleButtonClick('rename')">
        <Icon :path="mdiTableEdit" size="18" />
        <span>Rename Table</span>
      </div>
      <div class="tables-btn drop" :class="{ active: activeButton === 'drop' }" @click="handleButtonClick('drop')">
        <Icon :path="mdiTableRemove" size="18" />
        <span>Drop Table</span>
      </div>
      <div class="tables-btn terminal" :class="{ active: activeButton === 'terminal' }" @click="handleButtonClick('terminal')">
        <Icon :path="mdiConsoleLine" size="18" />
        <span>Open Terminal</span>
      </div>
      <div class="tables-btn list" :class="{ active: showTableList }" @click="toggleTableList">
        <Icon :path="mdiFormatListBulleted" size="18" />
        <span>Table List</span>
      </div>
    </div>

    <div class="tables-list" :class="{ collapsed: !showTableList }">
      <div v-for="table in tables" :key="table" class="table-item" :class="{ active: !showTableList && currentTable === table }" @click="handleTableSelect(table)">
        <span>{{ table }}</span>
      </div>

      <div class="sidebar-footer">
      <p>Total <span id="tables-counts">{{ tables.length }}</span> tables</p>
      <p>Click table name to view data</p>
    </div>
    </div>

  </div>
</template>

<script setup>
import { ref, defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiConsoleLine,
  mdiTableEdit,
  mdiTablePlus,
  mdiTableRemove,
  mdiFormatListBulleted,
} from '@mdi/js'

const props = defineProps({
  tables: { type: Array, default: () => [] },
  currentTable: { type: String, default: '' },
  activeButton: { type: String, default: '' }
})

const emit = defineEmits(['create', 'rename', 'drop', 'terminal', 'select-table', 'list-toggle'])

const showTableList = ref(false)

function toggleTableList() {
  showTableList.value = !showTableList.value
  emit('list-toggle', showTableList.value)
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
.sidebar {
  width: 20%;
  background-color: #2c3e50;
  color: white;
  display: flex;
  flex-direction: column;
  box-shadow: 3px 0 15px rgba(0, 0, 0, 0.1);
  z-index: 10;
}

.sidebar-header {
  padding: 25px 20px;
  background-color: #1a252f;
  border-bottom: 1px solid #34495e;
}

.sidebar-header h2 {
  font-size: 1.5rem;
  display: flex;
  align-items: center;
  gap: 10px;
}

.tables-buttons {
  border-bottom: 1px solid #34495e;
  user-select: none;
}

.tables-btn {
  padding: 15px 20px;
  cursor: pointer;
  border-bottom: 1px solid #34495e;
  font-weight: 600;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 10px;
}

.tables-btn:hover {
  border-left: 4px solid #2c3e50;
}

.tables-btn.create,
.tables-btn.drop,
.tables-btn.rename,
.tables-btn.terminal,
.tables-btn.list {
  background-color: #3c8dc3;
  color: white;
}

.tables-btn.list {
  margin-top: 100px;
}

.tables-btn.active {
  background-color: #f08080;
  color: white;
}

.tables-list {
  overflow-y: auto;
  flex-grow: 1;
  transition: all 0.3s ease;
}

.tables-list.collapsed {
  padding: 0;
  flex-grow: 0;
  display: none;
}

.table-item {
  padding: 15px 20px;
  cursor: pointer;
  transition: all 0.2s ease;
  border-left: 4px solid transparent;
  display: flex;
  align-items: center;
  justify-content: space-between;
  user-select: none;
}

.table-item:hover {
  background-color: #34495e;
  border-left: 4px solid #3498db;
}

.table-item.active {
  background-color: #34495e;
  border-left: 4px solid #3498db;
  color: #3498db;
}

.sidebar-footer {
  padding: 20px;
  border-top: 1px solid #34495e;
  font-size: 0.8rem;
  color: #7f8c8d;
}

.sidebar-footer p + p {
  margin-top: 5px;
}
</style>