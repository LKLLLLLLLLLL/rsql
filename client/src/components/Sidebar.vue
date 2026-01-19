<!-- Sidebar.vue -->
<template>
  <div class="sidebar">
    <div class="sidebar-header">
      <h2>Database Management</h2>
    </div>
    <div class="tables-buttons">
      <div class="tables-btn create" :class="{ active: activeButton === 'create' }" @click="emit('create')">
        <Icon :path="mdiTablePlus" size="18" />
        <span>Create New Table</span>
      </div>
      <div class="tables-btn rename" :class="{ active: activeButton === 'rename' }" @click="emit('rename')">
        <Icon :path="mdiTableEdit" size="18" />
        <span>Rename Table</span>
      </div>
      <div class="tables-btn drop" :class="{ active: activeButton === 'drop' }" @click="emit('drop')">
        <Icon :path="mdiTableRemove" size="18" />
        <span>Drop Table</span>
      </div>
      <div class="tables-btn terminal" :class="{ active: activeButton === 'terminal' }" @click="emit('terminal')">
        <Icon :path="mdiConsoleLine" size="18" />
        <span>Open Terminal</span>
      </div>
    </div>

    <div class="tables-list">
      <h3>Table List</h3>
      <div v-for="table in tables" :key="table" class="table-item" :class="{ active: currentTable === table }" @click="emit('select-table', table)">
        <span>{{ table }}</span>
        <button v-if="isDropMode" class="table-delete-btn" @click.stop="emit('delete-table', table)">
          <Icon :path="mdiTrashCanOutline" size="14" />
          删除
        </button>
      </div>
    </div>

    <div class="sidebar-footer">
      <p>Total <span id="tables-counts">{{ tables.length }}</span> tables</p>
      <p>Click table name to view data</p>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiConsoleLine,
  mdiTableEdit,
  mdiTablePlus,
  mdiTableRemove,
  mdiTrashCanOutline,
} from '@mdi/js'

const props = defineProps({
  tables: { type: Array, default: () => [] },
  currentTable: { type: String, default: '' },
  activeButton: { type: String, default: '' },
  isDropMode: { type: Boolean, default: false }
})

const emit = defineEmits(['create', 'rename', 'drop', 'terminal', 'select-table', 'delete-table'])
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
.tables-btn.terminal {
  background-color: #3c8dc3;
  color: white;
}

.tables-btn.active {
  background-color: #f08080;
  color: white;
}

.tables-list {
  padding: 20px 0;
  overflow-y: auto;
  flex-grow: 1;
}

.tables-list h3 {
  padding: 0 20px 15px;
  font-size: 1rem;
  font-weight: 500;
  color: #bdc3c7;
  border-bottom: 1px solid #34495e;
  margin-bottom: 15px;
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

.table-delete-btn {
  background-color: #e74c3c;
  color: #fff;
  border: none;
  border-radius: 12px;
  padding: 4px 10px;
  font-size: 12px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.table-delete-btn:hover {
  background-color: #c0392b;
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