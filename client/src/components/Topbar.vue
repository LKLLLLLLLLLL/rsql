<!-- TopBar.vue -->
<template>
  <div class="top-bar">
    <h1>Current Table: <span>{{ currentTableName }}</span></h1>
    
    <div class="action-buttons">
      <button class="action-btn insert" @click="emit('insert')">
        <Icon :path="mdiPlus" size="18" />
        <span>Insert</span>
      </button>
      <button class="action-btn delete" @click="emit('delete')">
        <Icon :path="mdiDelete" size="18" />
        <span>Delete</span>
      </button>
      <button class="action-btn update" @click="emit('update')">
        <Icon :path="mdiPencil" size="18" />
        <span>Update</span>
      </button>
      <button class="action-btn query" @click="emit('query')">
        <Icon :path="mdiMagnify" size="18" />
        <span>Query</span>
      </button>
      <button class="action-btn export" @click="emit('export')">
        <Icon :path="mdiDownload" size="18" />
        <span>Export</span>
      </button>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiDelete,
  mdiDownload,
  mdiMagnify,
  mdiPencil,
  mdiPlus,
} from '@mdi/js'

const props = defineProps({
  currentTableName: { type: String, default: '' }
})

const emit = defineEmits(['insert', 'delete', 'update', 'query', 'export'])
</script>

<style scoped>
.top-bar {
  background-color: white;
  padding: 20px 30px;
  box-shadow: 0 3px 10px rgba(0, 0, 0, 0.08);
  display: flex;
  justify-content: space-between;
  align-items: center;
  z-index: 5;
}

.top-bar h1 {
  font-size: 1.8rem;
  color: #2c3e50;
}

.action-buttons {
  display: flex;
  gap: 12px;
  user-select: none;
}

.action-btn {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s ease;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.95rem;
}

.action-btn.insert {
  background-color: #2ecc71;
  color: white;
}

.action-btn.delete {
  background-color: #e74c3c;
  color: white;
}

.action-btn.update {
  background-color: #3498db;
  color: white;
}

.action-btn.query {
  background-color: #9b59b6;
  color: white;
}

.action-btn.export {
  background-color: #f39c12;
  color: white;
}

.action-btn:hover {
  transform: translateY(-2px);
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
}

.action-btn:active {
  transform: translateY(0);
}

@media (max-width: 1200px) {
  .action-btn span {
    display: none;
  }

  .action-btn {
    padding: 12px 15px;
  }
}

@media (max-width: 768px) {
  .top-bar {
    flex-direction: column;
    gap: 20px;
  }

  .action-buttons {
    flex-wrap: wrap;
    justify-content: center;
  }
}
</style>