<!-- TopBar.vue -->
<template>
  <div class="top-bar">
    <div class="table-info">
      <h2>{{ currentTableName }}</h2>
      <span class="table-count">{{ tableCount }} records</span>
    </div>
    
    <div class="action-buttons">
      <button class="action-btn insert" @click="emit('insert')">
        <Icon :path="mdiPlus" size="16" />
        <span>Insert</span>
      </button>
      <button class="action-btn delete" @click="emit('delete')">
        <Icon :path="mdiDelete" size="16" />
        <span>Delete</span>
      </button>
      <button class="action-btn update" @click="emit('update')">
        <Icon :path="mdiPencil" size="16" />
        <span>Update</span>
      </button>
      <button class="action-btn query" @click="emit('query')">
        <Icon :path="mdiMagnify" size="16" />
        <span>Query</span>
      </button>
      <button class="action-btn export" @click="emit('export')">
        <Icon :path="mdiDownload" size="16" />
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
  currentTableName: { type: String, default: '' },
  tableCount: { type: Number, default: 0 }
})

const emit = defineEmits(['insert', 'delete', 'update', 'query', 'export'])
</script>

<style scoped>
.top-bar {
  background-color: #ffffff;
  padding: 16px 24px;
  border-bottom: 1px solid #e3e8ef;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
}

.table-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.table-info h2 {
  font-size: 1.1rem;
  font-weight: 600;
  color: #1a1f36;
  margin: 0;
}

.table-count {
  font-size: 0.85rem;
  color: #6b7280;
  background: #f3f4f6;
  padding: 4px 8px;
  border-radius: 12px;
  display: inline-block;
  font-weight: 500;
}

.action-buttons {
  display: flex;
  gap: 8px;
  user-select: none;
}

.action-btn {
  padding: 8px 16px;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  background: #ffffff;
  color: #4b5563;
}

.action-btn:hover {
  background: #f3f4f6;
  border-color: #d1d5db;
}

.action-btn:active {
  transform: translateY(0);
}

.action-btn.insert:hover {
  background: #e0f2fe;
  color: #0284c7;
  border-color: #bae6fd;
}

.action-btn.delete:hover {
  background: #fee2e2;
  color: #dc2626;
  border-color: #fecaca;
}

.action-btn.update:hover {
  background: #e0f2fe;
  color: #0284c7;
  border-color: #bae6fd;
}

.action-btn.query:hover {
  background: #f0f9ff;
  color: #0284c7;
  border-color: #e0f2fe;
}

.action-btn.export:hover {
  background: #f0f9ff;
  color: #0284c7;
  border-color: #e0f2fe;
}

@media (max-width: 1200px) {
  .action-btn span {
    display: none;
  }

  .action-btn {
    padding: 8px;
  }
  
  .action-btn::before {
    content: attr(data-title);
    display: none;
  }
}

@media (max-width: 768px) {
  .top-bar {
    flex-direction: column;
    gap: 16px;
    align-items: stretch;
  }

  .action-buttons {
    justify-content: center;
    flex-wrap: wrap;
  }
}
</style>