<!-- TopBar.vue -->
<template>
  <div class="top-bar">
    <div class="table-info">
      <div class="title-row">
        <h2>
          <Icon :path="titleIcon" size="20" />
          {{ displayTitle }}
        </h2>
        <span v-if="showOperationStatus" class="operation-status">{{ operationStatus }}</span>
      </div>
      <span v-if="showRecordCount" class="table-count">{{ tableCount }} records</span>
    </div>
    
    <div class="action-buttons">
      <button 
        v-if="currentMode === 'table'"
        class="action-btn insert" 
        :class="{ active: currentMode === 'insert' }"
        @click="emit('insert')">
        <Icon :path="mdiPlus" size="16" />
        <span>Insert</span>
      </button>
      <button 
        v-if="currentMode === 'table'"
        class="action-btn delete" 
        :class="{ active: currentMode === 'delete' }"
        @click="emit('delete')">
        <Icon :path="mdiDelete" size="16" />
        <span>Delete</span>
      </button>
      <button 
        v-if="currentMode === 'table'"
        class="action-btn update" 
        :class="{ active: currentMode === 'update' }"
        @click="emit('update')">
        <Icon :path="mdiPencil" size="16" />
        <span>Update</span>
      </button>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed } from 'vue'
import Icon from './Icon.vue'
import {
  mdiDelete,
  mdiPencil,
  mdiPlus,
  mdiTable,
  mdiTablePlus,
  mdiTableEdit,
  mdiTableRemove,
  mdiConsole,
} from '@mdi/js'

const props = defineProps({
  currentTableName: { type: String, default: '' },
  tableCount: { type: Number, default: 0 },
  currentMode: { type: String, default: 'table' } // table, create, rename, drop, terminal, insert, delete, update, query, export
})

const emit = defineEmits(['insert', 'delete', 'update'])

// 计算显示的图标
const titleIcon = computed(() => {
  switch (props.currentMode) {
    case 'create':
      return mdiTablePlus
    case 'rename':
      return mdiTableEdit
    case 'drop':
      return mdiTableRemove
    case 'terminal':
      return mdiConsole
    case 'insert':
      return mdiPlus
    case 'delete':
      return mdiDelete
    case 'update':
      return mdiPencil
    default:
      return mdiTable
  }
})

// 计算显示的标题
const displayTitle = computed(() => {
  switch (props.currentMode) {
    case 'create':
      return 'Creating New Table'
    case 'rename':
      return 'Renaming Table'
    case 'drop':
      return 'Dropping Table'
    case 'terminal':
      return 'Terminal'
    case 'table':
      return `Table: ${props.currentTableName}`
    default:
      return props.currentTableName
  }
})

// 是否显示记录数
const showRecordCount = computed(() => {
  return ['table', 'insert', 'delete', 'update'].includes(props.currentMode)
})

// 是否显示操作状态
const showOperationStatus = computed(() => {
  return ['insert', 'delete', 'update'].includes(props.currentMode)
})

// 操作状态文本
const operationStatus = computed(() => {
  switch (props.currentMode) {
    case 'insert':
      return 'Inserting Data'
    case 'delete':
      return 'Deleting Records'
    case 'update':
      return 'Updating Records'
    default:
      return ''
  }
})

// 按钮是否禁用
const isButtonDisabled = computed(() => {
  return ['create', 'rename', 'drop', 'terminal'].includes(props.currentMode)
})
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
  min-width: 0; /* 允许子元素收缩 */
}

.title-row {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.table-info h2 {
  font-size: 1.1rem;
  font-weight: 600;
  color: #1a1f36;
  margin: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 300px; /* 限制最大宽度 */
  display: flex;
  align-items: center;
  gap: 8px;
}

.table-count {
  font-size: 0.85rem;
  color: #6b7280;
  background: #f3f4f6;
  padding: 4px 8px;
  border-radius: 12px;
  display: inline-block;
  font-weight: 500;
  white-space: nowrap;
  width: fit-content; /* 宽度跟随内容 */
  min-width: fit-content; /* 最小宽度跟随内容 */
  max-width: fit-content; /* 最大宽度跟随内容 */
}

.operation-status {
  font-size: 0.85rem;
  color: #0284c7;
  background: #e0f2fe;
  padding: 4px 8px;
  border-radius: 12px;
  display: inline-block;
  font-weight: 500;
  white-space: nowrap;
  width: fit-content;
  min-width: fit-content;
  max-width: fit-content;
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
  white-space: nowrap;
}

.action-btn:hover {
  background: #f3f4f6;
  border-color: #d1d5db;
}

.action-btn:active {
  transform: translateY(0);
}

.action-btn.disabled {
  opacity: 0.4;
  cursor: not-allowed;
  pointer-events: none;
}

.action-btn.active {
  background: #0284c7;
  color: #ffffff;
  border-color: #0284c7;
}

.action-btn.active:hover {
  background: #0369a1;
  border-color: #0369a1;
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

@media (max-width: 1200px) {
  .table-info h2 {
    max-width: 200px;
  }
  
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

  .table-info {
    align-items: center;
    text-align: center;
  }
  
  .table-info h2 {
    max-width: 100%;
  }

  .action-buttons {
    justify-content: center;
    flex-wrap: wrap;
  }
}
</style>