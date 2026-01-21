<!-- DataTable.vue -->
<template>
  <div class="table-container" :style="containerStyle">
    <!-- delete 和 update 模式的返回按钮 -->
    <div v-if="mode === 'delete' || mode === 'update'" class="mode-header">
      <button class="back-btn" @click="emit('back')">← 返回表格视图</button>
    </div>

    <div class="table-wrapper">
      <VirtualList
        :key="renderKey"
        :headers="headers"
        :rows="rows"
        :leading-headers="leadingHeaders"
        :visible-count="15"
        :row-height="mode === 'update' ? 56 : 52"
        :max-height="maxHeight"
        :column-metadata="columnMetadata"
        class="virtual-table"
      >
        <template #leading-cell="{ rowIndex, leadingIndex }">
          <template v-if="mode === 'delete' && leadingIndex === 0">
            <div class="delete-actions">
              <button
                v-if="pendingRow !== rowIndex"
                class="delete-row-btn"
                type="button"
                @click="emit('pending-delete', rowIndex)"
              >
                Delete
              </button>
              <template v-else>
                <button
                  class="cancel-delete-btn"
                  type="button"
                  @click="emit('cancel-delete')"
                >
                  Cancel
                </button>
                <button
                  class="confirm-delete-btn"
                  type="button"
                  @click="emit('confirm-delete', rowIndex)"
                >
                  Confirm
                </button>
              </template>
            </div>
          </template>
          <template v-else-if="mode === 'update' && leadingIndex === 0">
            <div class="update-actions">
              <button
                v-if="editingRow !== rowIndex"
                class="update-row-btn"
                type="button"
                @click="emit('start-update', rowIndex)"
              >
                Update
              </button>
              <template v-else>
                <button
                  class="cancel-update-btn"
                  type="button"
                  @click="emit('cancel-update')"
                >
                  Cancel
                </button>
                <button
                  class="confirm-update-btn"
                  type="button"
                  @click="emit('confirm-update', rowIndex)"
                >
                  Confirm
                </button>
              </template>
            </div>
          </template>
          <template v-else>
            <span class="row-index">{{ rowIndex + 1 }}</span>
          </template>
        </template>

        <template v-if="mode === 'update'" #cell="{ value, rowIndex, colIndex, header }">
          <template v-if="editingRow === rowIndex">
            <input
              class="update-value"
              type="text"
              :value="getDraftValue(colIndex)"
              @input="emit('update-draft', { colIndex, value: $event.target.value })"
              :data-column="header"
              :placeholder="getPlaceholder(header)"
            />
          </template>
          <template v-else>
            <span class="cell-value">{{ value }}</span>
          </template>
        </template>

        <template v-else #cell="{ value }">
          <span class="cell-value">{{ value }}</span>
        </template>
      </VirtualList>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed, ref, onMounted } from 'vue'
import VirtualList from './List.vue'
import Icon from './Icon.vue'
import { mdiTable } from '@mdi/js'

const props = defineProps({
  headers: { type: Array, default: () => [] },
  rows: { type: Array, default: () => [] },
  currentTableName: { type: String, default: '' },
  mode: { type: String, default: 'view' }, // 'view', 'delete', 'update'
  pendingRow: { type: Number, default: null },
  editingRow: { type: Number, default: null },
  draftValues: { type: Array, default: () => [] },
  renderKey: { type: Number, default: 0 },
  columnMetadata: { type: Array, default: () => [] },
  maxHeight: { type: [Number, String, null], default: null },
  compact: { type: Boolean, default: false }
})

const emit = defineEmits([
  'pending-delete',
  'cancel-delete',
  'confirm-delete',
  'start-update',
  'cancel-update',
  'confirm-update',
  'update-draft',
  'back'
])

const updateTime = ref(new Date())

const recordsCount = computed(() => props.rows.length)
const formattedUpdateTime = computed(() => {
  return updateTime.value.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
})

const leadingHeaders = computed(() => {
  if (props.mode === 'delete') return ['Action', '#']
  if (props.mode === 'update') return ['Action', '#']
  return ['#']
})

const containerStyle = computed(() => {
  if (props.compact) {
    return { minHeight: 'auto', height: 'auto' }
  }
  return {}
})

function getDraftValue(colIndex) {
  return props.draftValues[colIndex] || ''
}

function getPlaceholder(headerName) {
  const meta = props.columnMetadata.find(h => h.name === headerName)
  if (!meta) return ''
  return meta.ableToBeNULL ? 'Can be null' : 'Required'
}

onMounted(() => {
  // 更新时间
  updateTime.value = new Date()
})
</script>

<style scoped>
.table-container {
  background-color: #ffffff;
  overflow: hidden;
  height: 100%;
  display: flex;
  flex-direction: column;
  border: 1px solid #e3e8ef;
  min-height: 400px;
}

.mode-header {
  padding: 12px 16px;
  border-bottom: 1px solid #e3e8ef;
  background-color: #f9fafb;
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}

.back-btn {
  padding: 8px 16px;
  background: transparent;
  color: #6b7280;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.9rem;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 6px;
}

.back-btn:hover {
  background-color: #ffffff;
  color: #1a1f36;
  border-color: #9ca3af;
}

.page-header {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  background: #f8fafc;
  flex-shrink: 0;
}

.header-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.page-header h2 {
  font-size: 1.1rem;
  color: #1a1f36;
  margin: 0;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 12px;
  text-align: center;
  /* justify-content: center; */
}

.header-info {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 24px;
  font-size: 0.85rem;
  flex-wrap: wrap;
}

.record-count {
  color: #4b5563;
  font-weight: 500;
  background: #f3f4f6;
  padding: 4px 12px;
  border-radius: 12px;
  white-space: nowrap;
  width: fit-content;
  min-width: fit-content;
  max-width: fit-content;
}

.last-update {
  color: #6b7280;
  text-align: center;
  white-space: nowrap;
  width: fit-content;
  min-width: fit-content;
  max-width: fit-content;
}

.table-wrapper {
  flex: 1;
  overflow: hidden;
  min-height: 0;
}

.virtual-table {
  height: 100%;
  width: 100%;
}

/* 单元格内容居中样式 */
:deep(.table-cell) {
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 12px 8px;
}

:deep(.table-header-cell) {
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  font-weight: 600;
  color: #374151;
  background-color: #f9fafb;
  padding: 12px 8px;
  border-bottom: 2px solid #e5e7eb;
}

:deep(.table-row) {
  border-bottom: 1px solid #f3f4f6;
}

:deep(.table-row:hover) {
  background-color: #f9fafb;
}

.delete-actions,
.update-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  width: 100%;
  padding: 0 8px;
}

.delete-row-btn,
.update-row-btn {
  padding: 8px 12px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.85rem;
  min-width: 80px;
  text-align: center;
  transition: all 0.2s ease;
  background: #f3f4f6;
  color: #4b5563;
}

.delete-row-btn:hover {
  background: #fee2e2;
  color: #dc2626;
}

.update-row-btn:hover {
  background: #e0f2fe;
  color: #0284c7;
}

.cancel-delete-btn,
.cancel-update-btn {
  padding: 8px 12px;
  background: #f3f4f6;
  color: #6b7280;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.85rem;
  min-width: 80px;
  transition: all 0.2s ease;
}

.cancel-delete-btn:hover,
.cancel-update-btn:hover {
  background: #e5e7eb;
}

.confirm-delete-btn {
  padding: 8px 12px;
  background: #fee2e2;
  color: #dc2626;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.85rem;
  min-width: 80px;
  transition: all 0.2s ease;
}

.confirm-delete-btn:hover {
  background: #fecaca;
}

.confirm-update-btn {
  padding: 8px 12px;
  background: #e0f2fe;
  color: #0284c7;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.85rem;
  min-width: 80px;
  transition: all 0.2s ease;
}

.confirm-update-btn:hover {
  background: #bae6fd;
}

.update-value {
  padding: 8px 12px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  font-size: 0.9rem;
  width: 100%;
  max-width: 200px;
  transition: all 0.2s ease;
  background: #ffffff;
  color: #1a1f36;
  text-align: center;
}

.update-value:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.row-index {
  font-weight: 500;
  color: #6b7280;
}

.cell-value {
  color: #1a1f36;
  word-break: break-word;
  max-width: 100%;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .page-header {
    padding: 16px;
  }

  .header-content {
    gap: 8px;
  }

  .header-info {
    flex-direction: column;
    gap: 12px;
    align-items: center;
  }

  .page-header h2 {
    font-size: 1rem;
    justify-content: center;
  }

  .record-count,
  .last-update {
    font-size: 0.8rem;
  }
}

@media (max-width: 480px) {
  .page-header h2 {
    flex-direction: column;
    gap: 8px;
    text-align: center;
  }
}
</style>
