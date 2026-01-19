<!-- DataTable.vue -->
<template>
  <div class="table-container" :class="{ 'delete-table-container': mode === 'delete', 'update-table-container': mode === 'update' }">
    <div class="page-header">
      <div class="header-content">
        <h2><Icon :path="mdiTable" size="20" /> {{ currentTableName }} Table Data</h2>
        <div class="header-info">
          <span class="record-count">Total {{ recordsCount }} records</span>
          <span class="last-update">Updated on <span id="update-time">1970-01-01 00:00</span></span>
        </div>
      </div>
    </div>
    
    <div class="table-scroll-wrapper">
      <VirtualList
        :key="renderKey"
        :headers="headers"
        :rows="rows"
        :leading-headers="leadingHeaders"
        :visible-count="12"
        :row-height="mode === 'update' ? 52 : 48"
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
      </VirtualList>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed } from 'vue'
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
  columnMetadata: { type: Array, default: () => [] }
})

const emit = defineEmits([
  'pending-delete',
  'cancel-delete',
  'confirm-delete',
  'start-update',
  'cancel-update',
  'confirm-update',
  'update-draft'
])

const recordsCount = computed(() => props.rows.length)
const leadingHeaders = computed(() => {
  if (props.mode === 'delete') return ['Delete', '#']
  if (props.mode === 'update') return ['Update', '#']
  return ['#']
})

function getDraftValue(colIndex) {
  return props.draftValues[colIndex] || ''
}

function getPlaceholder(headerName) {
  const meta = props.columnMetadata.find(h => h.name === headerName)
  if (!meta) return ''
  return meta.ableToBeNULL ? '可为空' : '必填'
}
</script>

<style scoped>
.table-container {
  background-color: #ffffff;
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
  height: 100%;
  flex-direction: column;
  display: flex;
  min-height: 360px;
  border: 1px solid #e3e8ef;
}

.table-scroll-wrapper {
  flex-grow: 1;
  overflow-y: auto;
  overflow-x: auto;
}

.page-header {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  background: #f8fafc;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-content {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.page-header h2 {
  font-size: 1.1rem;
  color: #1a1f36;
  margin: 0;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 12px;
}

.header-info {
  display: flex;
  gap: 20px;
  font-size: 0.85rem;
}

.record-count {
  color: #4b5563;
  font-weight: 500;
  background: #f3f4f6;
  padding: 4px 12px;
  border-radius: 12px;
}

.last-update {
  color: #6b7280;
}

.delete-table-container th:first-child,
.delete-table-container td:first-child {
  width: 160px;
  min-width: 160px;
  max-width: 160px;
  text-align: center;
  vertical-align: middle;
}

.update-table-container th:first-child,
.update-table-container td:first-child {
  width: 160px;
  min-width: 160px;
  max-width: 160px;
  text-align: center;
  vertical-align: middle;
}

.delete-actions,
.update-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  width: 100%;
}

.delete-row-btn,
.update-row-btn {
  padding: 6px 12px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.8rem;
  width: 70px;
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
  padding: 6px 12px;
  background: #f3f4f6;
  color: #6b7280;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.8rem;
  transition: all 0.2s ease;
}

.cancel-delete-btn:hover,
.cancel-update-btn:hover {
  background: #e5e7eb;
}

.confirm-delete-btn {
  padding: 6px 12px;
  background: #fee2e2;
  color: #dc2626;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.8rem;
  transition: all 0.2s ease;
}

.confirm-delete-btn:hover {
  background: #fecaca;
}

.confirm-update-btn {
  padding: 6px 12px;
  background: #e0f2fe;
  color: #0284c7;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.8rem;
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
  min-width: 160px;
  transition: all 0.2s ease;
  background: #ffffff;
  color: #1a1f36;
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
}
</style>