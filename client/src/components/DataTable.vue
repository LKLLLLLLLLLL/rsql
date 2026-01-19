<!-- DataTable.vue -->
<template>
  <div class="table-container" :class="{ 'delete-table-container': mode === 'delete', 'update-table-container': mode === 'update' }">
    <div class="table-header">
      <h3>{{ currentTableName }} Table Data</h3>
      <div class="table-info">
        <span>Total {{ recordsCount }} records</span>
        <span>Updated on <span id="update-time">1970-01-01 00:00</span></span>
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
            {{ rowIndex + 1 }}
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
            {{ value }}
          </template>
        </template>
      </VirtualList>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed } from 'vue'
import VirtualList from './List.vue'

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
  background-color: white;
  border-radius: 10px;
  overflow: hidden;
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.05);
  height: 100%;
  flex-direction: column;
  display: flex;
  min-height: 360px;
}

.table-scroll-wrapper {
  flex-grow: 1;
  overflow-y: auto;
  overflow-x: auto;
}

.table-header {
  padding: 20px 25px;
  background-color: #f8f9fa;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
}

.table-header h3 {
  color: #2c3e50;
  font-size: 1.3rem;
}

.table-info {
  display: flex;
  gap: 20px;
  color: #7f8c8d;
  font-size: 0.9rem;
}

.delete-table-container th:first-child,
.delete-table-container td:first-child {
  width: 180px;
  min-width: 180px;
  max-width: 180px;
  text-align: center;
  vertical-align: middle;
}

.update-table-container th:first-child,
.update-table-container td:first-child {
  width: 180px;
  min-width: 180px;
  max-width: 180px;
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
  font-weight: 600;
  font-size: 0.85rem;
  width: 80px;
  text-align: center;
}

.delete-row-btn {
  background-color: #e74c3c;
  color: white;
}

.delete-row-btn:hover {
  background-color: #c0392b;
}

.update-row-btn {
  background-color: #3498db;
  color: white;
}

.update-row-btn:hover {
  background-color: #217dbb;
}

.cancel-delete-btn,
.cancel-update-btn {
  padding: 6px 12px;
  background-color: #95a5a6;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 600;
  font-size: 0.85rem;
  width: 80px;
}

.cancel-delete-btn:hover,
.cancel-update-btn:hover {
  background-color: #7f8c8d;
}

.confirm-delete-btn {
  padding: 6px 12px;
  background-color: #e74c3c;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 600;
  font-size: 0.85rem;
  width: 80px;
}

.confirm-delete-btn:hover {
  background-color: #c0392b;
}

.confirm-update-btn {
  padding: 6px 12px;
  background-color: #3498db;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 600;
  font-size: 0.85rem;
  width: 80px;
}

.confirm-update-btn:hover {
  background-color: #217dbb;
}

.update-value {
  padding: 8px 10px;
  border: 1px solid #dfe4ea;
  border-radius: 6px;
  font-size: 0.95rem;
  min-width: 160px;
}
</style>