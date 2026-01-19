<template>
  <div class="modern-virtual-scroll" ref="scrollRef" @scroll="onScroll" :style="{ maxHeight: maxHeightPx }">
    <table class="modern-virtual-table">
      <thead class="table-header">
        <tr>
          <th 
            v-for="(header, idx) in leadingHeaders" 
            :key="`lead-h-${idx}`"
            class="header-cell leading-header"
          >
            {{ header }}
          </th>
          <th 
            v-for="(header, idx) in headers" 
            :key="`h-${idx}`"
            class="header-cell data-header"
            :style="{ minWidth: `${minWidth}px`, maxWidth: `${maxWidth}px` }"
          >
            <div class="header-content">
              <span>{{ header }}</span>
              <button class="sort-btn" @click="toggleSort(idx)">
                <Icon :path="getSortIcon(idx)" size="14" />
              </button>
            </div>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-if="rows.length === 0">
          <td :colspan="totalColumns" class="empty-cell">
            <div class="empty-state">
              <Icon :path="mdiTable" size="48" class="empty-icon" />
              <p>No data available</p>
            </div>
          </td>
        </tr>
        <template v-else>
          <tr v-if="paddingTop > 0" class="spacer-row" :style="{ height: `${paddingTop}px` }">
            <td :colspan="totalColumns"></td>
          </tr>
          <tr
            v-for="(row, localIndex) in visibleRows"
            :key="renderStart + localIndex"
            :style="{ height: `${rowHeight}px` }"
            :class="{ 'row-even': (renderStart + localIndex) % 2 === 0, 'row-odd': (renderStart + localIndex) % 2 === 1 }"
            @dblclick="onRowDoubleClick(renderStart + localIndex, row)"
          >
            <template v-for="(_, leadIdx) in leadingHeaders" :key="`lead-c-${leadIdx}`">
              <td class="data-cell leading-cell">
                <slot
                  name="leading-cell"
                  :row="row"
                  :row-index="renderStart + localIndex"
                  :leading-index="leadIdx"
                >
                  <div class="leading-content">
                    {{ renderStart + localIndex + 1 }}
                  </div>
                </slot>
              </td>
            </template>
            <td 
              v-for="(value, colIndex) in row" 
              :key="`c-${colIndex}`"
              class="data-cell data-column"
              :class="{ 'highlighted': isHighlighted(renderStart + localIndex, colIndex) }"
              @click="onCellClick(renderStart + localIndex, colIndex, value)"
            >
              <slot
                name="cell"
                :value="value"
                :row="row"
                :row-index="renderStart + localIndex"
                :col-index="colIndex"
                :header="headers[colIndex]"
              >
                <div class="cell-content">
                  <span>{{ formatValue(value) }}</span>
                </div>
              </slot>
            </td>
          </tr>
          <tr v-if="paddingBottom > 0" class="spacer-row" :style="{ height: `${paddingBottom}px` }">
            <td :colspan="totalColumns"></td>
          </tr>
        </template>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { 
  mdiTable, 
  mdiArrowUp, 
  mdiArrowDown, 
  mdiSortVariant,
  mdiInformation
} from '@mdi/js'

// Define the Icon component inside this script
const Icon = {
  name: 'Icon',
  props: {
    path: { type: String, required: true },
    size: { type: [Number, String], default: 18 },
  },
  setup(props) {
    return () => {
      return {
        __html: `<svg class="icon" width="${props.size}" height="${props.size}" viewBox="0 0 24 24" aria-hidden="true"><path d="${props.path}" fill="currentColor"/></svg>`
      }
    }
  },
  render() {
    return {
      template: `<svg class="icon" :width="size" :height="size" viewBox="0 0 24 24" aria-hidden="true"><path :d="path" fill="currentColor"/></svg>`
    }
  }
}

const props = defineProps({
  headers: { type: Array, default: () => [] },
  rows: { type: Array, default: () => [] },
  rowHeight: { type: Number, default: 48 },
  visibleCount: { type: Number, default: 12 },
  leadingHeaders: { type: Array, default: () => [] },
  maxHeight: { type: Number, default: '720' },
  buffer: { type: Number, default: 4 },
  minWidth: { type: Number, default: 140 },
  maxWidth: { type: Number, default: 240 },
  enableSorting: { type: Boolean, default: true },
  enableHighlighting: { type: Boolean, default: false }
})

const emit = defineEmits(['row-click', 'cell-click', 'row-double-click'])

const scrollRef = ref(null)
const startIndex = ref(0)
const sortState = ref({ columnIndex: -1, direction: null }) // null: none, 'asc': ascending, 'desc': descending
const highlightedCells = ref(new Set())

const maxHeightPx = computed(() => 
  props.maxHeight != null ? `${props.maxHeight}px` : `${props.visibleCount * props.rowHeight}px`
)

const totalColumns = computed(() => props.headers.length + props.leadingHeaders.length)

const bufferSize = computed(() => Number.isFinite(props.buffer) ? props.buffer : 4)

const safeStart = computed(() => Math.max(startIndex.value - bufferSize.value, 0))

const endIndex = computed(() => 
  Math.min(startIndex.value + props.visibleCount + bufferSize.value, props.rows.length)
)

const renderStart = computed(() => safeStart.value)

const visibleRows = computed(() => props.rows.slice(safeStart.value, endIndex.value))

const paddingTop = computed(() => safeStart.value * props.rowHeight)

const paddingBottom = computed(() => 
  Math.max(props.rows.length - endIndex.value, 0) * props.rowHeight
)

// Sorting methods
const toggleSort = (columnIndex) => {
  if (!props.enableSorting) return;
  
  if (sortState.value.columnIndex === columnIndex) {
    // Cycle through sort states: asc -> desc -> none
    if (sortState.value.direction === 'asc') {
      sortState.value.direction = 'desc';
    } else if (sortState.value.direction === 'desc') {
      sortState.value.direction = null;
      sortState.value.columnIndex = -1;
    } else {
      sortState.value.direction = 'asc';
    }
  } else {
    sortState.value.columnIndex = columnIndex;
    sortState.value.direction = 'asc';
  }
  
  // In a real implementation, you would sort the data here
  console.log(`Sorting by column ${columnIndex} in ${sortState.value.direction} order`);
}

const getSortIcon = (columnIndex) => {
  if (sortState.value.columnIndex !== columnIndex) {
    return mdiSortVariant;
  }
  
  return sortState.value.direction === 'asc' ? mdiArrowUp : mdiArrowDown;
}

// Cell formatting
const formatValue = (value) => {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

// Cell highlighting
const isHighlighted = (rowIndex, colIndex) => {
  if (!props.enableHighlighting) return false;
  return highlightedCells.value.has(`${rowIndex}-${colIndex}`);
}

// Event handlers
const onScroll = () => {
  const el = scrollRef.value
  if (!el) return
  startIndex.value = Math.floor(el.scrollTop / props.rowHeight)
}

const onCellClick = (rowIndex, colIndex, value) => {
  if (props.enableHighlighting) {
    const cellKey = `${rowIndex}-${colIndex}`;
    if (highlightedCells.value.has(cellKey)) {
      highlightedCells.value.delete(cellKey);
    } else {
      highlightedCells.value.add(cellKey);
    }
  }
  emit('cell-click', { rowIndex, colIndex, value });
}

const onRowDoubleClick = (rowIndex, rowData) => {
  emit('row-double-click', { rowIndex, rowData });
}

// Watch for data changes
watch(
  () => props.rows.length,
  () => {
    const el = scrollRef.value
    if (el && startIndex.value > props.rows.length) {
      startIndex.value = 0
      el.scrollTop = 0
    }
  }
)

let resizeObserver = null
onMounted(() => {
  const el = scrollRef.value
  if (!el || typeof ResizeObserver === 'undefined') return
  resizeObserver = new ResizeObserver(() => {
    startIndex.value = Math.floor((el.scrollTop || 0) / props.rowHeight)
  })
  resizeObserver.observe(el)
})

onBeforeUnmount(() => {
  if (resizeObserver && scrollRef.value) {
    resizeObserver.unobserve(scrollRef.value)
  }
})
</script>

<style scoped>
.modern-virtual-scroll {
  width: 100%;
  overflow-y: auto;
  overflow-x: auto;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.modern-virtual-table {
  width: max-content;
  min-width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.table-header {
  position: sticky;
  top: 0;
  z-index: 10;
}

.header-cell {
  position: relative;
  padding: 12px 16px;
  background: #f1f5f9;
  color: #334155;
  font-weight: 600;
  text-align: left;
  border-bottom: 2px solid #cbd5e1;
  border-right: 1px solid #e2e8f0;
  font-size: 0.9rem;
}

.header-cell:first-child {
  border-top-left-radius: 8px;
}

.header-cell:last-child {
  border-top-right-radius: 8px;
  border-right: none;
}

.leading-header {
  background: #e2e8f0;
  min-width: 50px;
  width: 50px;
}

.data-header {
  background: #f1f5f9;
}

.header-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.sort-btn {
  background: none;
  border: none;
  color: #64748b;
  cursor: pointer;
  padding: 4px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  opacity: 0.7;
  transition: all 0.2s ease;
}

.sort-btn:hover {
  opacity: 1;
  background: rgba(255, 255, 255, 0.3);
  color: #475569;
}

.data-cell {
  padding: 10px 16px;
  border-bottom: 1px solid #e2e8f0;
  border-right: 1px solid #e2e8f0;
  vertical-align: middle;
  transition: background-color 0.2s ease;
}

.data-cell:last-child {
  border-right: none;
}

.leading-cell {
  background: #f8fafc;
  text-align: center;
  font-weight: 500;
  color: #64748b;
}

.leading-content {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}

.data-column {
  background: white;
}

.row-even {
  background-color: #f8fafc;
}

.row-odd {
  background-color: white;
}

.data-cell:hover {
  background-color: #fffbeb;
}

.data-cell.highlighted {
  background-color: #fef3c7;
  border: 1px solid #f59e0b;
  position: relative;
}

.data-cell.highlighted::after {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  border: 2px solid #f59e0b;
  pointer-events: none;
}

.cell-content {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.spacer-row {
  display: block;
}

.empty-cell {
  text-align: center;
  padding: 40px 20px;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  color: #94a3b8;
}

.empty-icon {
  color: #cbd5e1;
}

/* Responsive styles */
@media (max-width: 768px) {
  .header-cell, .data-cell {
    padding: 8px 10px;
    font-size: 0.8rem;
  }
  
  .header-content {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
  
  .sort-btn {
    align-self: flex-end;
    margin-top: -20px;
  }
}
</style>