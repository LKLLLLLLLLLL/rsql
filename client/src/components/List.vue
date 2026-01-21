<template>
  <div class="modern-virtual-scroll" ref="scrollRef" @scroll="onScroll" :style="containerStyle">
    <table class="modern-virtual-table">
      <thead class="table-header">
        <tr>
          <th 
            v-for="(header, idx) in leadingHeaders" 
            :key="`lead-h-${idx}`"
            class="header-cell leading-header"
            :style="{ height: `${headerHeight}px` }"
          >
            {{ header }}
          </th>
          <th 
            v-for="(header, idx) in headers" 
            :key="`h-${idx}`"
            class="header-cell data-header"
            :style="{ 
              minWidth: `${minWidth}px`, 
              maxWidth: `${maxWidth}px`,
              height: `${headerHeight}px`
            }"
          >
            <div class="header-content">
              <span>{{ formatHeaderWithType(header) }}</span>
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
import Icon from './Icon.vue'
import { mdiTable } from '@mdi/js'

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
  enableHighlighting: { type: Boolean, default: false },
  columnMetadata: { type: Array, default: () => [] }
})

const emit = defineEmits(['row-click', 'cell-click', 'row-double-click'])

const scrollRef = ref(null)
const startIndex = ref(0)
const highlightedCells = ref(new Set())
const containerHeight = ref(0) // 容器实际高度

const maxHeightPx = computed(() => 
  props.maxHeight != null ? `${props.maxHeight}px` : `${props.visibleCount * props.rowHeight}px`
)

// 如果外部没有提供 maxHeight，则使用 height:100% 以便填满父容器（用于大屏幕）
const containerStyle = computed(() => {
  if (props.maxHeight != null) {
    return { maxHeight: maxHeightPx.value }
  }
  // 当没有传入 maxHeight 时，优先使用 height:100%，允许外层 flex 布局决定高度
  return { height: '100%' }
})

const totalColumns = computed(() => props.headers.length + props.leadingHeaders.length)

const bufferSize = computed(() => Number.isFinite(props.buffer) ? props.buffer : 4)

// 动态计算可见行数：根据容器实际高度
const dynamicVisibleCount = computed(() => {
  if (containerHeight.value === 0) {
    return props.visibleCount // 初始值使用 prop
  }
  // 容器高度 - 表头高度，然后除以行高
  const contentHeight = containerHeight.value - headerHeight.value
  return Math.ceil(contentHeight / props.rowHeight)
})

const safeStart = computed(() => Math.max(startIndex.value - bufferSize.value, 0))

const endIndex = computed(() => 
  Math.min(startIndex.value + dynamicVisibleCount.value + bufferSize.value, props.rows.length)
)

const renderStart = computed(() => safeStart.value)

const visibleRows = computed(() => props.rows.slice(safeStart.value, endIndex.value))

const paddingTop = computed(() => safeStart.value * props.rowHeight)

const paddingBottom = computed(() => 
  Math.max(props.rows.length - endIndex.value, 0) * props.rowHeight
)

// 固定表头高度，确保中英文切换时高度一致
const headerHeight = computed(() => {
  // 固定的表头高度，不随内容变化
  return 56;
})

// 调试：监控 columnMetadata
const debugColumnMetadata = computed(() => {

  return props.columnMetadata
})

// Get column type by name from metadata
const getColumnType = (headerName) => {
  if (!Array.isArray(props.columnMetadata) || props.columnMetadata.length === 0) {
    return 'UNKNOWN'
  }
  const meta = props.columnMetadata.find(m => m.name === headerName)

  return meta?.type || 'UNKNOWN'
}

// Format header with type
const formatHeaderWithType = (header) => {
  const type = getColumnType(header)
  return `${header} (${type})`
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
  
  // 初始化容器高度
  containerHeight.value = el.clientHeight
  
  resizeObserver = new ResizeObserver(() => {
    // 更新容器高度
    containerHeight.value = el.clientHeight
    // 更新滚动位置
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
  background: white;
}

.modern-virtual-table {
  width: max-content;
  min-width: 100%;
  border-collapse: collapse;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  table-layout: fixed;
}

.table-header {
  position: sticky;
  top: 0;
  z-index: 10;
}

.header-cell {
  position: relative;
  padding: 0;
  background: #f1f5f9;
  color: #334155;
  font-weight: 600;
  text-align: center;
  border-bottom: 2px solid #cbd5e1;
  border-right: 1px solid #e2e8f0;
  font-size: 0.9rem;
  height: 56px;
  min-height: 56px;
  max-height: 56px;
  vertical-align: middle;
  box-sizing: border-box;
}

.header-cell:last-child {
  border-right: none;
}

.leading-header {
  background: #e2e8f0;
  min-width: 60px;
  width: 60px;
}

.data-header {
  background: #f1f5f9;
}

.header-content {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  width: 100%;
  padding: 0 16px;
  box-sizing: border-box;
}

.header-content span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: block;
}

.data-cell {
  padding: 0;
  border-bottom: 1px solid #e2e8f0;
  border-right: 1px solid #e2e8f0;
  vertical-align: middle;
  transition: background-color 0.2s ease;
  text-align: center;
  height: v-bind(rowHeight + 'px');
  min-height: v-bind(rowHeight + 'px');
  max-height: v-bind(rowHeight + 'px');
  box-sizing: border-box;
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
  width: 100%;
  padding: 0 16px;
  box-sizing: border-box;
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
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  width: 100%;
  padding: 0 16px;
  box-sizing: border-box;
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
  height: 200px;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  color: #94a3b8;
  height: 100%;
}

.empty-icon {
  color: #cbd5e1;
}

/* 确保所有表格单元格内容垂直水平居中 */
.modern-virtual-table td,
.modern-virtual-table th {
  display: table-cell;
  vertical-align: middle;
}

/* 修复表格布局 */
.modern-virtual-table tr {
  display: table-row;
}

/* Responsive styles */
@media (max-width: 768px) {
  .header-cell, .data-cell {
    padding: 0 8px;
    font-size: 0.8rem;
  }
  
  .header-content,
  .cell-content,
  .leading-content {
    padding: 0 8px;
  }
  
  .header-cell {
    height: 48px;
    min-height: 48px;
    max-height: 48px;
  }
}
</style>