<!-- DatabasePage.vue -->
<template>
  <div class="app">
    <Sidebar
      :tables="tables"
      :current-table="currentTableName"
      :active-button="activeSidebarButton"
      :is-drop-mode="dropMode"
      @create="showSection('create')"
      @rename="showSection('rename')"
      @drop="toggleDropMode"
      @terminal="showSection('terminal')"
      @select-table="selectTable"
      @delete-table="openDropModal"
    />

    <div class="main-content">
      <Topbar
        v-if="shouldShowTopBar"
        :current-table-name="currentTableName"
        @insert="showInsertSection"
        @delete="showSection('delete')"
        @update="showSection('update')"
        @query="showSection('query')"
        @export="showSection('export')"
      />

      <div class="data-display">
        <!-- 表格视图 -->
        <DataTable
          v-if="activeSection === 'table'"
          :headers="viewHeaders"
          :rows="viewRows"
          :current-table-name="currentTableName"
          mode="view"
          :column-metadata="currentTableHeaders"
        />

        <!-- 删除视图 -->
        <DataTable
          v-if="activeSection === 'delete'"
          :headers="viewHeaders"
          :rows="viewRows"
          :current-table-name="currentTableName"
          mode="delete"
          :pending-row="deletePendingRow"
          :render-key="deleteRenderKey"
          :column-metadata="currentTableHeaders"
          @pending-delete="deletePendingRow = $event"
          @cancel-delete="deletePendingRow = null"
          @confirm-delete="confirmDelete"
        />

        <!-- 更新视图 -->
        <DataTable
          v-if="activeSection === 'update'"
          :headers="viewHeaders"
          :rows="viewRows"
          :current-table-name="currentTableName"
          mode="update"
          :editing-row="updateEditingRow"
          :draft-values="updateDraft"
          :render-key="updateRenderKey"
          :column-metadata="currentTableHeaders"
          @start-update="startUpdate"
          @cancel-update="cancelUpdate"
          @confirm-update="confirmUpdate"
          @update-draft="updateDraft[$event.colIndex] = $event.value"
        />

        <!-- 创建表 -->
        <CreateTable
          v-if="activeSection === 'create'"
          @create-table="handleCreateTable"
          @back="showSection('table')"
        />

        <!-- 终端 -->
        <Terminal
          v-if="activeSection === 'terminal'"
          :ws-url="wsUrl"
          @sql-executed="handleSqlResult"
        />

        <!-- 插入数据 -->
        <InsertData
          v-if="activeSection === 'insert'"
          :table-name="currentTableName"
          :columns="currentTableHeaders"
          @back="showSection('table')"
          @insert="handleInsertData"
        />

        <!-- 其他占位区域 -->
        <div v-if="activeSection === 'query'" class="query-operation">
          <div class="operation-panel">
            <h4>查询功能</h4>
            <p>查询功能开发中...</p>
          </div>
        </div>

        <div v-if="activeSection === 'export'" class="export-operation">
          <div class="operation-panel">
            <h4>导出功能</h4>
            <p>导出功能开发中...</p>
          </div>
        </div>

        <div v-if="activeSection === 'rename'" class="rename-operation">
          <div class="operation-panel">
            <h4>重命名表</h4>
            <p>重命名功能开发中...</p>
          </div>
        </div>
      </div>
    </div>

    <DropTableModal
      :visible="dropModalVisible"
      :table-name="pendingDropTable"
      @cancel="dropModalVisible = false"
      @confirm="confirmDropTable"
    />

    <Toast ref="toastRef" :message="toastMessage" :duration="toastDuration" />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick } from 'vue'
import Sidebar from './Sidebar.vue'
import Topbar from './Topbar.vue'
import DataTable from './DataTable.vue'
import CreateTable from './CreateTable.vue'
import Terminal from './Terminal.vue'
import InsertData from './InsertData.vue'
import DropTableModal from './DropTableModal.vue'
import Toast from '../components/Toast.vue'

// 响应式数据
const viewHeaders = ref([])
const viewRows = ref([])
const currentTableName = ref('Users')
const tables = ref(['Users', 'Products'])
const activeSection = ref(null)
const activeSidebarButton = ref('')
const dropMode = ref(false)

// 删除相关
const deletePendingRow = ref(null)
const deleteRenderKey = ref(0)

// 更新相关
const updateEditingRow = ref(null)
const updateDraft = ref([])
const updateRenderKey = ref(0)

// 弹窗相关
const dropModalVisible = ref(false)
const pendingDropTable = ref('')

// Toast相关
const toastRef = ref(null)
const toastMessage = ref('')
const toastDuration = 2500

// 数据缓存
let currentTableHeaders = []
let currentTableRows = []
let currentDisplayHeaders = []

// WebSocket URL
const wsUrl = computed(() => {
  const username = 'root'
  const password = 'password'
  if (typeof window === 'undefined') {
    return `ws://127.0.0.1:4456/ws?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
  }
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${protocol}://${window.location.host}/ws?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
})

// 计算属性
const recordsCount = computed(() => viewRows.value.length)
const shouldShowTopBar = computed(() => ['table', 'delete', 'update'].includes(activeSection.value))

// 辅助函数
function checkTypeMatches(type, data) {
  const t = String(type || '').trim().toUpperCase()
  const makeResult = (valid, normalized = data, message = '') => ({ valid, normalized, message })

  switch (t) {
    case 'INT':
    case 'INTEGER': {
      const s = typeof data === 'number' ? String(data) : String(data ?? '').trim()
      if (!/^[+-]?\d+$/.test(s)) return makeResult(false, null, 'INT expects an integer without decimals')
      const n = Number(s)
      if (!Number.isInteger(n)) return makeResult(false, null, 'INT expects an integer')
      return makeResult(true, n)
    }

    case 'CHAR': {
      const s = String(data ?? '')
      if (s.length > 32) return makeResult(false, null, 'CHAR length must be <= 32')
      return makeResult(true, s)
    }

    case 'VARCHAR': {
      return makeResult(true, String(data ?? ''))
    }

    case 'FLOAT': {
      const s = typeof data === 'number' ? String(data) : String(data ?? '').trim()
      if (!/^[+-]?\d+(\.\d+)?$/.test(s)) return makeResult(false, null, 'FLOAT expects a numeric value')
      const n = Number(s)
      if (!Number.isFinite(n)) return makeResult(false, null, 'FLOAT expects a finite number')
      const normalized = s.includes('.') ? n : Number(n.toFixed(2))
      return makeResult(true, normalized)
    }

    case 'BOOLEAN': {
      if (data === true || data === false) return makeResult(true, data)
      const s = String(data ?? '').trim().toLowerCase()
      if (s === 'true' || s === '1' || s === 'True') return makeResult(true, true)
      if (s === 'false' || s === '0' || s === 'False') return makeResult(true, false)
      return makeResult(false, null, 'BOOLEAN expects true or false')
    }

    case 'NULL':
      return makeResult(true, null)

    default:
      return makeResult(false, null, `Unknown type: ${t}`)
  }
}

function renderTable(headers, rows) {
  viewHeaders.value = Array.isArray(headers) ? headers.slice() : []
  viewRows.value = Array.isArray(rows) ? rows.slice() : []
}

function renderDeleteTable(headers, rows) {
  renderTable(headers, rows)
  deletePendingRow.value = null
  deleteRenderKey.value += 1
}

function renderUpdateTable(headers, rows) {
  renderTable(headers, rows)
  updateEditingRow.value = null
  updateDraft.value = []
  updateRenderKey.value += 1
}

function triggerToast(msg) {
  toastMessage.value = msg
  if (toastRef.value && typeof toastRef.value.show === 'function') {
    toastRef.value.show()
  }
}

function showSection(section) {
  activeSection.value = section
  activeSidebarButton.value = section
  
  if (section === 'create' || section === 'terminal') {
    dropMode.value = false
    loadTablesList()
  }
}

function toggleDropMode() {
  dropMode.value = !dropMode.value
  activeSidebarButton.value = dropMode.value ? 'drop' : ''
  loadTablesList()
}

function selectTable(tableName) {
  currentTableName.value = tableName
  showSection('table')
  loadTableData(tableName)
}

function openDropModal(tableName) {
  pendingDropTable.value = tableName
  dropModalVisible.value = true
}

// 数据库操作函数
async function loadTableData(tableName) {
  // 实现数据加载逻辑
  console.log('Loading table data for:', tableName)
  // 这里应该从API加载数据
}

async function loadTablesList() {
  // 实现表格列表加载逻辑
  console.log('Loading tables list')
  // 这里应该从API加载表格列表
}

function handleCreateTable({ tableName, columns }) {
  console.log('Creating table:', tableName, columns)
  // 生成SQL并发送
  let sql = `CREATE TABLE ${tableName} (\n`
  columns.forEach((col, index) => {
    sql += `  ${col.name} ${col.type}`
    if (!col.allowNull) sql += ' NOT NULL'
    if (col.primaryKey) sql += ' PRIMARY KEY'
    if (col.unique) sql += ' UNIQUE'
    if (index < columns.length - 1) sql += ',\n'
  })
  sql += `\n);`
  
  console.log('Generated SQL:', sql)
  triggerToast('创建表语句已发送')
}

function handleInsertData(insertData) {
  console.log('Inserting data:', insertData)
  // 生成SQL并发送
  triggerToast('插入语句已发送')
}

function showInsertSection() {
  if (currentTableHeaders.length === 0) {
    alert('请先选择一个表格查看数据，然后再执行插入操作')
    return
  }
  showSection('insert')
}

function confirmDelete(idx) {
  console.log('Confirm delete row:', idx)
  // 实现删除逻辑
  triggerToast('删除语句已发送')
}

function startUpdate(idx) {
  updateEditingRow.value = idx
  updateDraft.value = Array.isArray(currentTableRows[idx]) ? [...currentTableRows[idx]] : []
}

function cancelUpdate() {
  updateEditingRow.value = null
  updateDraft.value = []
  updateRenderKey.value += 1
}

function confirmUpdate(idx) {
  console.log('Confirm update row:', idx, updateDraft.value)
  // 实现更新逻辑
  triggerToast('更新语句已发送')
}

function confirmDropTable() {
  const name = pendingDropTable.value
  if (!name) {
    dropModalVisible.value = false
    return
  }
  
  const sql = `DROP TABLE ${name};`
  console.log('Drop table SQL:', sql)
  
  dropModalVisible.value = false
  triggerToast('删除表语句已发送')
  
  // 重新加载表格列表
  setTimeout(() => {
    loadTablesList()
  }, 100)
}

function handleSqlResult(data) {
  console.log('SQL result:', data)
}

// 生命周期
onMounted(() => {
  loadTablesList()
  loadTableData(currentTableName.value)
  showSection('table')
})
</script>

<style>
/* 全局样式 */
html, body {
  margin: 0;
  padding: 0;
  height: 100%;
  background-color: #f5f7fa;
  font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
}

#app {
  min-height: 100vh;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
  font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
}

.app {
  display: flex;
  height: 100vh;
  background-color: #f5f7fa;
  color: #333;
}

.main-content {
  width: 80%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.data-display {
  flex-grow: 1;
  padding: 30px;
  overflow-y: auto;
  background-color: #f9fafc;
}

.query-operation,
.export-operation,
.rename-operation {
  display: block;
}

.operation-panel {
  margin-top: 24px;
  background-color: white;
  border-radius: 10px;
  padding: 24px;
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.05);
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.operation-panel h4 {
  font-size: 1.2rem;
  color: #2c3e50;
  display: flex;
  align-items: center;
  gap: 8px;
}

.icon {
  vertical-align: middle;
  display: inline-block;
  transform: translateY(0px);
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #95a5a6;
  padding: 40px;
  text-align: center;
}

.empty-state i {
  font-size: 4rem;
  margin-bottom: 20px;
  opacity: 0.5;
}

.empty-state h3 {
  font-size: 1.5rem;
  margin-bottom: 10px;
  color: #7f8c8d;
}

@media (max-width: 768px) {
  .app {
    flex-direction: column;
  }

  .sidebar {
    width: 100%;
    height: auto;
    max-height: 40vh;
  }

  .main-content {
    width: 100%;
  }
}
</style>