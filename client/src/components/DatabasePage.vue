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
        :table-count="recordsCount"
        @insert="showInsertSection"
        @delete="showSection('delete')"
        @update="showSection('update')"
        @query="showSection('query')"
        @export="showSection('export')"
      />

      <div class="content-container">
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
          @insert="handleInsertData"
        />

        <!-- 查询功能 -->
        <div v-if="activeSection === 'query'" class="page-content">
          <div class="page-header">
            <div class="header-content">
              <h2><Icon :path="mdiMagnify" size="20" /> Query Builder</h2>
              <p class="header-subtitle">Build advanced queries with visual tools</p>
            </div>
          </div>
          <div class="content-placeholder">
            <div class="placeholder-content">
              <Icon :path="mdiChartLine" size="48" />
              <h3>Advanced Query Builder</h3>
              <p>Query builder with visual interface is under development.</p>
            </div>
          </div>
        </div>

        <!-- 导出功能 -->
        <div v-if="activeSection === 'export'" class="page-content">
          <div class="page-header">
            <div class="header-content">
              <h2><Icon :path="mdiDownload" size="20" /> Export Data</h2>
              <p class="header-subtitle">Export table data in various formats</p>
            </div>
          </div>
          <div class="content-placeholder">
            <div class="placeholder-content">
              <Icon :path="mdiDatabaseExport" size="48" />
              <h3>Export Functionality</h3>
              <p>Export to CSV, JSON, and Excel formats is under development.</p>
            </div>
          </div>
        </div>

        <!-- 重命名表 -->
        <div v-if="activeSection === 'rename'" class="page-content">
          <div class="page-header">
            <div class="header-content">
              <h2><Icon :path="mdiTableEdit" size="20" /> Rename Table</h2>
              <p class="header-subtitle">Rename existing database tables</p>
            </div>
          </div>
          <div class="content-placeholder">
            <div class="placeholder-content">
              <Icon :path="mdiRenameBox" size="48" />
              <h3>Rename Table Functionality</h3>
              <p>Table renaming feature is under development.</p>
            </div>
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
import { ref, computed, onMounted } from 'vue'
import Sidebar from './Sidebar.vue'
import Topbar from './Topbar.vue'
import DataTable from './DataTable.vue'
import CreateTable from './CreateTable.vue'
import Terminal from './Terminal.vue'
import InsertData from './InsertData.vue'
import DropTableModal from './DropTableModal.vue'
import Toast from '../components/Toast.vue'
import Icon from './Icon.vue'
import { mdiMagnify, mdiDownload, mdiTableEdit, mdiChartLine, mdiDatabaseExport, mdiRenameBox } from '@mdi/js'

// 响应式数据
const viewHeaders = ref([])
const viewRows = ref([])
const currentTableName = ref('Users')
const tables = ref(['Users', 'Products', 'Orders', 'Customers'])
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
function renderTable(headers, rows) {
  viewHeaders.value = Array.isArray(headers) ? headers.slice() : []
  viewRows.value = Array.isArray(rows) ? rows.slice() : []
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
  // 模拟数据加载
  console.log('Loading table data for:', tableName)
  viewHeaders.value = ['ID', 'Name', 'Email', 'Created At']
  viewRows.value = [
    [1, 'John Doe', 'john@example.com', '2024-01-15'],
    [2, 'Jane Smith', 'jane@example.com', '2024-01-16'],
    [3, 'Bob Johnson', 'bob@example.com', '2024-01-17']
  ]
}

async function loadTablesList() {
  console.log('Loading tables list')
  tables.value = ['Users', 'Products', 'Orders', 'Customers']
}

function handleCreateTable({ tableName, columns }) {
  console.log('Creating table:', tableName, columns)
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
/* DatabasePage.vue - 更新全局样式部分 */
/* 全局样式 */
html, body {
  margin: 0;
  padding: 0;
  height: 100%;
  background-color: #f8fafc;
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif;
  color: #1a1f36;
}

#app {
  min-height: 100vh;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
  font-family: inherit;
}

.app {
  display: flex;
  height: 100vh;
  background-color: #f8fafc;
  color: #1a1f36;
  overflow: hidden;
}

.main-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.content-container {
  flex: 1;
  padding: 24px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.page-content {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #ffffff;
  border-radius: 12px;
  border: 1px solid #e3e8ef;
}

.content-placeholder {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px;
}

.placeholder-content {
  text-align: center;
  max-width: 400px;
  color: #6b7280;
}

.placeholder-content h3 {
  font-size: 1.1rem;
  margin: 16px 0 8px;
  color: #1a1f36;
  font-weight: 600;
}

.placeholder-content p {
  font-size: 0.95rem;
  line-height: 1.5;
  margin: 0;
}

/* 统一页面标题样式 */
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

.header-subtitle {
  font-size: 0.9rem;
  color: #6b7280;
  margin: 0;
}

/* 滚动条优化 */
.content-container::-webkit-scrollbar {
  width: 6px;
}

.content-container::-webkit-scrollbar-track {
  background: #f1f5f9;
}

.content-container::-webkit-scrollbar-thumb {
  background: #d1d5db;
  border-radius: 3px;
}

.content-container::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .app {
    flex-direction: column;
  }

  .content-container {
    padding: 16px;
  }

  .page-header {
    padding: 20px;
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }

  .content-placeholder {
    padding: 32px 24px;
  }
}

/* 动画效果 */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.2s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>