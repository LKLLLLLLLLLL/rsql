<!-- DatabasePage.vue -->
<template>
  <div class="app">
    <Sidebar
      :tables="tables"
      :current-table="currentTableName"
      :active-button="activeSidebarButton"
      @create="showSection('create')"
      @rename="toggleRenameMode"
      @drop="toggleDropMode"
      @terminal="showSection('terminal')"
      @select-table="selectTable"
      @list-toggle="handleListToggle"
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
            <p>请在左侧表格列表中选择要重命名的表</p>
            <div class="rename-table-list">
              <div 
                v-for="table in tables" 
                :key="table" 
                class="rename-table-item"
                @click="openRenameModal(table)"
              >
                <span>{{ table }}</span>
                <button class="rename-table-btn">
                  <Icon :path="mdiPencilOutline" size="16" />
                  重命名
                </button>
              </div>
            </div>
          </div>
        </div>

        <div v-if="activeSection === 'drop'" class="drop-operation">
          <div class="operation-panel">
            <h4>删除表</h4>
            <p>请选择要删除的表。此操作无法撤销,请谨慎操作。</p>
            <div class="drop-table-list">
              <div 
                v-for="table in tables" 
                :key="table" 
                class="drop-table-item"
                @click="openDropModal(table)"
              >
                <span>{{ table }}</span>
                <button class="drop-table-btn">
                  <Icon :path="mdiTrashCanOutline" size="16" />
                  删除
                </button>
              </div>
            </div>
          </div>
        </div>

        <div v-if="activeSection === 'list-view'" class="list-view-operation">
          <div class="empty-state">
            <p>请与左侧表列表中选择表</p>
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

    <RenameTableModal
      :visible="renameModalVisible"
      :old-table-name="pendingRenameTable"
      @cancel="renameModalVisible = false"
      @confirm="confirmRenameTable"
    />

    <Toast ref="toastRef" :message="toastMessage" :duration="toastDuration" />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { mdiPencilOutline, mdiTrashCanOutline } from '@mdi/js'
import Sidebar from './Sidebar.vue'
import Icon from './Icon.vue'
import Topbar from './Topbar.vue'
import DataTable from './DataTable.vue'
import CreateTable from './CreateTable.vue'
import Terminal from './Terminal.vue'
import InsertData from './InsertData.vue'
import DropTableModal from './DropTableModal.vue'
import RenameTableModal from './RenameTableModal.vue'
import Toast from '../components/Toast.vue'

// 响应式数据
const viewHeaders = ref([])
const viewRows = ref([])
const currentTableName = ref('Users')
const tables = ref([])
const activeSection = ref('terminal')
const activeSidebarButton = ref('terminal')

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
const renameModalVisible = ref(false)
const pendingRenameTable = ref('')

// Toast相关
const toastRef = ref(null)
const toastMessage = ref('')
const toastDuration = 2500

// WebSocket连接
const wsConnected = ref(false)
let wsRef = null

// 数据缓存
let currentTableHeaders = [] // 列元数据 (name, type, ableToBeNULL, primaryKey, unique)
let currentTableRows = [] // 所有数据行
let currentDisplayHeaders = [] // 显示用的列名

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
  // 如果再次点击delete或update，则返回到table视图
  if ((section === 'delete' || section === 'update') && activeSection.value === section) {
    activeSection.value = 'table'
    activeSidebarButton.value = ''
    return
  }
  
  activeSection.value = section
  activeSidebarButton.value = section
  
  if (section === 'create' || section === 'terminal') {
    loadTablesList()
  }
}

function toggleDropMode() {
  showSection('drop')
  loadTablesList()
}

function toggleRenameMode() {
  showSection('rename')
}
function openRenameModal(tableName) {
  dropModalVisible.value = false  // 关闭删除模态框
  pendingRenameTable.value = tableName
  renameModalVisible.value = true
}

function confirmRenameTable(newName) {
  const oldName = pendingRenameTable.value
  if (!oldName || !newName) {
    renameModalVisible.value = false
    return
  }
  
  const sql = `ALTER TABLE ${oldName} RENAME TO ${newName};`
  
  if (sendSqlViaWebSocket(sql)) {
    renameModalVisible.value = false
    triggerToast('重命名表语句已发送')
  }
}

function selectTable(tableName) {
  currentTableName.value = tableName
  activeSidebarButton.value = 'list'
  showSection('table')
  loadTableData(tableName)
}

function handleListToggle(isOpen) {
  // 当表列表展开时，清空旋上的操作按钮选中状态
  if (isOpen) {
    activeSidebarButton.value = ''
    activeSection.value = 'list-view'
  }
}

function openDropModal(tableName) {
  // 关闭所有其他的模态框
  renameModalVisible.value = false
  pendingDropTable.value = tableName
  dropModalVisible.value = true
}

// WebSocket操作函数
function connectWebSocket() {
  const socket = new WebSocket(wsUrl.value)
  wsRef = socket

  socket.onopen = () => {
    wsConnected.value = true
    console.log('WebSocket connected')
  }

  socket.onclose = () => {
    wsConnected.value = false
    console.log('WebSocket closed')
  }

  socket.onerror = (err) => {
    console.warn('WebSocket error', err)
    wsConnected.value = false
  }

  socket.onmessage = (ev) => {
    try {
      const data = JSON.parse(ev.data)
      console.log('WebSocket response:', data)
      handleSqlResult(data)
      
      // 如果操作成功，重新加载数据
      if (data.success) {
        // 延迟加载，给后端一点时间完成操作
        setTimeout(() => {
          loadTablesList()
          if (currentTableName.value) {
            loadTableData(currentTableName.value)
          }
        }, 100)
      }
    } catch (e) {
      console.warn('Parse WebSocket message failed', e, ev.data)
    }
  }
}

function sendSqlViaWebSocket(sql) {
  if (!wsRef || wsRef.readyState !== WebSocket.OPEN) {
    triggerToast('WebSocket未连接，请稍后重试')
    return false
  }

  const payload = {
    username: 'root',
    userid: 0,
    request_content: sql,
  }
  
  console.log('Sending SQL:', sql)
  wsRef.send(JSON.stringify(payload))
  return true
}

// 数据库操作函数
async function loadTableData(tableName) {
  // 从 public 文件夹中加载表格数据
  try {
    const response = await fetch(`/${tableName}.json`)
    if (!response.ok) {
      throw new Error(`Failed to load ${tableName}.json: ${response.statusText}`)
    }
    const data = await response.json()
    
    // 提取表格元数据
    const headers = data.headers || []
    const types = data.type || []
    const ableToBeNULL = data.ableToBeNULL || []
    const primaryKeys = data.primaryKey || []
    const uniques = data.unique || []
    
    // 构建列元数据对象数组
    currentTableHeaders = headers.map((name, index) => ({
      name,
      type: types[index] || 'VARCHAR',
      ableToBeNULL: ableToBeNULL[index] || false,
      primaryKey: primaryKeys[index] || false,
      unique: uniques[index] || false
    }))
    
    // 存储所有数据行
    currentTableRows = data.rows || []
    currentDisplayHeaders = headers.slice()
    
    // 渲染表格
    renderTable(currentDisplayHeaders, currentTableRows)
    
    console.log('Table data loaded:', {
      table: tableName,
      headers: currentTableHeaders,
      rowCount: currentTableRows.length
    })
  } catch (error) {
    console.error('Error loading table data:', error)
    currentTableHeaders = []
    currentTableRows = []
    currentDisplayHeaders = []
    renderTable([], [])
  }
}

async function loadTablesList() {
  // 从 public/TABLES.json 加载表格列表
  try {
    const response = await fetch('/TABLES.json')
    if (!response.ok) {
      throw new Error(`Failed to load TABLES.json: ${response.statusText}`)
    }
    const data = await response.json()
    tables.value = Array.isArray(data.tables) ? data.tables : []
    console.log('Tables loaded:', tables.value)
  } catch (error) {
    console.error('Error loading tables:', error)
    tables.value = []
  }
}

function handleCreateTable({ tableName, columns }) {
  // 生成CREATE TABLE SQL
  let sql = `CREATE TABLE ${tableName} (\n`
  columns.forEach((col, index) => {
    sql += `  ${col.name} ${col.type}`
    if (!col.allowNull) sql += ' NOT NULL'
    if (col.primaryKey) sql += ' PRIMARY KEY'
    if (col.unique) sql += ' UNIQUE'
    if (index < columns.length - 1) sql += ',\n'
  })
  sql += `\n);`
  
  // 通过WebSocket发送
  if (sendSqlViaWebSocket(sql)) {
    triggerToast('创建表语句已发送')
    // 返回到表格视图
    setTimeout(() => showSection('table'), 500)
  }
}

function handleInsertData(insertData) {
  // insertData是行数组: [{ colName: value, ... }]
  const rows = insertData
  const tableName = currentTableName.value
  
  if (!rows || rows.length === 0) {
    triggerToast('没有要插入的数据')
    return
  }

  // 生成INSERT SQL语句
  rows.forEach(row => {
    const columns = Object.keys(row).filter(key => row[key] !== '')
    const values = columns.map(key => {
      const value = row[key]
      // 根据类型判断是否需要加引号
      const meta = currentTableHeaders.find(h => h.name === key)
      if (meta && (meta.type === 'INT' || meta.type === 'INTEGER' || meta.type === 'FLOAT')) {
        return value
      }
      // 字符串类型加引号
      return `'${value.replace(/'/g, "''")}'`
    })
    
    const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`
    sendSqlViaWebSocket(sql)
  })
  
  triggerToast(`已发送 ${rows.length} 条插入语句`)
  // 返回到表格视图
  setTimeout(() => showSection('table'), 500)
}

function showInsertSection() {
  if (!currentTableHeaders || currentTableHeaders.length === 0) {
    alert('请先选择一个表格查看数据，然后再执行插入操作')
    return
  }
  showSection('insert')
}

function confirmDelete(idx) {
  if (!currentTableHeaders || currentTableHeaders.length === 0) {
    triggerToast('无法确定表结构')
    return
  }

  const row = currentTableRows[idx]
  if (!row) {
    triggerToast('无效的行索引')
    return
  }

  // 生成WHERE子句（使用主键或所有列）
  const whereConditions = []
  currentTableHeaders.forEach((header, colIdx) => {
    const value = row[colIdx]
    if (value !== null && value !== undefined) {
      if (header.type === 'INT' || header.type === 'INTEGER' || header.type === 'FLOAT') {
        whereConditions.push(`${header.name} = ${value}`)
      } else {
        whereConditions.push(`${header.name} = '${String(value).replace(/'/g, "''")}'`)
      }
    }
  })

  const sql = `DELETE FROM ${currentTableName.value} WHERE ${whereConditions.join(' AND ')};`
  
  if (sendSqlViaWebSocket(sql)) {
    triggerToast('删除语句已发送')
    deletePendingRow.value = null
  }
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
  if (!currentTableHeaders || currentTableHeaders.length === 0) {
    triggerToast('无法确定表结构')
    return
  }

  const oldRow = currentTableRows[idx]
  const newRow = updateDraft.value
  
  if (!oldRow || !newRow) {
    triggerToast('无效的数据')
    return
  }

  // 生成SET子句
  const setClause = []
  currentTableHeaders.forEach((header, colIdx) => {
    const newValue = newRow[colIdx]
    if (newValue !== null && newValue !== undefined && newValue !== '') {
      if (header.type === 'INT' || header.type === 'INTEGER' || header.type === 'FLOAT') {
        setClause.push(`${header.name} = ${newValue}`)
      } else {
        setClause.push(`${header.name} = '${String(newValue).replace(/'/g, "''")}'`)
      }
    }
  })

  // 生成WHERE子句（使用原始值）
  const whereConditions = []
  currentTableHeaders.forEach((header, colIdx) => {
    const value = oldRow[colIdx]
    if (value !== null && value !== undefined) {
      if (header.type === 'INT' || header.type === 'INTEGER' || header.type === 'FLOAT') {
        whereConditions.push(`${header.name} = ${value}`)
      } else {
        whereConditions.push(`${header.name} = '${String(value).replace(/'/g, "''")}'`)
      }
    }
  })

  const sql = `UPDATE ${currentTableName.value} SET ${setClause.join(', ')} WHERE ${whereConditions.join(' AND ')};`
  
  if (sendSqlViaWebSocket(sql)) {
    triggerToast('更新语句已发送')
    updateEditingRow.value = null
    updateDraft.value = []
  }
}

function confirmDropTable() {
  const name = pendingDropTable.value
  if (!name) {
    dropModalVisible.value = false
    return
  }
  
  const sql = `DROP TABLE ${name};`
  
  if (sendSqlViaWebSocket(sql)) {
    dropModalVisible.value = false
    triggerToast('删除表语句已发送')
  }
}

function handleSqlResult(data) {
  console.log('SQL result:', data)
}

// 生命周期
onMounted(() => {
  connectWebSocket()
  loadTablesList()
  loadTableData(currentTableName.value)
  showSection('terminal')
})

onBeforeUnmount(() => {
  if (wsRef) {
    wsRef.close()
    wsRef = null
  }
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

.rename-table-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 20px;
}

.rename-table-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  background: #f8f9fa;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  border: 2px solid transparent;
}

.rename-table-item:hover {
  background: #e9ecef;
  border-color: #3498db;
}

.rename-table-item span {
  font-size: 1rem;
  font-weight: 500;
  color: #2c3e50;
}

.rename-table-btn {
  background: #3498db;
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 6px 12px;
  font-size: 0.9rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  transition: background 0.2s ease;
}

.rename-table-btn:hover {
  background: #217dbb;
}

.drop-table-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 20px;
}

.drop-table-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  background: #f8f9fa;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  border: 2px solid transparent;
}

.drop-table-item:hover {
  background: #ffe5e5;
  border-color: #e74c3c;
}

.drop-table-item span {
  font-size: 1rem;
  font-weight: 500;
  color: #2c3e50;
}

.drop-table-btn {
  background: #e74c3c;
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 6px 12px;
  font-size: 0.9rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  transition: background 0.2s ease;
}

.drop-table-btn:hover {
  background: #c0392b;
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