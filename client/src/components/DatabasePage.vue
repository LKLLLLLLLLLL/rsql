<!-- DatabasePage.vue -->
<template>
  <div class="app">
    <!-- DatabasePage.vue 中的 Sidebar 组件部分 -->
    <Sidebar
      :tables="tables"
      :current-table="currentTableName"
      :active-button="activeSidebarButton"
      :is-drop-mode="dropMode"
      :ws-url="wsUrl"
      @create="showSection('create')"
      @rename="showSection('rename')"
      @drop="toggleDropMode"
      @terminal="showSection('terminal')"
      @select-table="selectTable"
      @delete-table="openDropModal"
      @clear-selection="clearTableSelection"
    />

    <div class="main-content">
      <!-- <div class="welcome-banner" v-if="username">欢迎 {{ username }}</div> -->
      <Topbar
        v-if="shouldShowTopBar"
        :current-table-name="currentTableName"
        :table-count="recordsCount"
        :current-mode="activeSection"
        @insert="showInsertSection"
        @delete="showSection('delete')"
        @update="showSection('update')"
      />

      <div class="content-container">
        <!-- 未选择表的提示 -->
        <div v-if="activeSection === 'empty'" class="page-content">
          <div class="empty-state">
            <Icon :path="mdiTable" size="64" color="#cbd5e1" />
            <h3>Please select a table from the left</h3>
            <p>Select a table to view data, insert or modify data</p>
          </div>
        </div>

        <!-- 表格视图 -->
        <div v-if="activeSection === 'table'" class="table-view-container">
          <DataTable
            :headers="viewHeaders"
            :rows="viewRows"
            :current-table-name="currentTableName"
            mode="view"
            :column-metadata="currentTableHeaders"
          />
        </div>

        <!-- 删除视图 -->
        <div v-if="activeSection === 'delete'" class="table-view-container">
          <DataTable
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
            @back="showSection('table')"
          />
        </div>

        <!-- 更新视图 -->
        <div v-if="activeSection === 'update'" class="table-view-container">
          <DataTable
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
            @back="showSection('table')"
          />
        </div>

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
          @back="showSection('table')"
          @insert="handleInsertData"
        />

        <!-- 重命名表 -->
        <div v-if="activeSection === 'rename'" class="page-content">
          <div class="table-list-content">
            <div v-if="tables.length === 0" class="empty-state">
              <Icon :path="mdiTableOff" size="48" />
              <p>No tables available</p>
            </div>
            <div v-else class="table-operation-list">
              <div
                v-for="table in tables"
                :key="`${table.tableId}`"
                class="table-operation-item"
              >
                <div class="table-info-section">
                  <Icon :path="mdiTable" size="20" />
                  <span class="table-name-text">{{ table.tableName }}</span>
                </div>
                <button class="operation-btn rename-btn" @click="openRenameModal(table.tableName)">
                  <Icon :path="mdiPencilOutline" size="16" />
                  Rename
                </button>
              </div>
            </div>
          </div>
        </div>

        <!-- 删除表 -->
        <div v-if="activeSection === 'drop'" class="page-content">
          <div class="table-list-content">
            <div v-if="tables.length === 0" class="empty-state">
              <Icon :path="mdiTableOff" size="48" />
              <p>No tables available</p>
            </div>
            <div v-else class="table-operation-list">
              <div
                v-for="table in tables"
                :key="`${table.tableId}`"
                class="table-operation-item"
              >
                <div class="table-info-section">
                  <Icon :path="mdiTable" size="20" />
                  <span class="table-name-text">{{ table.tableName }}</span>
                </div>
                <button class="operation-btn drop-btn" @click="openDropModal(table.tableName)">
                  <Icon :path="mdiTrashCanOutline" size="16" />
                  Delete
                </button>
              </div>
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
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { connect as wsConnect, send as wsSend, connected as wsConnected, addMessageListener, removeMessageListener, close as wsClose } from '../services/wsService'
import { getCredentials } from '../services/sessionService'
import Sidebar from './Sidebar.vue'
import Topbar from './Topbar.vue'
import DataTable from './DataTable.vue'
import CreateTable from './CreateTable.vue'
import Terminal from './Terminal.vue'
import InsertData from './InsertData.vue'
import DropTableModal from './DropTableModal.vue'
import RenameTableModal from './RenameTableModal.vue'
import Toast from '../components/Toast.vue'
import Icon from './Icon.vue'
import { mdiTableEdit, mdiTable, mdiTableOff, mdiPencilOutline, mdiTrashCanOutline, mdiTableRemove } from '@mdi/js'

// 数据类型映射表
const typeMapping = {
  0: 'INTEGER',
  1: 'FLOAT',
  2: 'CHAR',
  3: 'VARCHAR',
  4: 'BOOLEAN',
  5: 'DATE',
  6: 'TIMESTAMP'
}

function getTypeName(typeCode) {
  const code = typeof typeCode === 'number' ? typeCode : parseInt(typeCode)
  return typeMapping[code] || `TYPE_${code}`
}

// 响应式数据
const username = ref('')
const viewHeaders = ref([])
const viewRows = ref([])
const currentTableName = ref('')
const currentTableId = ref(null)
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

// WebSocket连接（使用单例 wsService）
// `wsConnected` 是从 wsService 导入的响应式 ref

// 数据缓存
let currentTableHeaders = [] // 列元数据 (name, type, ableToBeNULL, primaryKey, unique)
let currentTableRows = [] // 所有数据行
let currentDisplayHeaders = [] // 显示用的列名

// 表列表查询标志
let isLoadingTableList = false
let tableListTimeoutId = null
let isLoadingTableSchema = false
let isLoadingTableRows = false
let pendingTableIdForSchema = null
let pendingTableNameForRows = ''

// WebSocket URL
const wsUrl = computed(() => {
  try {
    const creds = getCredentials()
    const u = creds.username
    const p = creds.password
    if (!u || !p) return null
    if (typeof window === 'undefined') {
      return `ws://127.0.0.1:4456/ws?username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
    }
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    return `${protocol}://${window.location.host}/ws?username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
  } catch {
    return null
  }
})

// 计算属性
const recordsCount = computed(() => viewRows.value.length)
const shouldShowTopBar = computed(() =>
  ['table', 'insert', 'delete', 'update', 'create', 'rename', 'drop', 'terminal'].includes(activeSection.value)
)

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

  // Only load table list automatically when entering create view.
  // Entering the terminal should not trigger a table-list query,
  // otherwise the terminal will receive that system response immediately.
  if (section === 'create') {
    loadTablesList()
  }
}

function toggleDropMode() {
  showSection('drop')
  loadTablesList()
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
    triggerToast('Table rename statement sent')
    // 如果重命名的是当前表，更新表名
    if (currentTableName.value === oldName) {
      currentTableName.value = newName
    }
  }
}

function selectTable(tableObj) {
  // tableObj 可以是字符串（向后兼容）或对象 { tableId, tableName }
  const tableName = typeof tableObj === 'string' ? tableObj : tableObj.tableName
  const tableId = typeof tableObj === 'object' && tableObj !== null ? tableObj.tableId : null
  currentTableName.value = tableName
  currentTableId.value = tableId
  activeSidebarButton.value = 'list'
  showSection('table')
  loadTableData({ tableName, tableId })
}

function clearTableSelection() {
  currentTableName.value = ''
  activeSidebarButton.value = ''
  showSection('empty')
}

function openDropModal(tableName) {
  // 关闭所有其他的模态框
  renameModalVisible.value = false
  pendingDropTable.value = tableName
  dropModalVisible.value = true
}

// WebSocket操作函数
// WebSocket 消息处理：由单例 wsService 触发（传入已解析的 JSON 对象）
function handleWsMessage(data) {
  try {
    console.log('WebSocket response:', data, 'isLoadingTableList:', isLoadingTableList)

    // 检查是否是表列表查询的结果
    if (isLoadingTableList) {
      const rows = data?.Query?.rows || data?.rayon_response?.uniform_result?.[0]?.data?.rows || []

      if (Array.isArray(rows) && rows.length > 0) {
        console.log('Processing table list query response')
        isLoadingTableList = false

        try {
          const tableList = rows
            .map(row => {
              const tableId = typeof row[0] === 'object' ? row[0].Integer : row[0]
              const tableName = typeof row[1] === 'object' ? row[1].Chars?.value : row[1]
              return { tableId, tableName }
            })
            .filter(item => !!item.tableName)

          tables.value = tableList
          console.log('Tables loaded successfully:', tables.value)
        } catch (parseError) {
          console.error('Error parsing table list:', parseError)
          tables.value = []
        }
      } else {
        console.warn('Expected table list rows but got empty or invalid format:', rows)
        isLoadingTableList = false
      }

      if (tableListTimeoutId) {
        clearTimeout(tableListTimeoutId)
        tableListTimeoutId = null
      }
    } else if (isLoadingTableSchema) {
      const rows = data?.Query?.rows || data?.rayon_response?.uniform_result?.[0]?.data?.rows || []
      const cols = data?.Query?.cols?.[0] || data?.rayon_response?.uniform_result?.[0]?.data?.columns || []

      if (Array.isArray(rows)) {
        try {
          const findIdx = (name, fallback) => {
            const idx = cols.findIndex(c => String(c || '').toLowerCase() === name)
            return idx >= 0 ? idx : fallback
          }

            const idxTableId = findIdx('table_id', 0)
            const idxColName = findIdx('column_name', 1)
            const idxColType = findIdx('data_type', 2)
            const idxAllowNull = findIdx('is_nullable', 3)
            const idxPrimary = findIdx('is_primary', 4)
            const idxUnique = findIdx('is_unique', 5)
            const idxIsDropped = findIdx('is_dropped', 9)

          const filtered = rows.filter(row => {
            const raw = row[idxTableId]
            const tid = typeof raw === 'object' ? raw.Integer : raw

            // Filter out dropped columns
            const droppedRaw = row[idxIsDropped]
            const droppedVal = (typeof droppedRaw === 'object' && droppedRaw !== null && 'Bool' in droppedRaw) ? droppedRaw.Bool : droppedRaw
            const isDropped = droppedVal === true || droppedVal === 'true' || droppedVal === 1
            if (isDropped) return false

            return pendingTableIdForSchema === null || tid === pendingTableIdForSchema
          })

          currentTableHeaders = filtered.map(row => {
            const nameRaw = row[idxColName]
            const typeRaw = row[idxColType]
            const allowNullRaw = row[idxAllowNull]
            const primaryRaw = row[idxPrimary]
            const uniqueRaw = row[idxUnique]

              const name = typeof nameRaw === 'object' ? nameRaw.Chars?.value : nameRaw

              const type = typeof typeRaw === 'number' ? getTypeName(typeRaw) : (typeof typeRaw === 'object' ? typeRaw.Chars?.value : typeRaw)
              const allowNull = allowNullRaw === true || allowNullRaw === 'true' || allowNullRaw === 1
              const primaryKey = primaryRaw === true || primaryRaw === 'true' || primaryRaw === 1
              const unique = uniqueRaw === true || uniqueRaw === 'true' || uniqueRaw === 1
              return {
                name,
                type: type || 'VARCHAR',
                ableToBeNULL: allowNull,
                primaryKey,
                unique,
              }
            })

          currentDisplayHeaders = currentTableHeaders.map(h => h.name)
          isLoadingTableSchema = false
          isLoadingTableRows = true

          const payloadRows = {
            username: (() => {
              try { return getCredentials().username || '' } catch { return '' }
            })(),
            userid: 0,
            request_content: `select * from ${pendingTableNameForRows}`,
          }
          console.log('Sending table rows query for', pendingTableNameForRows)
          try { wsSend(JSON.stringify(payloadRows)) } catch (e) { console.warn('ws send failed', e) }
        } catch (e) {
          console.error('Error parsing column metadata:', e)
          currentTableHeaders = []
          currentDisplayHeaders = []
          isLoadingTableSchema = false
          isLoadingTableRows = false
          renderTable([], [])
        }
      } else {
        console.warn('Invalid column metadata rows:', rows)
        isLoadingTableSchema = false
        isLoadingTableRows = false
        renderTable([], [])
      }
    } else if (isLoadingTableRows) {
      const rows = data?.Query?.rows || data?.rayon_response?.uniform_result?.[0]?.data?.rows || []

      if (Array.isArray(rows)) {
        currentTableRows = rows
        renderTable(currentDisplayHeaders, currentTableRows)
        console.log('Table rows loaded:', pendingTableNameForRows, 'rows:', rows.length)
      } else {
        console.warn('Invalid table rows format:', rows)
        currentTableRows = []
        renderTable(currentDisplayHeaders, [])
      }
      isLoadingTableRows = false
    } else if (!isLoadingTableList && !isLoadingTableSchema && !isLoadingTableRows) {
      console.log('Processing SQL result')
      handleSqlResult(data)

      if (data.success) {
        setTimeout(() => {
          loadTablesList()
          if (currentTableName.value && currentTableId.value !== null) {
            loadTableData({ tableName: currentTableName.value, tableId: currentTableId.value })
          }
        }, 100)
      }
    } else {
      console.warn('Expected table list query response but got different format:', data)
      isLoadingTableList = false
    }
  } catch (e) {
    console.warn('Parse WebSocket message failed', e, data)
    isLoadingTableList = false
  }
}

function sendSqlViaWebSocket(sql) {
  if (!wsConnected || !wsConnected.value) {
    triggerToast('WebSocket not connected, please try again later')
    return false
  }

  const payload = {
            username: (() => { try { return getCredentials().username || '' } catch { return '' } })(),
    userid: 0,
    request_content: sql,
  }

  console.log('Sending SQL:', sql)
  try { wsSend(JSON.stringify(payload)) } catch (e) { console.warn('ws send failed', e) }
  return true
}

// 数据库操作函数
function loadTableData(tableInfo) {
  // 通过 WebSocket 查询 sys_column 获取列元数据，并随后查询实际表数据
  if (!wsConnected || !wsConnected.value) {
    console.warn('WebSocket not connected, cannot load table data')
    renderTable([], [])
    return
  }

  const tableName = typeof tableInfo === 'object' && tableInfo !== null ? tableInfo.tableName : String(tableInfo || '')
  const tableId = typeof tableInfo === 'object' && tableInfo !== null ? tableInfo.tableId : currentTableId.value

  if (!tableName) {
    console.warn('No table name provided')
    renderTable([], [])
    return
  }

  pendingTableIdForSchema = tableId
  pendingTableNameForRows = tableName
  isLoadingTableSchema = true
  isLoadingTableRows = false

  const payload = {
    username: (() => { try { return getCredentials().username || '' } catch { return '' } })(),
    userid: 0,
    request_content: 'select * from sys_column',
  }

  console.log('Sending column metadata query for table:', tableName, 'id:', tableId)
  try { wsSend(JSON.stringify(payload)) } catch (e) { console.warn('ws send failed', e) }
}

function loadTablesList() {
  // 通过 WebSocket 发送 SQL 查询获取系统表中的表列表
  if (!wsConnected || !wsConnected.value) {
    console.warn('WebSocket not connected, cannot load tables')
    tables.value = []
    return
  }

  // 防止重复查询
  if (isLoadingTableList) {
    console.warn('Table list query already in progress, skipping')
    return
  }

  // 设置标志位，让 onmessage 处理器知道这是一个表列表查询
  isLoadingTableList = true

  // 清理旧的超时计时器
  if (tableListTimeoutId) {
    clearTimeout(tableListTimeoutId)
    tableListTimeoutId = null
  }

  // 设置超时，防止被永久锁定
  tableListTimeoutId = setTimeout(() => {
    if (isLoadingTableList) {
      console.warn('Table list query timeout, resetting flag')
      isLoadingTableList = false
    }
    tableListTimeoutId = null
  }, 5000)

  // 发送查询命令
  const payload = {
    username: (() => { try { return getCredentials().username || '' } catch { return '' } })(),
    userid: 0,
    request_content: 'select * from sys_table',
  }

  console.log('Sending table list query')
  try { wsSend(JSON.stringify(payload)) } catch (e) { console.warn('ws send failed', e) }
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
    triggerToast('Create table statement sent')
    // 返回到表格视图
    setTimeout(() => showSection('table'), 500)
  }
}

function handleInsertData(insertData) {
  // insertData是行数组: [{ colName: value, ... }]
  const rows = insertData
  const tableName = currentTableName.value

  if (!rows || rows.length === 0) {
    triggerToast('No data to insert')
    return
  }

  // 生成INSERT SQL语句
  rows.forEach(row => {
    const columns = Object.keys(row).filter(key => row[key] !== '')
    const values = columns.map(key => {
      const value = row[key]
      const meta = currentTableHeaders.find(h => h.name === key)

      if (!meta) {
        // 如果找不到列元数据，默认当作字符串处理
        return `'${value.replace(/'/g, "''")}'`
      }

      const colType = meta.type?.toUpperCase() || 'VARCHAR'

      // 数值类型：直接使用值，不加引号
      if (colType === 'INTEGER' || colType === 'INT' || colType === 'FLOAT' || colType === 'DOUBLE') {
        // 验证是否是有效的数值
        if (colType === 'INTEGER' || colType === 'INT') {
          const num = parseInt(value, 10)
          if (isNaN(num)) {
            throw new Error(`列 "${key}" 应为整数，但收到 "${value}"`)
          }
          return String(num)
        } else {
          // FLOAT/DOUBLE
          const num = parseFloat(value)
          if (isNaN(num)) {
            throw new Error(`列 "${key}" 应为浮点数，但收到 "${value}"`)
          }
          // 如果是整数（没有小数点），转换为浮点数格式
          if (!String(value).includes('.')) {
            return num.toFixed(2)
          }
          return String(num)
        }
      }

      // 布尔类型
      if (colType === 'BOOLEAN' || colType === 'BOOL') {
        const boolVal = String(value).toLowerCase()
        if (['true', '1', 'yes', 't', 'y'].includes(boolVal)) {
          return '1'
        } else if (['false', '0', 'no', 'f', 'n'].includes(boolVal)) {
          return '0'
        } else {
          throw new Error(`列 "${key}" 应为布尔值，但收到 "${value}"`)
        }
      }

      // 日期/时间/字符串类型：加引号
      // CHAR, VARCHAR, TEXT, DATE, DATETIME, TIMESTAMP, etc.
      return `'${value.replace(/'/g, "''")}'`
    })

    const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`
    sendSqlViaWebSocket(sql)
  })

  triggerToast(`${rows.length} insert statements sent`)
  // 返回到表格视图
  setTimeout(() => showSection('table'), 500)
}

function showInsertSection() {
  if (!currentTableHeaders || currentTableHeaders.length === 0) {
    alert('Please select a table to view data before performing insert operations')
    return
  }
  showSection('insert')
}

// 转换值为 SQL 适用格式
function formatValueForSQL(value, colType) {
  const upperType = colType?.toUpperCase() || 'VARCHAR'

  // 数值类型
  if (upperType === 'INTEGER' || upperType === 'INT' || upperType === 'FLOAT' || upperType === 'DOUBLE') {
    const num = upperType === 'INTEGER' || upperType === 'INT'
      ? parseInt(value, 10)
      : parseFloat(value)

    // 浮点数且没有小数点，转换为 xx.00 格式
    if ((upperType === 'FLOAT' || upperType === 'DOUBLE') && !String(value).includes('.')) {
      return num.toFixed(2)
    }

    return String(num)
  }

  // 其他类型：加引号
  return `'${String(value).replace(/'/g, "''")}'`
}

function confirmDelete(idx) {
  if (!currentTableHeaders || currentTableHeaders.length === 0) {
    triggerToast('Cannot determine table structure')
    return
  }

  const row = currentTableRows[idx]
  if (!row) {
    triggerToast('Invalid row index')
    return
  }

  // 生成WHERE子句（使用主键或所有列）
  const whereConditions = []
  currentTableHeaders.forEach((header, colIdx) => {
    const value = row[colIdx]
    if (value !== null && value !== undefined) {
      whereConditions.push(`${header.name} = ${formatValueForSQL(value, header.type)}`)
    }
  })

  const sql = `DELETE FROM ${currentTableName.value} WHERE ${whereConditions.join(' AND ')};`

  if (sendSqlViaWebSocket(sql)) {
    triggerToast('Delete statement sent')
    deletePendingRow.value = null
    // 重新加载表格数据
    setTimeout(() => loadTableRows(currentTableName.value), 500)
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
    triggerToast('Cannot determine table structure')
    return
  }

  const oldRow = currentTableRows[idx]
  const newRow = updateDraft.value

  if (!oldRow || !newRow) {
    triggerToast('Invalid data')
    return
  }

  // 生成SET子句
  const setClause = []
  currentTableHeaders.forEach((header, colIdx) => {
    const newValue = newRow[colIdx]
    if (newValue !== null && newValue !== undefined && newValue !== '') {
      setClause.push(`${header.name} = ${formatValueForSQL(newValue, header.type)}`)
    }
  })

  // 生成WHERE子句（使用原始值）
  const whereConditions = []
  currentTableHeaders.forEach((header, colIdx) => {
    const value = oldRow[colIdx]
    if (value !== null && value !== undefined) {
      whereConditions.push(`${header.name} = ${formatValueForSQL(value, header.type)}`)
    }
  })

  const sql = `UPDATE ${currentTableName.value} SET ${setClause.join(', ')} WHERE ${whereConditions.join(' AND ')};`

  if (sendSqlViaWebSocket(sql)) {
    triggerToast('Update statement sent')
    updateEditingRow.value = null
    updateDraft.value = []
    // 重新加载表格数据
    setTimeout(() => loadTableRows(currentTableName.value), 500)
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
    triggerToast('Drop table statement sent')
    // 如果删除的是当前表，清空当前表选择
    if (currentTableName.value === name) {
      currentTableName.value = ''
      currentTableId.value = null
      showSection('empty')
    }
  }
}

function handleSqlResult(data) {
  console.log('SQL result:', data)
}

// 生命周期
onMounted(() => {
  try {
    username.value = getCredentials().username || ''
  } catch {}
  try {
    wsConnect()
    addMessageListener(handleWsMessage)
  } catch (e) {
    console.warn('Failed to start WebSocket via wsService', e)
  }
  // 默认不加载任何表格数据，让用户选择
  showSection('terminal')
})

onBeforeUnmount(() => {
  try { removeMessageListener(handleWsMessage) } catch (e) {}
})
</script>

<style>
/* DatabasePage.vue - 全局样式 */
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
  padding: 0; /* remove outer padding so inner pages control spacing */
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.page-content {
  display: flex;
  flex-direction: column;
  height: 100%;
  /* Remove outer framed container: transparent, borderless, no padding. */
  background: transparent;
  border: none;
  padding: 0;
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

/* 表格视图容器 */
.table-view-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: #ffffff;
  border: 1px solid #e3e8ef;
  overflow: hidden;
  min-height: 400px;
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

/* 空状态样式 */
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #9ca3af;
  padding: 40px;
  text-align: center;
}

.empty-state h3 {
  font-size: 1.3rem;
  color: #6b7280;
  margin: 24px 0 12px;
  font-weight: 600;
}

.empty-state p {
  font-size: 0.95rem;
  color: #9ca3af;
  max-width: 300px;
  line-height: 1.6;
  margin: 0;
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

/* 表格操作列表样式 */
.table-list-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.table-operation-list {
  flex: 1;
  padding: 24px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.table-operation-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  background: #ffffff;
  border: 1px solid #e3e8ef;
  transition: all 0.2s ease;
}

.table-operation-item:hover {
  border-color: #cbd5e1;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}

.table-info-section {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
}

.table-info-section span {
  color: #1a1f36;
  font-size: 0.95rem;
  font-weight: 500;
}

.operation-btn {
  padding: 8px 16px;
  border-radius: 6px;
  border: 1px solid;
  background: transparent;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 500;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 6px;
}

.rename-btn {
  border-color: #3b82f6;
  color: #3b82f6;
}

.rename-btn:hover {
  background: #3b82f6;
  color: #ffffff;
}

.drop-btn {
  border-color: #ef4444;
  color: #ef4444;
}

.drop-btn:hover {
  background: #ef4444;
  color: #ffffff;
}

.table-operation-list::-webkit-scrollbar {
  width: 6px;
}

.table-operation-list::-webkit-scrollbar-track {
  background: #f1f5f9;
}

.table-operation-list::-webkit-scrollbar-thumb {
  background: #d1d5db;
  border-radius: 3px;
}

.table-operation-list::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}

/* 改进布局 */
.main-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.content-container {
  flex: 1;
  min-height: 0; /* 防止内容溢出 */
}

/* 表格容器特定样式 */
.table-view-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.welcome-banner {
  margin: 16px 24px 0 24px;
  background: #ffffff;
  border: 1px solid #e3e8ef;
  border-left: 4px solid #315efb;
  color: #1a1f36;
  padding: 12px 16px;
  font-weight: 500;
}
</style>
