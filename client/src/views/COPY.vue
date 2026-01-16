<template>
    <div class="app">
        <!-- Left sidebar -->
        <div class="sidebar">
        <div class="sidebar-header">
            <h2>Database Management</h2>
        </div>
        <div class="tables-buttons">
            <div class="tables-btn create">
            <Icon :path="mdiTablePlus" size="18" />
            <span>Create New Table</span>
            </div>
            <div class="tables-btn rename">
            <Icon :path="mdiTableEdit" size="18" />
            <span>Rename Table</span>
            </div>
            <div class="tables-btn drop">
            <Icon :path="mdiTableRemove" size="18" />
            <span>Drop Table</span>
            </div>
            <div class="tables-btn terminal">
            <Icon :path="mdiConsoleLine" size="18" />
            <span>Open Terminal</span>
            </div>
        </div>

        <div class="tables-list">
            <h3> Table List</h3>
        </div>

        <div class="sidebar-footer">
            <p>Total <span id="tables-counts">8</span> tables</p>
            <p>Click table name to view data</p>
        </div>
        </div>

        <!-- Right main content area -->
        <div class="main-content">
        <!-- Top bar -->
        <div class="top-bar">
            <h1>Current Table: <span>{{ currentTableName }}</span></h1>

            <div class="action-buttons">
                <button class="action-btn insert">
                    <Icon :path="mdiPlus" size="18" />
                    <span>Insert</span>
                </button>
                <button class="action-btn delete">
                    <Icon :path="mdiDelete" size="18" />
                    <span>Delete</span>
                </button>
                <button class="action-btn update">
                    <Icon :path="mdiPencil" size="18" />
                    <span>Update</span>
                </button>
                <button class="action-btn query">
                    <Icon :path="mdiMagnify" size="18" />
                    <span>Query</span>
                </button>
                <button class="action-btn export">
                    <Icon :path="mdiDownload" size="18" />
                    <span>Export</span>
                </button>
            </div>
        </div>

        <!-- Data Display Area -->
        <div class="data-display">
            <div class="table-container">
            <div class="table-header">
                <h3>{{ currentTableName }} Table Data</h3>
                <div class="table-info">
                <span>Total {{ recordsCount }} records</span>
                <span>Updated on <span id="update-time">1970-01-01 00:00</span></span>
                </div>
            </div>
            <div class="table-scroll-wrapper">
                <VirtualList
                    :headers="viewHeaders"
                    :rows="viewRows"
                    :leading-headers="['#']"
                    :visible-count="12"
                    :row-height="48"
                >
                    <template #leading-cell="{ rowIndex }">
                        {{ rowIndex + 1 }}
                    </template>
                </VirtualList>
            </div>
            </div>

            <div class="create-operation">
                <div class="operation-panel">
                    <h4><Icon :path="mdiTablePlus" size="18" /> 创建新表</h4>

                    <div class="form-row">
                    <label for="create-table-name">表名</label>
                    <input type="text" id="create-table-name" placeholder="例如：users" aria-label="表名" />
                    </div>

                    <div class="columns-section">
                    <div class="columns-header">
                        <h4>列定义</h4>
                        <button type="button" class="add-column-btn" id="add-column-btn">
                            添加列
                        </button>
                    </div>
                    <div class="columns-list" id="columns-container"></div>
                    </div>

                    <button type="button" class="submit-create-btn" id="submit-create-btn">
                        提交创建表
                    </button>
                </div>
            </div>

            <div class="terminal-operation">
                <h1>Terminal</h1>
                <div class="terminal-panel">
                    <!-- 代码编写区 -->
                    <div class="code-area">
                        <textarea
                            class="codeArea-text"
                            v-model="codeInput"
                            :placeholder="connected ? '输入 SQL 后提交' : '正在连接到 WebSocket...'"
                            :disabled="!connected"
                        ></textarea>
                    </div>
                    <button
                        class="codeArea-submit"
                        type="button"
                        :disabled="!connected"
                        @click="submitSql"
                    >
                        {{ connected ? 'Submit' : '连接中' }}
                    </button>
                    <!-- 结果展示区 -->
                    <div class="codeArea-result">
                        <div v-if="!connected">WebSocket 未连接，请稍等或检查后端是否运行。</div>
                        <div v-else-if="codeResults.length === 0">暂无响应</div>
                        <div v-else>
                            <div v-for="(item, idx) in codeResults" :key="idx" class="codeArea-result-item">
                                <div>时间: {{ new Date(item.timestamp * 1000).toLocaleString() }} | Conn: {{ item.connection_id }}</div>
                                <div v-if="item.success">✅ {{ item.rayon_response.response_content }}</div>
                                <div v-else>❌ {{ item.rayon_response.error || '未知错误' }}</div>
                                <div>耗时: {{ item.rayon_response.execution_time }} ms</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="insert-operation">
                <div class="operation-panel">
                    <div class="operation-header">
                        <button type="button" class="back-btn" id="back-to-table-btn">← 返回表格</button>
                        <h4>插入数据到 <span id="insert-table-name"></span> 表</h4>
                    </div>

                    <div class="insert-rows-section">
                        <div class="insert-rows-header">
                            <h4>数据行</h4>
                            <button type="button" class="add-insert-row-btn" id="add-insert-row-btn">添加行</button>
                        </div>
                        <div class="insert-rows-list" id="insert-rows-container"></div>
                    </div>

                    <button type="button" class="submit-insert-btn" id="submit-insert-btn">提交插入数据</button>
                </div>
            </div>
            <div class="delete-operation">
                <div class="table-container delete-table-container">
                    <div class="table-header">
                        <h3 id="delete-table-title">{{ currentTableName }} Table Data</h3>
                        <div class="table-info">
                            <span>Total {{ recordsCount }} records</span>
                            <span>Updated on <span id="delete-update-time">1970-01-01 00:00</span></span>
                        </div>
                    </div>

                    <div class="table-scroll-wrapper">
                        <VirtualList
                            :key="deleteRenderKey"
                            :headers="viewHeaders"
                            :rows="viewRows"
                            :leading-headers="['Delete', '#']"
                            :visible-count="12"
                            :row-height="48"
                        >
                            <template #leading-cell="{ rowIndex, leadingIndex }">
                                <template v-if="leadingIndex === 0">
                                    <div class="delete-actions">
                                        <button
                                            v-if="deletePendingRow !== rowIndex"
                                            class="delete-row-btn"
                                            type="button"
                                            @click="deletePendingRow = rowIndex"
                                        >
                                            Delete
                                        </button>
                                        <template v-else>
                                            <button
                                                class="cancel-delete-btn"
                                                type="button"
                                                @click="deletePendingRow = null"
                                            >
                                                Cancel
                                            </button>
                                            <button
                                                class="confirm-delete-btn"
                                                type="button"
                                                @click="confirmDelete(rowIndex)"
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
                        </VirtualList>
                    </div>
                </div>
            </div>
            <div class="update-operation">
                <div class="table-container update-table-container">
                    <div class="table-header">
                        <h3 id="update-table-title">{{ currentTableName }} Table Data</h3>
                        <div class="table-info">
                            <span>Total {{ recordsCount }} records</span>
                            <span>Updated on <span id="update-update-time">1970-01-01 00:00</span></span>
                        </div>
                    </div>

                    <div class="table-scroll-wrapper">
                        <VirtualList
                            :key="updateRenderKey"
                            :headers="viewHeaders"
                            :rows="viewRows"
                            :leading-headers="['Update', '#']"
                            :visible-count="12"
                            :row-height="52"
                        >
                            <template #leading-cell="{ rowIndex, leadingIndex }">
                                <template v-if="leadingIndex === 0">
                                    <div class="update-actions">
                                        <button
                                            v-if="updateEditingRow !== rowIndex"
                                            class="update-row-btn"
                                            type="button"
                                            @click="startUpdate(rowIndex)"
                                        >
                                            Update
                                        </button>
                                        <template v-else>
                                            <button
                                                class="cancel-update-btn"
                                                type="button"
                                                @click="cancelUpdate"
                                            >
                                                Cancel
                                            </button>
                                            <button
                                                class="confirm-update-btn"
                                                type="button"
                                                @click="confirmUpdate(rowIndex)"
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
                            <template #cell="{ value, rowIndex, colIndex, header }">
                                <template v-if="updateEditingRow === rowIndex">
                                    <input
                                        class="update-value"
                                        type="text"
                                        v-model="updateDraft[colIndex]"
                                        :data-column="header"
                                        :placeholder="headerPlaceholder(header)"
                                    />
                                </template>
                                <template v-else>
                                    {{ value }}
                                </template>
                            </template>
                        </VirtualList>
                    </div>
                </div>
            </div>
            <div class="query-operation"></div>
            <div class="export-operation"></div>
        </div>
        </div>
    <Toast ref="toastRef" :message="toastMessage" :duration="toastDuration" />
    </div>
</template>

<script setup>
import { computed, onMounted, onBeforeUnmount, ref, defineComponent, h } from 'vue'
import VirtualList from './List.vue'
import Toast from '../components/Toast.vue'
import {
    mdiConsoleLine,
    mdiDelete,
    mdiDownload,
    mdiMagnify,
    mdiPencil,
    mdiPlus,
    mdiTableEdit,
    mdiTablePlus,
    mdiTableRemove,
    mdiTable,
    mdiTableColumnPlusAfter,
    mdiCheckCircleOutline,
} from '@mdi/js'

const Icon = defineComponent({
    name: 'Icon',
    props: {
        path: { type: String, required: true },
        size: { type: [Number, String], default: 18 },
    },
    setup(props) {
        return () =>
            h(
                'svg',
                {
                    class: 'icon',
                    width: props.size,
                    height: props.size,
                    viewBox: '0 0 24 24',
                    'aria-hidden': 'true',
                },
                [h('path', { d: props.path, fill: 'currentColor' })],
            )
    },
})

const viewHeaders = ref([])
const viewRows = ref([])
const currentTableName = ref('Users')
const recordsCount = computed(() => viewRows.value.length)
const deletePendingRow = ref(null)
const updateEditingRow = ref(null)
const updateDraft = ref([])
const deleteRenderKey = ref(0)
const updateRenderKey = ref(0)
const toastRef = ref(null)
const toastMessage = ref('')
const toastDuration = 2500
let currentTableHeaders = []
let currentTableRows = []
let currentDisplayHeaders = []

// WebSocket 相关状态
const wsUrl = 'ws://127.0.0.1:4455/ws'
const wsRef = ref(null)
const connected = ref(false)
const codeInput = ref('')
const codeResults = ref([])

function connectWebSocket() {
    const socket = new WebSocket(wsUrl)
    wsRef.value = socket

    socket.onopen = () => {
        connected.value = true
    }

    socket.onclose = () => {
        connected.value = false
    }

    socket.onerror = (err) => {
        console.warn('WebSocket error', err)
        connected.value = false
    }

    socket.onmessage = (ev) => {
        try {
            const data = JSON.parse(ev.data)
            codeResults.value.push(data)
        } catch (e) {
            console.warn('Parse WebSocket message failed', e, ev.data)
        }
    }
}

function submitSql() {
    if (!wsRef.value || wsRef.value.readyState !== WebSocket.OPEN) {
        alert('WebSocket 未连接，请稍后重试')
        return
    }

    const sql = codeInput.value.trim()
    if (!sql) {
        alert('请输入 SQL 再提交')
        return
    }

    const payload = {
        username: 'guest',
        userid: 0,
        request_content: sql,
    }
    wsRef.value.send(JSON.stringify(payload))
}

function ensureWsReady() {
    if (!wsRef.value || wsRef.value.readyState !== WebSocket.OPEN) {
        alert('WebSocket 未连接，请先启动后端或等待连接成功')
        return false
    }
    return true
}

function sendSqlStatement(sql, actionLabel = 'SQL') {
    const trimmed = (sql || '').trim()
    if (!trimmed) {
        alert(`${actionLabel} 为空，未发送`)
        return
    }
    if (!ensureWsReady()) return
    const payload = {
        username: 'guest',
        userid: 0,
        request_content: trimmed,
    }
    wsRef.value.send(JSON.stringify(payload))
    codeInput.value = trimmed
    triggerToast(`${actionLabel} 已发送`) // 轻量提示
}

// 将包含多个语句的文本拆分为多条 INSERT/SQL 逐条发送
function sendSqlBatch(sqlText, actionLabel = 'SQL 批量') {
    if (!ensureWsReady()) return
    const parts = (sqlText || '')
        .split(';')
        .map(s => s.trim())
        .filter(Boolean)
    if (parts.length === 0) {
        alert(`${actionLabel} 为空，未发送`)
        return
    }
    parts.forEach((stmt, idx) => {
        const sql = stmt.endsWith(';') ? stmt : `${stmt};`
        const payload = {
            username: 'guest',
            userid: 0,
            request_content: sql,
        }
        wsRef.value.send(JSON.stringify(payload))
        console.log(`${actionLabel} 第 ${idx + 1} 条`, sql)
    })
    codeInput.value = parts.map(s => (s.endsWith(';') ? s : `${s};`)).join('\n')
    triggerToast(`${actionLabel} 已逐条发送 (${parts.length} 条)`) // 轻量提示
}

onMounted(() => {
    connectWebSocket()
})

onBeforeUnmount(() => {
    if (wsRef.value) {
        wsRef.value.close()
    }
})

function checkTypeMatches(type, data) {
    const t = String(type || '').trim().toUpperCase()
    const makeResult = (valid, normalized = data, message = '') => ({ valid, normalized, message })

    switch (t) {
        case 'INT': {
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
            if (s === 'true' || s === '1') return makeResult(true, true)
            if (s === 'false' || s === '0') return makeResult(true, false)
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

function headerPlaceholder(headerName) {
    const meta = currentTableHeaders.find(h => h.name === headerName)
    if (!meta) return ''
    return meta.ableToBeNULL ? '可为空' : '必填'
}

function confirmDelete(idx) {
    const tableName = currentTableName.value
    if (!tableName) {
        alert('未获取到表名，无法生成删除语句')
        return
    }

    const pkHeaders = currentTableHeaders.filter(h => h.primaryKey)
    if (pkHeaders.length === 0) {
        alert('当前表未设置主键，无法生成删除语句')
        return
    }

    const rowData = Array.isArray(currentTableRows) ? currentTableRows[idx] : null
    if (!rowData) {
        alert('未找到该行数据，无法生成删除语句')
        return
    }

    const whereClauses = []
    for (const h of pkHeaders) {
        const colIndex = currentDisplayHeaders.findIndex(n => n === h.name)
        if (colIndex < 0) continue
        const rawValue = rowData[colIndex]
        if (rawValue === undefined || rawValue === null || String(rawValue).trim() === '') {
            alert(`主键列 "${h.name}" 的值为空，无法生成删除语句`)
            return
        }
        const t = String(h.type || '').trim().toUpperCase()
        let formatted
        if (t === 'INT' || t === 'INTEGER') {
            formatted = String(rawValue)
        } else if (t === 'FLOAT') {
            const s = String(rawValue)
            const n = parseFloat(s)
            formatted = s.includes('.') ? s : n.toFixed(2)
        } else {
            formatted = `'${String(rawValue).replace(/'/g, "''")}'`
        }
        whereClauses.push(`${h.name} = ${formatted}`)
    }

    if (whereClauses.length === 0) {
        alert('无法定位主键列，未生成删除语句')
        return
    }

    const sql = `DELETE FROM ${tableName} WHERE ${whereClauses.join(' AND ')};`
    console.log('Delete SQL:', sql)

    sendSqlStatement(sql, '删除语句')

    deletePendingRow.value = null
    renderTable(currentDisplayHeaders, currentTableRows)
    renderDeleteTable(currentDisplayHeaders, currentTableRows)
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
    const newRow = Array.isArray(updateDraft.value) ? updateDraft.value.slice() : []
    while (newRow.length < currentTableHeaders.length) newRow.push('')

    const missingRequired = []
    const missingPK = []
    const typeErrors = []
    const uniqueErrors = []

    currentTableHeaders.forEach((h, i) => {
        const val = newRow[i] ?? ''
        const trimmed = String(val).trim()

        if (!h.ableToBeNULL && trimmed === '') missingRequired.push(h.name)
        if (h.primaryKey && trimmed === '') missingPK.push(h.name)

        if (trimmed !== '') {
            const result = checkTypeMatches(h.type, trimmed)
            if (!result.valid) {
                typeErrors.push({ column: h.name, type: h.type, value: trimmed, message: result.message })
            }
        }
    })

    if (missingRequired.length > 0) {
        alert(`未填写必填列：${missingRequired.join(', ')}`)
        return
    }
    if (missingPK.length > 0) {
        alert(`主键未填写：${missingPK.join(', ')}`)
        return
    }
    if (typeErrors.length > 0) {
        const msg = typeErrors.map(e => `列 "${e.column}" (类型: ${e.type})：值 "${e.value}" 不匹配 - ${e.message}`).join('\n')
        alert('数据类型验证失败：\n' + msg)
        return
    }

    const uniqueCols = currentTableHeaders
        .map((h, i) => ({ h, i }))
        .filter(({ h }) => h.unique)
    uniqueCols.forEach(({ h, i }) => {
        const val = newRow[i]
        const trimmed = String(val ?? '').trim()
        if (trimmed === '') return
        const dupRowIndex = currentTableRows.findIndex((row, ridx) => ridx !== idx && Array.isArray(row) && row[i] === trimmed)
        if (dupRowIndex >= 0) {
            uniqueErrors.push({ column: h.name, value: trimmed, row: dupRowIndex + 1 })
        }
    })

    if (uniqueErrors.length > 0) {
        const msg = uniqueErrors.map(e => `列 "${e.column}" (唯一) 值 "${e.value}" 在第 ${e.row} 行重复`).join('\n')
        alert('唯一性约束验证失败：\n' + msg)
        return
    }

    const tableName = currentTableName.value
    if (!tableName) {
        alert('未获取到表名，无法生成更新语句')
        return
    }

    const formatValue = (type, value) => {
        const t = String(type || '').trim().toUpperCase()
        if (t === 'INT' || t === 'INTEGER') return String(value)
        if (t === 'FLOAT') {
            const s = String(value)
            const n = parseFloat(s)
            return s.includes('.') ? s : n.toFixed(2)
        }
        return `'${String(value).replace(/'/g, "''")}'`
    }

    const setClauses = currentTableHeaders.map((h, i) => `${h.name} = ${formatValue(h.type, newRow[i] ?? '')}`)

    const pkHeaders = currentTableHeaders.filter(h => h.primaryKey)
    if (pkHeaders.length === 0) {
        alert('当前表未设置主键，无法生成更新语句')
        return
    }

    const originalRow = Array.isArray(currentTableRows[idx]) ? currentTableRows[idx] : []
    const whereClauses = []
    for (const h of pkHeaders) {
        const colIndex = currentDisplayHeaders.findIndex(n => n === h.name)
        if (colIndex < 0) continue
        const rawValue = (originalRow && colIndex < originalRow.length) ? originalRow[colIndex] : ''
        if (rawValue === undefined || rawValue === null || String(rawValue).trim() === '') {
            alert(`主键列 "${h.name}" 的值为空，无法生成更新语句`)
            return
        }
        whereClauses.push(`${h.name} = ${formatValue(h.type, rawValue)}`)
    }

    if (whereClauses.length === 0) {
        alert('无法定位主键列，未生成更新语句')
        return
    }

    const sql = `UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ${whereClauses.join(' AND ')};`
    console.log('Update SQL:', sql)

    sendSqlStatement(sql, '更新语句')

    updateEditingRow.value = null
    updateDraft.value = []
    renderTable(currentDisplayHeaders, currentTableRows)
    renderUpdateTable(currentDisplayHeaders, currentTableRows)
}

onMounted(() => {
    const base = (import.meta.env.BASE_URL || '/').replace(/\/$/, '')
    const buildAssetUrl = (relativePath) => `${base}/${relativePath.replace(/^\//, '')}`

    // Section visibility helpers
    const tableContainer = document.querySelector('.table-container')
    const createSection = document.querySelector('.create-operation')
    const insertSection = document.querySelector('.insert-operation')
    const deleteSection = document.querySelector('.delete-operation')
    const updateSection = document.querySelector('.update-operation')
    const terminalSection = document.querySelector('.terminal-operation')
    const topBar = document.querySelector('.top-bar')
    const sections = { table: tableContainer, create: createSection, insert: insertSection, delete: deleteSection, update: updateSection, terminal: terminalSection }

    // Table elements and fallback data
    const tableElement = tableContainer ? tableContainer.querySelector('table') : null
    const tableHead = tableElement ? tableElement.querySelector('thead') : null
    const tableBody = tableElement ? tableElement.querySelector('tbody') : null
    const tablesListEl = document.querySelector('.tables-list')
    const deleteTableElement = deleteSection ? deleteSection.querySelector('table') : null
    const deleteTableHead = deleteTableElement ? deleteTableElement.querySelector('.delete-table-head') : null
    const deleteTableBody = deleteTableElement ? deleteTableElement.querySelector('.delete-table-body') : null
    const deleteRecordsCount = document.getElementById('delete-records-count')
    const deleteTableTitle = document.getElementById('delete-table-title')
    const updateTableElement = updateSection ? updateSection.querySelector('table') : null
    const updateTableHead = updateTableElement ? updateTableElement.querySelector('.update-table-head') : null
    const updateTableBody = updateTableElement ? updateTableElement.querySelector('.update-table-body') : null
    const updateRecordsCount = document.getElementById('update-records-count')
    const updateTableTitle = document.getElementById('update-table-title')

    function checkTypeMatches(type, data) {
        const t = String(type || '').trim().toUpperCase();
        const makeResult = (valid, normalized = data, message = '') => ({ valid, normalized, message });

        switch (t) {
            case 'INT': {
                const s = typeof data === 'number' ? String(data) : String(data ?? '').trim();
                if (!/^[+-]?\d+$/.test(s)) return makeResult(false, null, 'INT expects an integer without decimals');
                const n = Number(s);
                if (!Number.isInteger(n)) return makeResult(false, null, 'INT expects an integer');
                return makeResult(true, n);
            }

            case 'CHAR': {
                const s = String(data ?? '');
                if (s.length > 32) return makeResult(false, null, 'CHAR length must be <= 32');
                return makeResult(true, s);
            }

            case 'VARCHAR': {
                // Very permissive: accept anything and stringify
                return makeResult(true, String(data ?? ''));
            }

            case 'FLOAT': {
                const s = typeof data === 'number' ? String(data) : String(data ?? '').trim();
                if (!/^[+-]?\d+(\.\d+)?$/.test(s)) return makeResult(false, null, 'FLOAT expects a numeric value');
                const n = Number(s);
                if (!Number.isFinite(n)) return makeResult(false, null, 'FLOAT expects a finite number');
                // If no decimal part was provided, standardize to two decimals
                const normalized = s.includes('.') ? n : Number(n.toFixed(2));
                return makeResult(true, normalized);
            }

            case 'BOOLEAN': {
                if (data === true || data === false) return makeResult(true, data);
                const s = String(data ?? '').trim().toLowerCase();
                if (s === 'true' || s === '1' || s === 'True') return makeResult(true, true);
                if (s === 'false' || s === '0' || s === 'False') return makeResult(true, false);
                return makeResult(false, null, 'BOOLEAN expects true or false');
            }

            case 'NULL':
                return makeResult(true, null);

            default:
                return makeResult(false, null, `Unknown type: ${t}`);
        }
    }

    // Default table cache from public/DEFAULT_TABLE.json
    let defaultTableCache = null
    async function getDefaultTable() {
        if (defaultTableCache) return defaultTableCache
        try {
            const url = buildAssetUrl('DEFAULT_TABLE.json')
            const res = await fetch(url, { cache: 'no-store' })
            if (!res.ok) throw new Error('Failed to fetch DEFAULT_TABLE.json')
            const json = await res.json()
            const headers = Array.isArray(json.headers) ? json.headers : []
            const rows = Array.isArray(json.rows) ? json.rows : []
            defaultTableCache = { headers, rows }
            return defaultTableCache
        } catch (e) {
            console.warn('Load DEFAULT_TABLE.json failed:', e)
            defaultTableCache = { headers: [], rows: [] }
            return defaultTableCache
        }
    }

    // Store current table headers (normalized objects) for insert operation

    // Normalize headers into objects: { name, type, ableToBeNULL, primaryKey, unique }
    function normalizeHeaders(headers) {
        if (!Array.isArray(headers)) return []
        return headers.map((h) => {
        if (typeof h === 'string') {
            return {
            name: h,
            type: '',
            ableToBeNULL: false,
            primaryKey: false,
            unique: false
            }
        }
        const able = ('ableToBeNULL' in h) ? !!h.ableToBeNULL : (('AbleToBeNULL' in h) ? !!h.AbleToBeNULL : false)
        return {
            name: h.name || '',
            type: h.type || '',
            ableToBeNULL: able,
            primaryKey: !!h.primaryKey,
            unique: !!h.unique
        }
        })
    }

    async function loadTableData(tableName) { // 导入表数据
        if (tableName) {
            currentTableName.value = tableName
        }
        const candidates = []
        if (tableName) {
            candidates.push(buildAssetUrl(`${tableName}.json`)) // 【查表路径】
        }

        for (const url of candidates) { // 逐个尝试候选路径
            try {
                const res = await fetch(url, { cache: 'no-store' })
                if (!res.ok) throw new Error(`Fetch failed: ${url}`)
                const json = await res.json()
                const hasHeaders = Array.isArray(json.headers)
                const hasRows = Array.isArray(json.rows)
                let rawHeaders = hasHeaders ? json.headers : null
                let rows = hasRows ? json.rows : null
                if (!hasHeaders || !hasRows) {
                    const def = await getDefaultTable()
                    rawHeaders = hasHeaders ? json.headers : def.headers
                    rows = hasRows ? json.rows : def.rows
                }

                // Combine parallel header metadata arrays when present
                let normalized = []
                const types = Array.isArray(json.type) ? json.type : null
                const nullables = Array.isArray(json.ableToBeNULL) ? json.ableToBeNULL : (Array.isArray(json.AbleToBeNULL) ? json.AbleToBeNULL : null)
                const pks = Array.isArray(json.primaryKey) ? json.primaryKey : null
                const uniques = Array.isArray(json.unique) ? json.unique : null

                // Merge parallel arrays when the required ones exist and match in length; unique is optional
                if (Array.isArray(rawHeaders) && rawHeaders.length > 0 && types && nullables && pks &&
                    rawHeaders.length === types.length && rawHeaders.length === nullables.length && rawHeaders.length === pks.length) {
                    normalized = rawHeaders.map((name, i) => ({
                        name: name,
                        type: String(types[i] ?? ''),
                        ableToBeNULL: !!nullables[i],
                        primaryKey: !!pks[i],
                        unique: (Array.isArray(uniques) && uniques.length === rawHeaders.length) ? !!uniques[i] : false
                    }))
                } else {
                    normalized = normalizeHeaders(rawHeaders)
                }
                currentTableHeaders = normalized
                const displayHeaders = normalized.map(h => h.name)
                currentDisplayHeaders = displayHeaders
                currentTableRows = Array.isArray(rows) ? rows : []
                renderTable(displayHeaders, currentTableRows)
                renderDeleteTable(displayHeaders, currentTableRows)
                renderUpdateTable(displayHeaders, currentTableRows)
                return
            } catch (e) {
                // try next candidate
            }
        }

        console.warn('All JSON candidates failed, using DEFAULT_TABLE.json')
        const fallback = await getDefaultTable()
        const normalized = normalizeHeaders(fallback.headers)
        currentTableHeaders = normalized
        const displayHeaders = normalized.map(h => h.name)
        currentDisplayHeaders = displayHeaders
        currentTableRows = Array.isArray(fallback.rows) ? fallback.rows : []
        renderTable(displayHeaders, currentTableRows)
        renderDeleteTable(displayHeaders, currentTableRows)
        renderUpdateTable(displayHeaders, currentTableRows)
    }

    async function loadTablesList() { // 加载左侧导航栏的表列表
        const url = buildAssetUrl('TABLES.json')
        let names = ['Users', 'Products']
        try {
        const res = await fetch(url, { cache: 'no-store' })
        if (!res.ok) throw new Error('Failed to fetch TABLES.json')
        const json = await res.json()
        if (Array.isArray(json)) {
            names = json
        } else if (Array.isArray(json.tables)) {
            names = json.tables
        }
        } catch (e) {
        console.warn('Load TABLES.json failed, using default list', e)
        }
        // Sort by dictionary order ascending
        const sorted = Array.isArray(names)
            ? names.slice().map(n => String(n)).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
            : []
        renderTablesList(sorted)
    }

    function renderTablesList(names) { // 动态渲染左侧导航栏的表列表（按字典序升序）
        if (!tablesListEl) return
        const header = '<h3>Table List</h3>'
        const sorted = Array.isArray(names)
            ? names.slice().map(n => String(n)).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
            : []
        const items = sorted.map((n) => `<div class="table-item"><span>${n}</span></div>`).join('')
        tablesListEl.innerHTML = header + items
        const countEl = document.getElementById('tables-counts')
        if (countEl) countEl.textContent = String(sorted.length)
        attachTableItemClickHandlers()
    }

    function showSection(key) { // 切换右侧内容区显示的部分
        Object.values(sections).forEach((el) => {
            if (!el) return
            el.style.display = 'none'
        })
        if (key && sections[key]) {
            const shouldFlex = key === 'table' || key === 'delete' || key === 'update'
            sections[key].style.display = shouldFlex ? 'flex' : 'block'
        }

        if (topBar) {
            topBar.style.display = (key === 'table' || key === 'delete' || key === 'update') ? 'flex' : 'none'
        }
    }

    // load tables list into sidebar
    loadTablesList()

    // Initialize: keep right content area empty on page load
    showSection(null)

    // Create table - column row factory
    const columnsContainer = document.getElementById('columns-container')
    const addColumnBtn = document.getElementById('add-column-btn')
    const submitCreateBtn = document.getElementById('submit-create-btn')

    function createColumnRow() {
        const row = document.createElement('div')
        row.className = 'column-row'
        row.innerHTML = `
        <input type="text" class="column-name" placeholder="  列名" aria-label="列名">
        <select class="column-type" aria-label="数据类型">
            <option value="INTEGER">INTEGER</option>
            <option value="FLOAT">FLOAT</option>
            <option value="CHAR">CHAR</option>
            <option value="VARCHAR">VARCHAR</option>
            <option value="BOOLEAN">BOOLEAN</option>
            <option value="NULL">NULL</option>
        </select>
        <label class="checkbox-group">
            <input type="checkbox" class="allow-null" checked>
            <span class="checkbox-label">允许 NULL</span>
        </label>
        <label class="checkbox-group">
            <input type="checkbox" class="unique-key">
            <span class="checkbox-label">唯一</span>
        </label>
        <label class="checkbox-group">
            <input type="checkbox" class="primary-key">
            <span class="checkbox-label">主键</span>
        </label>
        <button type="button" class="remove-column">删除</button>
        `

        const allowNullCheckbox = row.querySelector('.allow-null')
        const primaryKeyCheckbox = row.querySelector('.primary-key')
        const removeBtn = row.querySelector('.remove-column')

        // Handle primary key and allow null relationship
        if (primaryKeyCheckbox && allowNullCheckbox) {
            primaryKeyCheckbox.addEventListener('change', () => {
                if (primaryKeyCheckbox.checked) {
                    // If primary key is checked, uncheck allow null and disable it
                    allowNullCheckbox.checked = false
                    allowNullCheckbox.disabled = true
                } else {
                    // If primary key is unchecked, enable allow null and check it by default
                    allowNullCheckbox.disabled = false
                    allowNullCheckbox.checked = true
                }
            })
        }

        if (removeBtn) {
            removeBtn.addEventListener('click', () => {
                if (!columnsContainer) return
                const total = columnsContainer.querySelectorAll('.column-row').length
                if (total <= 1) {
                alert('至少需要保留一列，无法删除最后一列')
                return
                }
                row.remove()
            })
        }

        return row
    }

    if (columnsContainer && addColumnBtn) {
        columnsContainer.appendChild(createColumnRow())

        addColumnBtn.addEventListener('click', () => {
        columnsContainer.appendChild(createColumnRow())
        })
    }

    if (submitCreateBtn) {
        submitCreateBtn.addEventListener('click', () => {
        const tableNameEl = document.getElementById('create-table-name')
        const tableName = tableNameEl && 'value' in tableNameEl ? tableNameEl.value.trim() : ''

        const rows = columnsContainer ? Array.from(columnsContainer.querySelectorAll('.column-row')) : []
        if (!tableName) {
            alert('表名不能为空，请填写后再提交')
            if (tableNameEl && typeof tableNameEl.focus === 'function') tableNameEl.focus()
            return
        }

        for (const row of rows) {
            const nameInput = row.querySelector('.column-name')
            const name = nameInput && 'value' in nameInput ? nameInput.value.trim() : ''
            if (!name) {
            alert('列名不能为空，请填写后再提交')
            if (nameInput && typeof nameInput.focus === 'function') nameInput.focus()
            return
            }
        }

        const columns = rows.map((row) => ({
            name: ((row.querySelector('.column-name') || {}).value || '').trim(),
            type: (row.querySelector('.column-type') || {}).value || 'TEXT',
            allowNull: !!(row.querySelector('.allow-null') || {}).checked,
            primaryKey: !!(row.querySelector('.primary-key') || {}).checked,
            unique: !!(row.querySelector('.unique-key') || {}).checked
        }))

        // Check if at least one primary key is selected
        const hasPrimaryKey = columns.some(col => col.primaryKey)
        if (!hasPrimaryKey) {
            alert('至少要选中一个主键，请选择后再提交')
            return
        }

        // Check if any primary key column allows NULL (this shouldn't happen but validate anyway)
        const primaryKeyWithNull = columns.find(col => col.primaryKey && col.allowNull)
        if (primaryKeyWithNull) {
            alert(`主键列 "${primaryKeyWithNull.name}" 不能允许 NULL，请修改后再提交`)
            return
        }

        console.log('Create table payload', { tableName, columns })

        let sql = `CREATE TABLE ${tableName} (\n`
        columns.forEach((col, index) => {
            sql += `  ${col.name} ${col.type}`
            if (!col.allowNull) sql += ' NOT NULL'
            if (col.primaryKey) sql += ' PRIMARY KEY'
            if (col.unique) sql += ' UNIQUE'
            if (index < columns.length - 1) sql += ',\n'
        })
        sql += `\n);`
        console.log('Generated SQL:\n', sql)

        sendSqlStatement(sql, '创建表语句')
        })
    }

    function attachTableItemClickHandlers() { // 绑定左侧导航栏表项点击事件
        document.querySelectorAll('.table-item').forEach((item) => {
        const newItem = item.cloneNode(true)
        if (item.parentNode) item.parentNode.replaceChild(newItem, item)
        newItem.addEventListener('click', async function () {
            document.querySelectorAll('.table-item').forEach((el) => {
            el.classList.remove('active')
            })
            newItem.classList.add('active')
            const span = newItem.querySelector('span')
            const tableName = span && span.textContent ? span.textContent.split(' ')[0] : 'Users'
            currentTableName.value = tableName
            const currentTableEl = document.getElementById('current-table')
            if (currentTableEl) currentTableEl.textContent = tableName
            const headerTitle = document.querySelector('.table-header h3')
            if (headerTitle) headerTitle.textContent = `${tableName} Table Data`
            console.log(`Switched to table: ${tableName}`)
            await loadTableData(tableName)
            showSection('table')
        })
        })
    }

    document.querySelectorAll('.action-btn').forEach((button) => {
        button.addEventListener('click', function () {
        const action = this.classList.contains('insert') ? 'Insert' : 
            this.classList.contains('delete') ? 'Delete' : 
            this.classList.contains('update') ? 'Update' : 
            this.classList.contains('query') ? 'Query' : 'Export'

        const currentTableEl = document.getElementById('current-table')
        const tableName = currentTableName.value || (currentTableEl ? currentTableEl.textContent : '')

        if (action === 'Insert') {
            if (currentTableHeaders.length === 0) {
                alert('请先选择一个表格查看数据，然后再执行插入操作')
                return
            }
            initInsertOperation(tableName)
            showSection('insert')
        } else if (action === 'Delete') {
            if (currentTableHeaders.length === 0) {
                alert('请先选择一个表格查看数据，然后再执行删除操作')
                return
            }
            if (deleteTableTitle) {
                deleteTableTitle.textContent = `${tableName} Table Data`
            }
            renderDeleteTable(currentDisplayHeaders, currentTableRows)
            showSection('delete')

        } else if (action === 'Update') {
            if (currentTableHeaders.length === 0) {
                alert('请先选择一个表格查看数据，然后再执行更新操作')
                return
            }
            if (updateTableTitle) {
                updateTableTitle.textContent = `${tableName} Table Data`
            }
            renderUpdateTable(currentDisplayHeaders, currentTableRows)
            showSection('update')
        } else if (action === 'Query') {

        } else if (action === 'Export') {

        }
        })
    })

    // Insert operation - row factory and logic
    const insertRowsContainer = document.getElementById('insert-rows-container')
    const addInsertRowBtn = document.getElementById('add-insert-row-btn')
    const submitInsertBtn = document.getElementById('submit-insert-btn')
    const backToTableBtn = document.getElementById('back-to-table-btn')

    // Back to table button handler
    if (backToTableBtn) {
        backToTableBtn.addEventListener('click', () => {
            showSection('table')
        })
    }

    function createInsertRow(headers) {
        const row = document.createElement('div')
        row.className = 'insert-data-row'
        
        let inputsHTML = headers.map(h => {
            const name = (typeof h === 'string') ? h : (h && h.name ? h.name : '')
            const isPK = (typeof h === 'object') ? !!h.primaryKey : false
            const canNull = (typeof h === 'object') ? !!h.ableToBeNULL : false
            const isUnique = (typeof h === 'object') ? !!h.unique : false

            let labelText = name
            if (isPK) labelText += '*'
            if (isUnique) labelText += ' (Unique)'
            const placeholderText = canNull ? '可为空' : '必填'

            return `
                <div class="insert-field">
                    <label>${labelText}</label>
                    <input type="text" class="insert-value" data-column="${name}" placeholder="${placeholderText}">
                </div>
            `
        }).join('')

        row.innerHTML = `
            <div class="insert-row-header">
                <span class="row-number"></span>
                <button type="button" class="remove-insert-row">删除行</button>
            </div>
            <div class="insert-fields-grid">
                ${inputsHTML}
            </div>
        `

        const removeBtn = row.querySelector('.remove-insert-row')
        if (removeBtn) {
            removeBtn.addEventListener('click', () => {
                if (!insertRowsContainer) return
                const total = insertRowsContainer.querySelectorAll('.insert-data-row').length
                if (total <= 1) {
                    alert('至少需要保留一行数据')
                    return
                }
                row.remove()
                updateInsertRowNumbers()
            })
        }
        return row
    }

    function updateInsertRowNumbers() {
        if (!insertRowsContainer) return
        const rows = insertRowsContainer.querySelectorAll('.insert-data-row')
        rows.forEach((row, index) => {
            const numberSpan = row.querySelector('.row-number')
            if (numberSpan) {
                numberSpan.textContent = `行 ${index + 1}`
            }
        })
    }

    function initInsertOperation(tableName) {
        const insertTableNameEl = document.getElementById('insert-table-name')
        if (insertTableNameEl) {
            insertTableNameEl.textContent = tableName
        }

        if (!insertRowsContainer) return
        
        // Clear existing rows
        insertRowsContainer.innerHTML = ''
        
        // Add first row
        if (currentTableHeaders.length > 0) {
            insertRowsContainer.appendChild(createInsertRow(currentTableHeaders))
            updateInsertRowNumbers()
        }
    }

    if (addInsertRowBtn) {
        addInsertRowBtn.addEventListener('click', () => {
            if (currentTableHeaders.length > 0 && insertRowsContainer) {
                insertRowsContainer.appendChild(createInsertRow(currentTableHeaders))
                updateInsertRowNumbers()
            }
        })
    }

    if (submitInsertBtn) {
        submitInsertBtn.addEventListener('click', () => {
            const currentTableEl = document.getElementById('current-table')
            const tableName = currentTableEl ? currentTableEl.textContent : ''
            
            if (!insertRowsContainer) return
            
            const dataRows = Array.from(insertRowsContainer.querySelectorAll('.insert-data-row'))
            
            if (dataRows.length === 0) {
                alert('请至少添加一行数据')
                return
            }

            // Validate required fields: ableToBeNULL === false
            const validationErrors = []
            dataRows.forEach((dataRow, idx) => {
                const missingCols = []
                currentTableHeaders.forEach(h => {
                    const name = h.name
                    const required = !h.ableToBeNULL
                    if (!required) return
                    const input = dataRow.querySelector(`.insert-value[data-column="${name}"]`)
                    const value = input ? String(input.value).trim() : ''
                    if (value === '') {
                        missingCols.push(name)
                    }
                })
                if (missingCols.length > 0) {
                    validationErrors.push({ row: idx + 1, columns: missingCols })
                }
            })

            if (validationErrors.length > 0) {
                const msg = validationErrors.map(e => `行 ${e.row} 未填写必填列：${e.columns.join(', ')}`).join('\n')
                alert(msg)
                return
            }

            // 主键不能为空：逐行检查所有 primaryKey 列
            const pkErrors = []
            dataRows.forEach((dataRow, idx) => {
                const missingPK = []
                currentTableHeaders.forEach(h => {
                    if (!h.primaryKey) return
                    const input = dataRow.querySelector(`.insert-value[data-column="${h.name}"]`)
                    const value = input ? String(input.value).trim() : ''
                    if (value === '') missingPK.push(h.name)
                })
                if (missingPK.length > 0) {
                    pkErrors.push({ row: idx + 1, columns: missingPK })
                }
            })

            if (pkErrors.length > 0) {
                const msg = pkErrors.map(e => `行 ${e.row} 主键未填写：${e.columns.join(', ')}`).join('\n')
                alert(msg)
                return
            }
            
            // Collect all data
            const insertData = []
            for (const dataRow of dataRows) {
                const inputs = dataRow.querySelectorAll('.insert-value')
                const rowData = {}
                let hasData = false
                
                inputs.forEach(input => {
                    const column = input.getAttribute('data-column')
                    const value = input.value.trim()
                    rowData[column] = value
                    if (value) hasData = true
                })
                
                if (hasData) {
                    insertData.push(rowData)
                }
            }

            if (insertData.length === 0) {
                alert('请至少填写一行数据')
                return
            }

            // Type validation: check if each value matches its column type
            const typeErrors = []
            insertData.forEach((row, rowIdx) => {
                const errors = []
                currentTableHeaders.forEach(h => {
                    const colName = h.name
                    const colType = h.type
                    const value = row[colName]
                    
                    // Skip empty values for nullable columns
                    if (!value || value === '') {
                        return
                    }
                    
                    // Perform type checking
                    const result = checkTypeMatches(colType, value)
                    if (!result.valid) {
                        errors.push({ column: colName, type: colType, value: value, message: result.message })
                    }
                })
                if (errors.length > 0) {
                    typeErrors.push({ row: rowIdx + 1, errors: errors })
                }
            })

            if (typeErrors.length > 0) {
                const msg = typeErrors.map(e => {
                    const errDetails = e.errors.map(err => 
                        `  列 "${err.column}" (类型: ${err.type})：值 "${err.value}" 不匹配 - ${err.message}`
                    ).join('\n')
                    return `行 ${e.row} 类型错误：\n${errDetails}`
                }).join('\n\n')
                alert('数据类型验证失败：\n\n' + msg)
                return
            }

            // Unique constraint validation: check for duplicate values in unique columns
            const uniqueErrors = []
            const uniqueColumns = currentTableHeaders.filter(h => h.unique)
            
            uniqueColumns.forEach(h => {
                const colName = h.name
                const valuesMap = new Map() // value -> array of row indices
                
                insertData.forEach((row, rowIdx) => {
                    const value = row[colName]
                    // Skip empty values
                    if (!value || value === '') {
                        return
                    }
                    
                    if (!valuesMap.has(value)) {
                        valuesMap.set(value, [])
                    }
                    valuesMap.get(value).push(rowIdx + 1)
                })
                
                // Find duplicate values
                const duplicates = []
                valuesMap.forEach((rowIndices, value) => {
                    if (rowIndices.length > 1) {
                        duplicates.push({ value, rows: rowIndices })
                    }
                })
                
                if (duplicates.length > 0) {
                    uniqueErrors.push({ column: colName, duplicates })
                }
            })

            if (uniqueErrors.length > 0) {
                const msg = uniqueErrors.map(e => {
                    const dupDetails = e.duplicates.map(dup => 
                        `  值 "${dup.value}" 在行 ${dup.rows.join(', ')} 中重复`
                    ).join('\n')
                    return `列 "${e.column}" (唯一约束) 存在重复值：\n${dupDetails}`
                }).join('\n\n')
                alert('唯一性约束验证失败：\n\n' + msg)
                return
            }

            // Generate JSON
            const jsonOutput = {
                table: tableName,
                operation: 'INSERT',
                data: insertData
            }

            // Generate SQL
            let sqlStatements = []
            insertData.forEach(row => {
                const columns = Object.keys(row).filter(col => row[col] !== '')
                const values = columns.map(col => {
                    const value = row[col]
                    // Find column metadata to get type
                    const colMeta = currentTableHeaders.find(h => h.name === col)
                    const colType = colMeta ? colMeta.type.toUpperCase() : ''
                    
                    // Handle INT and FLOAT without quotes
                    if (colType === 'INT' || colType === 'INTEGER') {
                        return value
                    } else if (colType === 'FLOAT') {
                        const numValue = parseFloat(value)
                        // If no decimal point in original value, format with .00
                        if (!value.includes('.')) {
                            return numValue.toFixed(2)
                        }
                        return value
                    } else {
                        // For other types, wrap in quotes and escape single quotes
                        return `'${value.replace(/'/g, "''")}'`
                    }
                })
                
                if (columns.length > 0) {
                    const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`
                    sqlStatements.push(sql)
                }
            })

            const sqlOutput = sqlStatements.join('\n')

            console.log('Insert JSON:', jsonOutput)
            console.log('Insert SQL:', sqlOutput)

            sendSqlBatch(sqlOutput, '插入语句')
        })
    }

    document.querySelectorAll('.tables-btn').forEach((button) => {
        button.addEventListener('click', function () {
        const action = this.classList.contains('create') ? 'Create' : 
            this.classList.contains('drop') ? 'Drop' : 
            this.classList.contains('rename') ? 'Rename' : 
            this.classList.contains('terminal') ? 'Terminal' : 'Unknown'
        if (action === 'Create') {
            document.querySelectorAll('.table-item').forEach((el) => {
                el.classList.remove('active')
            })
            showSection('create')
        } else if (action === 'Drop') {

        } else if (action === 'Rename') {
            
        } else if (action === 'Terminal') {
            document.querySelectorAll('.table-item').forEach((el) => {
                el.classList.remove('active')
            })
            showSection('terminal')
        }
        })
    })

})
</script>

<style>
html,
body {
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

.sidebar {
    width: 20%;
    background-color: #2c3e50;
    color: white;
    display: flex;
    flex-direction: column;
    box-shadow: 3px 0 15px rgba(0, 0, 0, 0.1);
    z-index: 10;
}

.sidebar-header {
    padding: 25px 20px;
    background-color: #1a252f;
    border-bottom: 1px solid #34495e;
}

.sidebar-header h2 {
    font-size: 1.5rem;
    display: flex;
    align-items: center;
    gap: 10px;
}

.tables-buttons {
    border-bottom: 1px solid #34495e;
    user-select: none;
}

.tables-btn {
    padding: 15px 20px;
    cursor: pointer;
    border-bottom: 1px solid #34495e;
    font-weight: 600;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 10px;
}

.tables-btn:hover {
    border-left: 4px solid #2c3e50;
}

.tables-btn.create,
.tables-btn.drop,
.tables-btn.rename {
    background-color: #3c8dc3;
    color: white;
}

.tables-btn.terminal {
    border-bottom: 1px solid #34495e;
    user-select: none;
    padding: 15px 20px;
    cursor: pointer;
    border-bottom: 1px solid #34495e;
    font-weight: 600;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 10px;
    background-color: #f08080
}
.tables-btn.terminal:hover {
    border-left: 4px solid #2c3e50;
}

.terminal-panel {
    display: flex; 
    flex-direction: column; 
    height: 800px; 
    min-height: 320px; 
    background: #f8f8f8; 
    border-radius: 8px; 
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}

.code-area {
    flex: 0 0 30%; 
    position: relative; 
    padding: 16px; 
    background: #f5f5f5; 
    border-bottom: 1px solid #eee;
}

.codeArea-text {
    width: 100%; 
    height: 100%; 
    resize: none; 
    font-family: monospace; 
    font-size: 24px; 
    border-radius: 4px; 
    border: 1px solid #ccc; 
    padding: 8px;
}

.codeArea-submit {
    padding: 10px 24px; 
    background: #4caf50; 
    color: #fff;
    border: none; 
    border-radius: 4px; 
    cursor: pointer; 
    font-weight: 600; 
    font-size: 15px;
    flex: 0 0 auto;
    align-self: center;
    margin: 12px 0;
    margin-left: auto
}

.codeArea-result {
    flex: 1 1 60%; 
    padding: 16px; 
    background: #fff; 
    min-height: 120px;
}

.tables-list {
    padding: 20px 0;
    overflow-y: auto;
    flex-grow: 1;
}

.tables-list h3 {
    padding: 0 20px 15px;
    font-size: 1rem;
    font-weight: 500;
    color: #bdc3c7;
    border-bottom: 1px solid #34495e;
    margin-bottom: 15px;
}

.table-item {
    padding: 15px 20px;
    cursor: pointer;
    transition: all 0.2s ease;
    border-left: 4px solid transparent;
    display: flex;
    align-items: center;
    gap: 12px;
    user-select: none;
}

.table-item:hover {
    background-color: #34495e;
    border-left: 4px solid #3498db;
}

.table-item.active {
    background-color: #34495e;
    border-left: 4px solid #3498db;
    color: #3498db;
}

.table-item i {
    font-size: 1.1rem;
}

.sidebar-footer {
    padding: 20px;
    border-top: 1px solid #34495e;
    font-size: 0.8rem;
    color: #7f8c8d;
}

.sidebar-footer p + p {
    margin-top: 5px;
}

.main-content {
    width: 80%;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.top-bar {
    background-color: white;
    padding: 20px 30px;
    box-shadow: 0 3px 10px rgba(0, 0, 0, 0.08);
    display: flex;
    justify-content: space-between;
    align-items: center;
    z-index: 5;
}

.top-bar h1 {
    font-size: 1.8rem;
    color: #2c3e50;
}

.action-buttons {
    display: flex;
    gap: 12px;
    user-select: none;
}

.action-btn {
    padding: 10px 20px;
    border: none;
    border-radius: 6px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.3s ease;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.95rem;
}

.action-btn.insert {
    background-color: #2ecc71;
    color: white;
}

.action-btn.delete {
    background-color: #e74c3c;
    color: white;
}

.action-btn.update {
    background-color: #3498db;
    color: white;
}

.action-btn.query {
    background-color: #9b59b6;
    color: white;
}

.action-btn.export {
    background-color: #f39c12;
    color: white;
}

.action-btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
}

.action-btn:active {
    transform: translateY(0);
}

.data-display {
    flex-grow: 1;
    padding: 30px;
    overflow-y: auto;
    background-color: #f9fafc;
}

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

.create-operation,
.terminal-operation,
.insert-operation,
.delete-operation,
.update-operation,
.query-operation,
.export-operation {
    display: none;
}

.delete-operation {
    width: 100%;
    flex: 1;
    height: 100%;
    display: flex;
    flex-direction: column;
}

.update-operation {
    width: 100%;
    flex: 1;
    height: 100%;
    display: flex;
    flex-direction: column;
}

.delete-row-btn {
    padding: 6px 12px;
    background-color: #e74c3c;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
    font-size: 0.85rem;
    width: 80px;
    text-align: center;
}

.delete-row-btn:hover {
    background-color: #c0392b;
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

.delete-actions {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    width: 100%;
}

.update-actions {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    width: 100%;
}

.cancel-delete-btn {
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

.cancel-delete-btn:hover {
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

.update-row-btn {
    padding: 6px 12px;
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
    font-size: 0.85rem;
    width: 80px;
    text-align: center;
}

.update-row-btn:hover {
    background-color: #217dbb;
}

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

.cancel-update-btn:hover {
    background-color: #7f8c8d;
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

table {
    min-width: 100%;
    width: max-content;
    border-collapse: collapse;
    flex-grow: 0;
}

thead {
    background-color: #f1f5f9;
}

th {
    padding: 18px 15px;
    text-align: left;
    font-weight: 600;
    color: #2c3e50;
    border-bottom: 2px solid #e1e8f0;
    font-size: 0.95rem;
    vertical-align: top;
    white-space: nowrap;
}

td {
    padding: 16px 15px;
    border-bottom: 1px solid #eef2f7;
    color: #4a5568;
    vertical-align: top;
    white-space: nowrap;
}

tbody tr:hover {
    background-color: #f8fafc;
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

@media (max-width: 1200px) {
    .action-btn span {
        display: none;
    }

    .action-btn {
        padding: 12px 15px;
    }

    .action-btn i {
        font-size: 1.2rem;
        margin-right: 0;
    }
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

    .top-bar {
        flex-direction: column;
        gap: 20px;
    }

    .action-buttons {
        flex-wrap: wrap;
        justify-content: center;
    }
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

.form-row {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
}

.form-row label {
    font-weight: 600;
    color: #2c3e50;
    min-width: 70px;
}

.form-row input,
.form-row select {
    padding: 10px 12px;
    border-radius: 6px;
    border: 1px solid #dfe4ea;
    font-size: 0.95rem;
    min-width: 160px;
}

.columns-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
}

.columns-header h4 {
    margin-bottom: 10px;
}

.default-value,
.column-type,
.column-name {
    height: 30px;
    font-size: 16px;
}

.columns-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
}

.column-row {
    display: grid;
    /* Adjusted to prevent the primary key column from stretching */
    grid-template-columns: 1.5fr 1fr auto auto auto auto;
    gap: 10px;
    align-items: center;
    background-color: #f8f9fa;
    padding: 12px;
    border-radius: 8px;
}

.column-row input,
.column-row select {
    width: 100%;
}

.column-row .checkbox-group {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.9rem;
    color: #2c3e50;
    white-space: nowrap;
}

.column-row .checkbox-label {
    font-size: 16px;
}

.column-row .remove-column {
    padding: 8px 12px;
    background-color: #e74c3c;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
}

.column-row .remove-column:hover {
    background-color: #c0392b;
}

.add-column-btn {
    padding: 10px 14px;
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
}

.add-column-btn:hover {
    background-color: #217dbb;
}

.submit-create-btn {
    align-self: flex-start;
    padding: 12px 20px;
    background-color: #2ecc71;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 700;
    font-size: 1rem;
}

.submit-create-btn:hover {
    background-color: #27ae60;
}

/* Insert operation styles */
.operation-header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 8px;
}

.operation-header h4 {
    margin: 0;
    flex-grow: 1;
}

.back-btn {
    padding: 10px 16px;
    background-color: #95a5a6;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
    font-size: 0.95rem;
    transition: background-color 0.2s ease;
    display: flex;
    align-items: center;
    gap: 6px;
}

.back-btn:hover {
    background-color: #7f8c8d;
}

.insert-rows-section {
    display: flex;
    flex-direction: column;
    gap: 18px;
}

.insert-rows-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
}

.insert-rows-header h4 {
    margin: 0;
}

.add-insert-row-btn {
    padding: 10px 14px;
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
    transition: background-color 0.2s ease;
}

.add-insert-row-btn:hover {
    background-color: #217dbb;
}

.insert-rows-list {
    display: flex;
    flex-direction: column;
    gap: 16px;
}

.insert-data-row {
    background-color: #f8f9fa;
    padding: 16px;
    border-radius: 8px;
    border: 1px solid #e1e8f0;
}

.insert-row-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid #dee2e6;
}

.insert-row-header .row-number {
    font-weight: 600;
    color: #2c3e50;
    font-size: 0.95rem;
}

.remove-insert-row {
    padding: 6px 12px;
    background-color: #e74c3c;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 600;
    font-size: 0.85rem;
    transition: background-color 0.2s ease;
}

.remove-insert-row:hover {
    background-color: #c0392b;
}

.insert-fields-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
}

.insert-field {
    display: flex;
    flex-direction: column;
    gap: 6px;
}

.insert-field label {
    font-weight: 600;
    color: #2c3e50;
    font-size: 0.9rem;
}

.insert-field .insert-value {
    padding: 10px 12px;
    border: 1px solid #dfe4ea;
    border-radius: 6px;
    font-size: 0.95rem;
    transition: border-color 0.2s ease;
}

.insert-field .insert-value:focus {
    outline: none;
    border-color: #3498db;
    box-shadow: 0 0 0 3px rgba(52, 152, 219, 0.1);
}

.submit-insert-btn {
    align-self: flex-start;
    padding: 12px 20px;
    background-color: #2ecc71;
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: 700;
    font-size: 1rem;
    transition: background-color 0.2s ease;
}

.submit-insert-btn:hover {
    background-color: #27ae60;
}

.status {
    font-weight: 600;
}

.status.active {
    color: #2ecc71;
}

.status.disabled {
    color: #e74c3c;
}

.status.pending {
    color: #f39c12;
}
</style>
