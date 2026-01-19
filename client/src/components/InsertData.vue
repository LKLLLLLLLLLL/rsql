<!-- InsertData.vue -->
<template>
  <div class="insert-operation">
    <div class="operation-panel">
      <div class="operation-header">
        <button type="button" class="back-btn" @click="emit('back')">← 返回表格</button>
        <h4>插入数据到 <span>{{ tableName }}</span> 表</h4>
      </div>

      <div class="insert-rows-section">
        <div class="insert-rows-header">
          <h4>数据行</h4>
          <button type="button" class="add-insert-row-btn" @click="addRow">添加行</button>
        </div>
        <div class="insert-rows-list">
          <div v-for="(row, rowIndex) in rows" :key="rowIndex" class="insert-data-row">
            <div class="insert-row-header">
              <span class="row-number">行 {{ rowIndex + 1 }}</span>
              <button 
                type="button" 
                class="remove-insert-row" 
                @click="removeRow(rowIndex)"
                :disabled="rows.length <= 1"
              >
                删除行
              </button>
            </div>
            <div class="insert-fields-grid">
              <div v-for="column in columns" :key="column.name" class="insert-field">
                <label>
                  {{ column.name }}
                  <span v-if="column.primaryKey">*</span>
                  <span v-if="column.unique"> (Unique)</span>
                </label>
                <input 
                  type="text" 
                  class="insert-value" 
                  v-model="row[column.name]"
                  :placeholder="column.ableToBeNULL ? '可为空' : '必填'"
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      <button type="button" class="submit-insert-btn" @click="submitInsert">提交插入数据</button>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, computed } from 'vue'

const props = defineProps({
  tableName: { type: String, default: '' },
  columns: { type: Array, default: () => [] }
})

const emit = defineEmits(['back', 'insert'])

const rows = ref([createEmptyRow()])

function createEmptyRow() {
  const row = {}
  props.columns.forEach(col => {
    row[col.name] = ''
  })
  return row
}

function addRow() {
  rows.value.push(createEmptyRow())
}

function removeRow(index) {
  if (rows.value.length > 1) {
    rows.value.splice(index, 1)
  }
}

function validateRows() {
  const errors = []
  
  // 验证必填字段
  rows.value.forEach((row, rowIndex) => {
    const missingCols = []
    props.columns.forEach(col => {
      if (!col.ableToBeNULL && !row[col.name]?.trim()) {
        missingCols.push(col.name)
      }
    })
    if (missingCols.length > 0) {
      errors.push({ row: rowIndex + 1, columns: missingCols })
    }
  })
  
  // 验证主键
  const pkErrors = []
  rows.value.forEach((row, rowIndex) => {
    const missingPK = []
    props.columns.forEach(col => {
      if (col.primaryKey && !row[col.name]?.trim()) {
        missingPK.push(col.name)
      }
    })
    if (missingPK.length > 0) {
      pkErrors.push({ row: rowIndex + 1, columns: missingPK })
    }
  })
  
  // 验证唯一约束
  const uniqueErrors = []
  const uniqueColumns = props.columns.filter(col => col.unique)
  
  uniqueColumns.forEach(col => {
    const valuesMap = new Map()
    rows.value.forEach((row, rowIdx) => {
      const value = row[col.name]?.trim()
      if (value) {
        if (!valuesMap.has(value)) {
          valuesMap.set(value, [])
        }
        valuesMap.get(value).push(rowIdx + 1)
      }
    })
    
    const duplicates = []
    valuesMap.forEach((rowIndices, value) => {
      if (rowIndices.length > 1) {
        duplicates.push({ value, rows: rowIndices })
      }
    })
    
    if (duplicates.length > 0) {
      uniqueErrors.push({ column: col.name, duplicates })
    }
  })
  
  return { errors, pkErrors, uniqueErrors }
}

function submitInsert() {
  const { errors, pkErrors, uniqueErrors } = validateRows()
  
  if (errors.length > 0) {
    const msg = errors.map(e => `行 ${e.row} 未填写必填列：${e.columns.join(', ')}`).join('\n')
    alert(msg)
    return
  }
  
  if (pkErrors.length > 0) {
    const msg = pkErrors.map(e => `行 ${e.row} 主键未填写：${e.columns.join(', ')}`).join('\n')
    alert(msg)
    return
  }
  
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
  
  // 过滤空行
  const insertData = rows.value.filter(row => {
    return Object.values(row).some(value => value?.trim())
  })
  
  if (insertData.length === 0) {
    alert('请至少填写一行数据')
    return
  }
  
  emit('insert', insertData)
}

// 当列变化时，重置行数据
watch(() => props.columns, () => {
  rows.value = [createEmptyRow()]
}, { deep: true })
</script>

<style scoped>
.insert-operation {
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

.remove-insert-row:hover:not(:disabled) {
  background-color: #c0392b;
}

.remove-insert-row:disabled {
  opacity: 0.5;
  cursor: not-allowed;
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
</style>