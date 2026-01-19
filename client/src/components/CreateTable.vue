<!-- CreateTable.vue -->
<template>
  <div class="create-operation">
    <div class="operation-panel">
      <h4><Icon :path="mdiTablePlus" size="18" /> 创建新表</h4>

      <div class="form-row">
        <label for="create-table-name">表名</label>
        <input 
          type="text" 
          id="create-table-name" 
          v-model="tableName"
          placeholder="例如：users" 
          aria-label="表名" 
        />
      </div>

      <div class="columns-section">
        <div class="columns-header">
          <h4>列定义</h4>
          <button type="button" class="add-column-btn" @click="addColumn">
            添加列
          </button>
        </div>
        <div class="columns-list">
          <div v-for="(column, index) in columns" :key="index" class="column-row">
            <input 
              type="text" 
              class="column-name" 
              v-model="column.name"
              placeholder="列名" 
              aria-label="列名"
            />
            <select class="column-type" v-model="column.type" aria-label="数据类型">
              <option value="INTEGER">INTEGER</option>
              <option value="FLOAT">FLOAT</option>
              <option value="CHAR">CHAR</option>
              <option value="VARCHAR">VARCHAR</option>
              <option value="BOOLEAN">BOOLEAN</option>
              <option value="NULL">NULL</option>
            </select>
            <label class="checkbox-group">
              <input 
                type="checkbox" 
                class="allow-null" 
                v-model="column.allowNull"
                :disabled="column.primaryKey"
              >
              <span class="checkbox-label">允许 NULL</span>
            </label>
            <label class="checkbox-group">
              <input type="checkbox" class="unique-key" v-model="column.unique">
              <span class="checkbox-label">唯一</span>
            </label>
            <label class="checkbox-group">
              <input 
                type="checkbox" 
                class="primary-key" 
                v-model="column.primaryKey"
                @change="handlePrimaryKeyChange(index)"
              >
              <span class="checkbox-label">主键</span>
            </label>
            <button type="button" class="remove-column" @click="removeColumn(index)" :disabled="columns.length <= 1">
              删除
            </button>
          </div>
        </div>
      </div>

      <button type="button" class="submit-create-btn" @click="submitCreate">
        提交创建表
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import Icon from './Icon.vue'
import { mdiTablePlus } from '@mdi/js'

const emit = defineEmits(['create-table'])

const tableName = ref('')
const columns = ref([
  {
    name: '',
    type: 'INTEGER',
    allowNull: true,
    primaryKey: false,
    unique: false
  }
])

function addColumn() {
  columns.value.push({
    name: '',
    type: 'INTEGER',
    allowNull: true,
    primaryKey: false,
    unique: false
  })
}

function removeColumn(index) {
  if (columns.value.length > 1) {
    columns.value.splice(index, 1)
  }
}

function handlePrimaryKeyChange(index) {
  if (columns.value[index].primaryKey) {
    columns.value[index].allowNull = false
  }
}

function submitCreate() {
  if (!tableName.value.trim()) {
    alert('表名不能为空，请填写后再提交')
    return
  }

  for (const column of columns.value) {
    if (!column.name.trim()) {
      alert('列名不能为空，请填写后再提交')
      return
    }
  }

  const hasPrimaryKey = columns.value.some(col => col.primaryKey)
  if (!hasPrimaryKey) {
    alert('至少要选中一个主键，请选择后再提交')
    return
  }

  const primaryKeyWithNull = columns.value.find(col => col.primaryKey && col.allowNull)
  if (primaryKeyWithNull) {
    alert(`主键列 "${primaryKeyWithNull.name}" 不能允许 NULL，请修改后再提交`)
    return
  }

  emit('create-table', {
    tableName: tableName.value,
    columns: columns.value
  })
}
</script>

<style scoped>
.create-operation {
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

.column-name,
.column-type {
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

.column-row .remove-column:hover:not(:disabled) {
  background-color: #c0392b;
}

.column-row .remove-column:disabled {
  opacity: 0.5;
  cursor: not-allowed;
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
</style>