<!-- CreateTable.vue -->
<template>
  <div class="create-operation">
    <div class="create-header">
      <h2><Icon :path="mdiTablePlus" size="20" /> 创建新表</h2>
      <p class="create-subtitle">定义表结构并创建新的数据库表</p>
    </div>

    <div class="create-panel">
      <div class="form-section">
        <div class="form-group">
          <label for="create-table-name" class="form-label">表名</label>
          <input 
            type="text" 
            id="create-table-name" 
            v-model="tableName"
            placeholder="例如：users" 
            class="form-input"
          />
        </div>
      </div>

      <div class="columns-section">
        <div class="section-header">
          <h3>列定义</h3>
          <button type="button" class="add-column-btn" @click="addColumn">
            <Icon :path="mdiPlus" size="16" />
            添加列
          </button>
        </div>
        
        <div class="columns-list">
          <div class="column-header">
            <span>列名</span>
            <span>数据类型</span>
            <span>允许 NULL</span>
            <span>唯一</span>
            <span>主键</span>
            <span>操作</span>
          </div>
          
          <div v-for="(column, index) in columns" :key="index" class="column-row">
            <input 
              type="text" 
              class="column-input" 
              v-model="column.name"
              placeholder="列名"
            />
            <select class="column-select" v-model="column.type">
              <option value="INTEGER">INTEGER</option>
              <option value="FLOAT">FLOAT</option>
              <option value="CHAR">CHAR</option>
              <option value="VARCHAR">VARCHAR</option>
              <option value="BOOLEAN">BOOLEAN</option>
              <option value="NULL">NULL</option>
            </select>
            <div class="column-checkbox">
              <input 
                type="checkbox" 
                v-model="column.allowNull"
                :disabled="column.primaryKey"
                class="checkbox-input"
              >
            </div>
            <div class="column-checkbox">
              <input type="checkbox" v-model="column.unique" class="checkbox-input">
            </div>
            <div class="column-checkbox">
              <input 
                type="checkbox" 
                v-model="column.primaryKey"
                @change="handlePrimaryKeyChange(index)"
                class="checkbox-input"
              >
            </div>
            <button 
              type="button" 
              class="remove-column-btn" 
              @click="removeColumn(index)" 
              :disabled="columns.length <= 1"
            >
              <Icon :path="mdiTrashCanOutline" size="16" />
            </button>
          </div>
        </div>
      </div>

      <div class="create-actions">
        <button type="button" class="submit-create-btn" @click="submitCreate">
          创建表
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import Icon from './Icon.vue'
import { mdiTablePlus, mdiPlus, mdiTrashCanOutline } from '@mdi/js'

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
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #ffffff;
  border-radius: 12px;
  border: 1px solid #e3e8ef;
}

.create-header {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  background: #f8fafc;
}

.create-header h2 {
  font-size: 1.1rem;
  color: #1a1f36;
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 0 0 8px 0;
  font-weight: 600;
}

.create-subtitle {
  font-size: 0.9rem;
  color: #6b7280;
  margin: 0;
}

.create-panel {
  flex: 1;
  padding: 24px;
  overflow-y: auto;
}

.form-section {
  margin-bottom: 32px;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 400px;
}

.form-label {
  font-weight: 500;
  color: #4b5563;
  font-size: 0.9rem;
}

.form-input {
  padding: 12px 16px;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
  font-size: 0.95rem;
  background-color: #ffffff;
  color: #1a1f36;
  transition: all 0.2s ease;
}

.form-input:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.form-input::placeholder {
  color: #9ca3af;
}

.columns-section {
  margin-bottom: 32px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.section-header h3 {
  font-size: 0.95rem;
  color: #1a1f36;
  margin: 0;
  font-weight: 600;
}

.add-column-btn {
  padding: 10px 16px;
  background: #f3f4f6;
  color: #4b5563;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.9rem;
  display: flex;
  align-items: center;
  gap: 8px;
  transition: all 0.2s ease;
}

.add-column-btn:hover {
  background: #e5e7eb;
}

.columns-list {
  background: #f9fafb;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
  overflow: hidden;
}

.column-header {
  display: grid;
  grid-template-columns: 1.5fr 1fr 0.8fr 0.8fr 0.8fr 0.5fr;
  gap: 16px;
  padding: 16px;
  background: #f3f4f6;
  border-bottom: 1px solid #e5e7eb;
  font-size: 0.85rem;
  color: #6b7280;
  font-weight: 500;
}

.column-row {
  display: grid;
  grid-template-columns: 1.5fr 1fr 0.8fr 0.8fr 0.8fr 0.5fr;
  gap: 16px;
  padding: 16px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
  align-items: center;
  transition: all 0.2s ease;
}

.column-row:last-child {
  border-bottom: none;
}

.column-row:hover {
  background: #f9fafb;
}

.column-input,
.column-select {
  padding: 10px 12px;
  border-radius: 6px;
  border: 1px solid #e5e7eb;
  font-size: 0.9rem;
  background-color: #ffffff;
  color: #1a1f36;
  transition: all 0.2s ease;
  width: 100%;
}

.column-input:focus,
.column-select:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.column-checkbox {
  display: flex;
  align-items: center;
  justify-content: center;
}

.checkbox-input {
  width: 18px;
  height: 18px;
  cursor: pointer;
  border-radius: 4px;
  border: 1px solid #d1d5db;
}

.checkbox-input:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.remove-column-btn {
  background: transparent;
  color: #9ca3af;
  border: none;
  border-radius: 6px;
  width: 32px;
  height: 32px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
  padding: 0;
}

.remove-column-btn:hover:not(:disabled) {
  background: #fee2e2;
  color: #ef4444;
}

.remove-column-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.create-actions {
  padding-top: 24px;
  border-top: 1px solid #e3e8ef;
}

.submit-create-btn {
  padding: 12px 32px;
  background: #3b82f6;
  color: white;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 600;
  font-size: 0.95rem;
  transition: all 0.2s ease;
  min-width: 120px;
}

.submit-create-btn:hover {
  background: #2563eb;
  transform: translateY(-1px);
}

.submit-create-btn:active {
  transform: translateY(0);
}

.create-panel::-webkit-scrollbar {
  width: 6px;
}

.create-panel::-webkit-scrollbar-track {
  background: #f3f4f6;
  border-radius: 3px;
}

.create-panel::-webkit-scrollbar-thumb {
  background: #d1d5db;
  border-radius: 3px;
}

.create-panel::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}
</style>