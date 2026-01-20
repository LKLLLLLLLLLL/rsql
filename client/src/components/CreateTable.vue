<!-- CreateTable.vue -->
<template>
  <div class="page-content">
    <div class="create-panel">
      <div class="form-section">
        <div class="form-group">
          <label for="create-table-name" class="form-label">Table Name</label>
          <input
            type="text"
            id="create-table-name"
            v-model="tableName"
            placeholder="e.g.: users"
            class="form-input"
          />
        </div>
      </div>

      <div class="columns-section">
        <div class="section-header">
          <h3>Column Definitions</h3>
          <button type="button" class="add-column-btn" @click="addColumn">
            <Icon :path="mdiPlus" size="16" />
            Add Column
          </button>
        </div>

        <div class="columns-list">
          <div class="column-header">
            <span>Column Name</span>
            <span>Data Type</span>
            <span>Allow NULL</span>
            <span>Unique</span>
            <span>Primary Key</span>
            <span>Action</span>
          </div>

          <!-- 添加滚动容器 -->
          <div class="column-rows-container">
            <div v-for="(column, index) in columns" :key="index" class="column-row">
              <input
                type="text"
                class="column-input"
                v-model="column.name"
                placeholder="Column name"
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
      </div>

      <div class="create-actions">
        <button type="button" class="submit-create-btn" @click="submitCreate">
          Create Table
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
    alert('Table name cannot be empty. Please fill it in before submitting.')
    return
  }

  for (const column of columns.value) {
    if (!column.name.trim()) {
      alert('Column name cannot be empty. Please fill in all column names before submitting.')
      return
    }
  }

  const hasPrimaryKey = columns.value.some(col => col.primaryKey)
  if (!hasPrimaryKey) {
    alert('At least one primary key must be selected. Please select a primary key before submitting.')
    return
  }

  const primaryKeyWithNull = columns.value.find(col => col.primaryKey && col.allowNull)
  if (primaryKeyWithNull) {
    alert(`Primary key column "${primaryKeyWithNull.name}" cannot allow NULL. Please modify before submitting.`)
    return
  }

  emit('create-table', {
    tableName: tableName.value,
    columns: columns.value
  })
}
</script>

<style scoped>
.page-content {
  display: flex;
  flex-direction: column;
  height: 100%;
  /* remove outer framed container; inner .create-panel has its own padding */
  background: transparent;
  border-radius: 0;
  border: none;
  overflow: visible;
  padding: 0;
}

.page-header {
  padding: 24px;
  border-bottom: 1px solid #e3e8ef;
  background: #f8fafc;
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

.create-panel {
  flex: 1;
  padding: 32px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  align-items: center;
  min-height: 0;
}

.form-section {
  margin-bottom: 40px;
  width: 100%;
  /* max-width: 800px; */
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
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
  width: 100%;
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
  flex: 1;
  display: flex;
  flex-direction: column;
  margin-bottom: 24px;
  width: 100%;
  /* max-width: 800px; */
  min-height: 0;
  overflow: hidden;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  flex-shrink: 0;
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
  flex: 1;
  background: #f9fafb;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.column-header {
  flex-shrink: 0;
  display: grid;
  grid-template-columns: 1.5fr 1fr 0.8fr 0.8fr 0.8fr 0.5fr;
  gap: 16px;
  padding: 16px;
  background: #f3f4f6;
  border-bottom: 1px solid #e5e7eb;
  font-size: 0.85rem;
  color: #6b7280;
  font-weight: 500;
  text-align: center;
}

.column-header span {
  display: flex;
  align-items: center;
  justify-content: center;
}

.column-rows-container {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
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
  text-align: center;
  flex-shrink: 0;
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
  text-align: left;
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
  margin: 0 auto;
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
  width: 100%;
  /* max-width: 800px; */
  display: flex;
  justify-content: center;
  flex-shrink: 0;
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

.column-rows-container::-webkit-scrollbar {
  width: 6px;
}

.column-rows-container::-webkit-scrollbar-track {
  background: #f3f4f6;
  border-radius: 3px;
}

.column-rows-container::-webkit-scrollbar-thumb {
  background: #d1d5db;
  border-radius: 3px;
}

.column-rows-container::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}
</style>
