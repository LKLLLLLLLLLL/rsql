<!-- RenameTableModal.vue -->
<template>
  <div v-if="visible" class="modal-overlay">
    <div class="modal-dialog">
      <h3><Icon :path="mdiPencilOutline" size="18" /> 重命名表</h3>
      <p>重命名表 <strong>{{ oldTableName }}</strong></p>
      <input 
        v-model="newName" 
        type="text" 
        class="rename-input" 
        placeholder="输入新的表名"
        @keyup.enter="handleConfirm"
      />
      <div class="modal-actions">
        <button class="modal-cancel" @click="emit('cancel')">取消</button>
        <button class="modal-confirm" @click="handleConfirm">
          <Icon :path="mdiCheckCircleOutline" size="16" /> 确认重命名
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiPencilOutline,
  mdiCheckCircleOutline,
} from '@mdi/js'

const props = defineProps({
  visible: { type: Boolean, default: false },
  oldTableName: { type: String, default: '' }
})

const emit = defineEmits(['cancel', 'confirm'])

const newName = ref('')

// 当模态框打开时，清空输入框
watch(() => props.visible, (visible) => {
  if (visible) {
    newName.value = ''
  }
})

function handleConfirm() {
  if (!newName.value.trim()) {
    alert('请输入新的表名')
    return
  }
  emit('confirm', newName.value.trim())
  newName.value = ''
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,0.35);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-dialog {
  background: #fff;
  border-radius: 8px;
  padding: 16px 20px;
  width: 360px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.15);
}

.modal-dialog h3 {
  margin-bottom: 12px;
  color: #2c3e50;
  display: flex;
  align-items: center;
  gap: 8px;
}

.modal-dialog p {
  margin-bottom: 12px;
  color: #34495e;
}

.rename-input {
  width: 100%;
  padding: 10px 12px;
  border: 1px solid #dfe4ea;
  border-radius: 6px;
  font-size: 0.95rem;
  margin-bottom: 16px;
}

.rename-input:focus {
  outline: none;
  border-color: #3498db;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 16px;
}

.modal-cancel {
  background: #bdc3c7;
  color: #2c3e50;
  border: none;
  border-radius: 6px;
  padding: 8px 12px;
  cursor: pointer;
}

.modal-cancel:hover {
  background: #95a5a6;
}

.modal-confirm {
  background: #3498db;
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 8px 12px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
}

.modal-confirm:hover {
  background: #217dbb;
}
</style>
