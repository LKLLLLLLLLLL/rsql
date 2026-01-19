<!-- DropTableModal.vue -->
<template>
  <div v-if="visible" class="modal-overlay">
    <div class="modal-dialog">
      <h3><Icon :path="mdiAlertOctagonOutline" size="18" /> 确认删除</h3>
      <p>确定删除表 <strong>{{ tableName }}</strong>？此操作不可撤销。</p>
      <div class="modal-actions">
        <button class="modal-cancel" @click="emit('cancel')">取消</button>
        <button class="modal-confirm" @click="emit('confirm')">
          <Icon :path="mdiCheckCircleOutline" size="16" /> 确认删除
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits } from 'vue'
import Icon from './Icon.vue'
import {
  mdiAlertOctagonOutline,
  mdiCheckCircleOutline,
} from '@mdi/js'

defineProps({
  visible: { type: Boolean, default: false },
  tableName: { type: String, default: '' }
})

const emit = defineEmits(['cancel', 'confirm'])
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

.modal-confirm {
  background: #e74c3c;
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
  background: #c0392b;
}
</style>