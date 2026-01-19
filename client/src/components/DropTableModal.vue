<template>
  <div v-if="visible" class="modal-overlay">
    <div class="modal-dialog">
      <div class="modal-header">
        <Icon :path="mdiAlertOctagonOutline" size="20" />
        <h3>确认删除</h3>
      </div>
      <p class="modal-message">确定删除表 <strong>{{ tableName }}</strong>？此操作不可撤销。</p>
      <div class="modal-actions">
        <button class="modal-cancel" @click="emit('cancel')">取消</button>
        <button class="modal-confirm" @click="emit('confirm')">
          <Icon :path="mdiCheckCircleOutline" size="16" />
          确认删除
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
  background: rgba(0,0,0,0.2);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  backdrop-filter: blur(2px);
}

.modal-dialog {
  background: #fff;
  border-radius: 12px;
  padding: 24px;
  width: 400px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.08);
  border: 1px solid #e3e8ef;
}

.modal-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  color: #ef4444;
}

.modal-header h3 {
  margin: 0;
  color: #1a1f36;
  font-size: 1.1rem;
  font-weight: 600;
}

.modal-message {
  color: #6b7280;
  margin: 16px 0;
  font-size: 0.95rem;
  line-height: 1.5;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 24px;
}

.modal-cancel {
  padding: 10px 20px;
  background: #f3f4f6;
  color: #4b5563;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.9rem;
  transition: all 0.2s ease;
}

.modal-cancel:hover {
  background: #e5e7eb;
}

.modal-confirm {
  padding: 10px 20px;
  background: #ef4444;
  color: #fff;
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

.modal-confirm:hover {
  background: #dc2626;
}
</style>