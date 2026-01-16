<template>
  <Transition name="fade">
    <div v-if="visible" class="toast toast-success">
      <div class="toast-content">
        <span class="toast-message">{{ message }}</span>
        <button @click="hide" class="toast-close" aria-label="close">×</button>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref } from 'vue'

const props = defineProps({
  message: { type: String, required: true },
  duration: { type: Number, default: 3000 },
  autoStart: { type: Boolean, default: false }
})

const visible = ref(false)
let timer = null

const clearTimer = () => {
  if (timer) {
    clearTimeout(timer)
    timer = null
  }
}

const hide = () => {
  visible.value = false
  clearTimer()
}

const show = () => {
  clearTimer()
  visible.value = true
  timer = setTimeout(() => hide(), props.duration)
}

defineExpose({ show, hide })

onMounted(() => {
  if (props.autoStart) show()
})

onBeforeUnmount(() => clearTimer())
</script>

<style scoped>
.toast {
  position: fixed;
  top: 20px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 9999;
  min-width: 280px;
  max-width: 520px;
  border-radius: 4px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.toast-success {
  background-color: #52c41a;
  color: #fff;
}

.toast-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
}

.toast-message {
  flex: 1;
  margin-right: 12px;
}

.toast-close {
  background: none;
  border: none;
  color: #fff;
  font-size: 20px;
  cursor: pointer;
  padding: 0;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.fade-enter-active,
.fade-leave-active {
  transition: all 0.25s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
  transform: translate(-50%, -16px);
}
</style>
