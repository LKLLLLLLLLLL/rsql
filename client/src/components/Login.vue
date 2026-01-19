<template>
  <div class="auth">
    <section class="hero">
      <div class="logo">RSQL</div>
      <h2>轻量数据库云控台</h2>
      <p>弹性 · 好用 · 省心</p>
    </section>

    <section class="card">
      <div class="tabs">
        <button :class="['tab', { active: mode === 'login' }]" @click="setMode('login')">登录</button>
        <button :class="['tab', { active: mode === 'register' }]" @click="setMode('register')">注册</button>
      </div>

      <form class="form" @submit.prevent="handleSubmit">
        <div v-if="mode === 'register'" class="field">
          <label for="name">姓名</label>
          <input id="name" v-model.trim="form.name" type="text" placeholder="你的名字" autocomplete="name" />
        </div>

        <div class="field">
          <label for="email">邮箱</label>
          <input
            id="email"
            v-model.trim="form.email"
            type="email"
            placeholder="name@example.com"
            autocomplete="email"
            required
          />
        </div>

        <div class="field">
          <label for="password">密码</label>
          <input
            id="password"
            v-model.trim="form.password"
            type="password"
            placeholder="请输入密码"
            autocomplete="current-password"
            required
          />
        </div>

        <div v-if="mode === 'register'" class="field">
          <label for="confirm">确认密码</label>
          <input
            id="confirm"
            v-model.trim="form.confirm"
            type="password"
            placeholder="再次输入密码"
            autocomplete="new-password"
            required
          />
        </div>

        <div class="actions">
          <label class="remember">
            <input v-if="mode === 'login'" v-model="remember" type="checkbox" />
            <span>{{ mode === 'login' ? '记住我' : '已阅读并同意使用条款' }}</span>
          </label>
          <button class="submit" type="submit" :disabled="pending">
            <span v-if="!pending">{{ mode === 'login' ? '登录' : '注册并登录' }}</span>
            <span v-else>处理中...</span>
          </button>
        </div>

        <p v-if="message" class="message" :class="{ error: messageType === 'error', success: messageType === 'success' }">
          {{ message }}
        </p>
      </form>
    </section>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()
const mode = ref('login')
const remember = ref(true)
const pending = ref(false)
const message = ref('')
const messageType = ref('success')

const form = reactive({
  name: '',
  email: '',
  password: '',
  confirm: '',
})

const resetMessage = () => {
  message.value = ''
}

const setMode = (next) => {
  if (mode.value === next) return
  mode.value = next
  resetMessage()
}

const simulateAuth = async () => {
  pending.value = true
  resetMessage()
  await new Promise((resolve) => setTimeout(resolve, 360))
  pending.value = false
}

const handleSubmit = async () => {
  if (!form.email || !form.password) {
    messageType.value = 'error'
    message.value = '请填写邮箱和密码'
    return
  }
  if (mode.value === 'register' && form.password !== form.confirm) {
    messageType.value = 'error'
    message.value = '两次密码不一致'
    return
  }
  await simulateAuth()
  messageType.value = 'success'
  message.value = mode.value === 'login' ? '登录成功，即将进入控制台' : '注册成功，已为你登录'
  router.push('/database')
}
</script>

<style scoped>
:global(body) {
  margin: 0;
  background: #f8f9fd;
  color: #0f172a;
  font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
}

.auth {
  min-height: 100vh;
  display: grid;
  grid-template-columns: 420px 1fr;
  background: #f8f9fd;
}

.hero {
  background: radial-gradient(circle at 20% 20%, rgba(255, 255, 255, 0.08), transparent 48%),
    linear-gradient(160deg, #0c1a3a 0%, #0a1531 100%);
  color: #fff;
  padding: 64px 48px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 16px;
}

.logo {
  font-size: 18px;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.hero h2 {
  font-size: 28px;
  margin: 0;
}

.hero p {
  margin: 0;
  color: #d7e2ff;
}

.card {
  max-width: 420px;
  width: 100%;
  margin: auto;
  background: #ffffff;
  border-radius: 18px;
  padding: 32px;
  box-shadow: 0 20px 60px rgba(15, 23, 42, 0.08);
}

.tabs {
  display: inline-flex;
  background: #f1f5f9;
  border-radius: 12px;
  padding: 4px;
  margin-bottom: 20px;
}

.tab {
  border: none;
  background: transparent;
  padding: 10px 18px;
  border-radius: 10px;
  font-weight: 600;
  color: #475569;
  cursor: pointer;
  transition: background 0.15s ease, color 0.15s ease;
}

.tab.active {
  background: #315efb;
  color: #ffffff;
}

.form {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

label {
  font-size: 13px;
  color: #475569;
}

input[type='email'],
input[type='password'],
input[type='text'] {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  padding: 12px 14px;
  font-size: 14px;
  transition: border 0.16s ease, box-shadow 0.16s ease;
}

input:focus {
  outline: none;
  border-color: #315efb;
  box-shadow: 0 0 0 3px rgba(49, 94, 251, 0.16);
}

.actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-top: 6px;
}

.remember {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #475569;
  font-size: 13px;
}

.remember input {
  accent-color: #315efb;
}

.submit {
  border: none;
  background: #315efb;
  color: #ffffff;
  padding: 12px 18px;
  border-radius: 10px;
  font-weight: 700;
  cursor: pointer;
  min-width: 140px;
  transition: transform 0.12s ease, box-shadow 0.16s ease;
  box-shadow: 0 12px 30px rgba(49, 94, 251, 0.24);
}

.submit:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 16px 36px rgba(49, 94, 251, 0.28);
}

.submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.message {
  margin: 6px 0 0;
  font-size: 13px;
  color: #475569;
}

.message.error {
  color: #e11d48;
}

.message.success {
  color: #16a34a;
}

@media (max-width: 960px) {
  .auth {
    grid-template-columns: 1fr;
  }

  .hero {
    min-height: 200px;
    border-bottom-left-radius: 20px;
    border-bottom-right-radius: 20px;
  }

  .card {
    margin: 24px auto 40px;
  }
}
</style>
