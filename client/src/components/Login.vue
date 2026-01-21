<template>
  <div class="auth">
    <section class="hero">
      <div class="logo-container">
        <div class="icon-container">
          <div class="logo-icon">
            <Icon :path="mdiDatabase" size="36" />
          </div>
        </div>
        <div class="logo-text">
          <!-- <div class="logo">RSQL</div> -->
          <div class="text-container">
            <h2>RSQL Dashboard</h2>
            <p>A modern relational database system built with Rust</p>
          </div>
        </div>
      </div>
    </section>

    <section class="card">
      <form class="form" @submit.prevent="handleSubmit">
        <div class="login-title">
          <h2>Log In</h2>
        </div>
        <div class="field">
          <label for="username">Username</label>
          <input id="username" v-model.trim="form.username" type="text" placeholder="Enter username" autocomplete="username" />
        </div>

        <div class="field">
          <label for="password">Password</label>
          <input
            id="password"
            v-model.trim="form.password"
            type="password"
            placeholder="Enter password"
            autocomplete="current-password"
            required
          />
        </div>

        <div class="actions">
          <button class="submit" type="submit" :disabled="pending">
            <span v-if="!pending">Loge In</span>
            <span v-else>Processing...</span>
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
import Icon from './Icon.vue'
import { mdiDatabase } from '@mdi/js'

const router = useRouter()
const pending = ref(false)
const message = ref('')
const messageType = ref('success')

const form = reactive({
  username: '',
  password: '',
})

const resetMessage = () => {
  message.value = ''
}

const simulateAuth = async () => {
  pending.value = true
  resetMessage()
  await new Promise((resolve) => setTimeout(resolve, 360))
  pending.value = false
}

const handleSubmit = async () => {
  await simulateAuth()
  console.log('Login credentials:', { username: form.username, password: form.password })
  try {
    localStorage.setItem('username', form.username || '')
    localStorage.setItem('password', form.password || '')
  } catch {}
  messageType.value = 'success'
  message.value = 'Login successful, redirecting to dashboard...'
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
  grid-template-columns: 1fr max(70vw, 600px);
  background: #f8f9fd;
}

.hero {
  background: radial-gradient(circle at 20% 20%, rgba(255, 255, 255, 0.08), transparent 48%),
    linear-gradient(160deg, #0c1a3a 0%, #0a1531 100%);
  color: #fff;
  padding: 64px 64px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 16px;
}

.logo-container {
  display: flex;
  flex-direction: row; /* 横向排列：图标在左，文字在右 */
  align-items: center;
  gap: 20px;
  /* constrain content width so hero looks good on wide layouts */
  max-width: 900px;
  margin: 0 auto;
  padding: 8px 0;
  transform: translateY(-50px);
}

.icon-container{
  display: flex;
  align-items: center;
  justify-content: center;
  width: auto;
}

.logo-icon {
  padding: 16px;
  background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(99, 102, 241, 0.3) 100%);
  border-radius: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 80px;
  height: 80px;
  border: 1px solid rgba(99, 102, 241, 0.2);
  box-shadow: 0 8px 24px rgba(99, 102, 241, 0.2);
}

.logo-text {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
}

.text-container{
  max-width: 640px;
  width: 100%;
  text-align: left;
}

.logo {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: #f8fafc;
}

.hero h2 {
  font-size: 32px;
  margin: 0;
  font-weight: 700;
  color: #f8fafc;
}

.hero p {
  margin: 0;
  color: #d7e2ff;
  font-size: 15px;
  line-height: 1.5;
  opacity: 0.9;
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
  font-weight: 500;
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
  width: 100%;
  box-sizing: border-box;
}

input:focus {
  outline: none;
  border-color: #315efb;
  box-shadow: 0 0 0 3px rgba(49, 94, 251, 0.16);
}
.actions {
  display: block;
  margin-top: 6px;
}

.submit {
  border: none;
  background: #315efb;
  color: #ffffff;
  padding: 12px 18px;
  width: 100%;
  border-radius: 10px;
  font-weight: 700;
  cursor: pointer;
  width: 100%;
  box-sizing: border-box;
  transition: transform 0.12s ease, box-shadow 0.16s ease;
  box-shadow: 0 12px 30px rgba(49, 94, 251, 0.24);
}

.submit:hover:not(:disabled) {
  box-shadow: 0 16px 36px rgba(49, 94, 251, 0.28);
}

.submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.login-title h2 {
  color: #0f172a;
  font-weight: 700;
  font-size: 24px;
  /* margin: 0 0 16px; */
  text-align: center;
}

.message {
  margin: 6px 0 0;
  font-size: 13px;
  color: #475569;
  text-align: center;
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
    padding: 40px 32px;
  }

  .logo-container {
    align-items: center;
    text-align: center;
    gap: 20px;
    flex-direction: column;
  }

  .card {
    margin: 24px auto 40px;
    max-width: 380px;
  }
}
</style>
