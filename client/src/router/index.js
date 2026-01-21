import { createRouter, createWebHistory } from 'vue-router'
import Login from '../components/Login.vue'
import DatabasePage from '../components/DatabasePage.vue'
import { connect as wsConnect, connected as wsConnected, addOpenListener, removeOpenListener, addErrorListener, removeErrorListener } from '../services/wsService'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'login',
      component: Login,
    },
    {
      path: '/database',
      name: 'database',
      component: DatabasePage,
    },
  ],
})

// route guard: check auth for non-login pages
router.beforeEach(async (to, from, next) => {
  if (to.path === '/') return next()

  try {
    const u = typeof window !== 'undefined' ? localStorage.getItem('username') : null
    const p = typeof window !== 'undefined' ? localStorage.getItem('password') : null
    if (!u || !p) return next({ path: '/' })
  } catch (e) {
    return next({ path: '/' })
  }

  // Ensure websocket connection is established before allowing access to protected pages.
  try {
    const ok = await new Promise((resolve) => {
      // already connected
      if (wsConnected && wsConnected.value) return resolve(true)

      let resolved = false
      const cleanup = () => {
        removeOpenListener(onOpen)
        removeErrorListener(onError)
      }

      const onOpen = () => { if (!resolved) { resolved = true; cleanup(); resolve(true) } }
      const onError = () => { if (!resolved) { resolved = true; cleanup(); resolve(false) } }

      addOpenListener(onOpen)
      addErrorListener(onError)

      const started = wsConnect()
      if (!started) {
        cleanup()
        return resolve(false)
      }

      // timeout
      setTimeout(() => {
        if (!resolved) { resolved = true; cleanup(); resolve(false) }
      }, 2000)
    })

    if (!ok) return next({ path: '/' })
  } catch (e) {
    return next({ path: '/' })
  }

  next()
})

export default router
