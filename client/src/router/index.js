import { createRouter, createWebHistory } from 'vue-router'
import Login from '../components/Login.vue'
import DatabasePage from '../components/DatabasePage.vue'

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
router.beforeEach((to, from, next) => {
  if (to.path === '/') return next()
  try {
    const u = typeof window !== 'undefined' ? localStorage.getItem('username') : null
    const p = typeof window !== 'undefined' ? localStorage.getItem('password') : null
    if (!u || !p) return next({ path: '/' })
  } catch (e) {
    return next({ path: '/' })
  }
  next()
})

export default router
