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

export default router
