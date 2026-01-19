import { createRouter, createWebHistory } from 'vue-router'
import DatabasePage from '../components/DatabasePage.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'database',
      component: DatabasePage,
    },
  ],
})

export default router
