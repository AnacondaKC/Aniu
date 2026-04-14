import { createRouter, createWebHistory } from 'vue-router'
import { appNavigation } from '@/config/navigation'
import { getStoredLoginFlag, getStoredToken } from '@/services/api'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: appNavigation[0].path
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/LoginView.vue')
    },
    {
      path: '/overview',
      name: 'overview',
      component: () => import('@/views/OverviewView.vue')
    },
    {
      path: '/tasks',
      name: 'tasks',
      component: () => import('@/views/TasksView.vue')
    },
    {
      path: '/chat',
      name: 'chat',
      component: () => import('@/views/ChatView.vue')
    },
    {
      path: '/schedule',
      name: 'schedule',
      component: () => import('@/views/ScheduleView.vue')
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('@/views/SettingsView.vue')
    }
  ]
})

router.beforeEach((to) => {
  const isAuthenticated = getStoredLoginFlag() && !!getStoredToken()

  if (to.path === '/login') {
    if (isAuthenticated) {
      return appNavigation[0].path
    }
    return true
  }

  if (!isAuthenticated) {
    return '/login'
  }

  return true
})

export default router
