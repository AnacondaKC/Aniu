import { createApp } from 'vue'
import { createPinia } from 'pinia'
import router from './router'
import App from './App.vue'
import { useAppStore } from '@/stores/legacy'
import { getStoredLoginFlag, getStoredToken } from '@/services/api'
import './style.css'

const app = createApp(App)
const pinia = createPinia()

app.use(pinia)
app.use(router)

const store = useAppStore(pinia)
if (getStoredLoginFlag() && getStoredToken()) {
  void store.initialize()
}

app.mount('#app')
