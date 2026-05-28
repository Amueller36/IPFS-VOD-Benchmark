import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/run': 'http://localhost:8080',
      '/status': 'http://localhost:8080',
      '/cancel': 'http://localhost:8080',
      '/queue': 'http://localhost:8080',
      '/seed': 'http://localhost:8080',
      '/seed-info': 'http://localhost:8080',
      '/peer-count': 'http://localhost:8080',
      '/seed-bitswap': 'http://localhost:8080',
      '/seed-bitswap-info': 'http://localhost:8080',
      '/update-peers': 'http://localhost:8080',
      '/runs/download': 'http://localhost:8080',
      '/runs/clear': 'http://localhost:8080',
      '/plots/download': 'http://localhost:8080',
      '/reshape': 'http://localhost:8080'
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true
  }
});
