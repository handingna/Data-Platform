<template>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">T</div>
        <div>
          <div class="brand-title">Traffic 中台</div>
          <div class="brand-subtitle">运营服务 · 轨迹数据</div>
        </div>
      </div>

      <nav class="nav">
        <RouterLink class="nav-item" to="/trip">行程分析</RouterLink>
        <RouterLink class="nav-item" to="/car">车辆画像</RouterLink>
        <RouterLink class="nav-item" to="/anomaly">异常诊断</RouterLink>
      </nav>
    </aside>

    <main class="main">
      <RouterView />
    </main>
  </div>
</template>

<script setup>
</script>

<style scoped>
.app-shell {
  position: relative;
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  min-height: 100vh;
  color: var(--text);
  background:
    radial-gradient(circle at top left, rgba(79, 124, 255, 0.08), transparent 24%),
    linear-gradient(180deg, var(--bg-0), var(--bg-1));
  overflow: hidden;
}

.sidebar {
  padding: 20px;
  border-right: 1px solid var(--border);
  background: rgba(255, 255, 255, 0.76);
  backdrop-filter: blur(12px);
}

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 18px;
  padding: 14px;
  border-radius: var(--radius-lg);
  background: var(--surface-0);
  border: 1px solid var(--border);
  box-shadow: var(--shadow-sm);
}

.brand-mark {
  display: grid;
  place-items: center;
  width: 40px;
  height: 40px;
  border-radius: 12px;
  background: linear-gradient(135deg, rgba(79, 124, 255, 0.9), rgba(94, 111, 255, 0.72));
  color: #fff;
  font-weight: 700;
}

.brand-title {
  font-size: 15px;
  font-weight: 700;
}

.brand-subtitle {
  margin-top: 2px;
  font-size: 12px;
  color: var(--text-muted);
}

.nav {
  display: grid;
  gap: 10px;
}

.nav-item {
  display: block;
  padding: 12px 14px;
  border-radius: 14px;
  color: var(--text);
  text-decoration: none;
  background: rgba(255, 255, 255, 0.92);
  border: 1px solid var(--border);
}

.nav-item:hover {
  background: rgba(79, 124, 255, 0.08);
  border-color: rgba(79, 124, 255, 0.22);
}

.nav-item.router-link-active {
  background: rgba(79, 124, 255, 0.12);
  border-color: rgba(79, 124, 255, 0.26);
}

.main {
  padding: 22px;
  overflow: auto;
}

.main :deep(.page) {
  max-width: 1600px;
  margin: 0 auto;
}

@media (max-width: 980px) {
  .app-shell {
    grid-template-columns: 1fr;
  }

  .sidebar {
    border-right: 0;
    border-bottom: 1px solid var(--border);
  }
}
</style>
