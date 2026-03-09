
// Lune Admins Panel Script
// Получение статистики с backend
async function fetchStats() {
  try {
    const res = await fetch('/api/stats');
    if (!res.ok) throw new Error('Failed to fetch stats');
    const data = await res.json();
    document.getElementById('stat-users').textContent = data.users_count ?? '-';
    document.getElementById('stat-active').textContent = data.products_count ?? '-';
    document.getElementById('stat-commands').textContent = data.orders_count ?? '-';
  } catch (e) {
    console.error(e);
  }
}
window.addEventListener('DOMContentLoaded', fetchStats);


// Sidebar navigation logic + section switching
const sidebarBtns = document.querySelectorAll('.sidebar-btn');

const sections = [
  document.getElementById('section-dashboard'),
  document.getElementById('section-users'),
  document.getElementById('section-products'),
  document.getElementById('section-logs'),
  document.getElementById('section-broadcast'),
  document.getElementById('section-profile'),
  document.getElementById('section-settings')
];
sidebarBtns.forEach((btn, idx) => {
  btn.addEventListener('click', () => {
    sidebarBtns.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    sections.forEach((sec, i) => {
      if (sec) sec.style.display = (i === idx) ? '' : 'none';
    });
    // Автозагрузка данных для разделов
    if (idx === 1) loadUsers();
    if (idx === 2) loadProducts();
    if (idx === 3) loadLogs();
    if (idx === 4) setupBroadcast();
    if (idx === 5) loadProfile();
    // Настройки не требуют загрузки
  });
});

// Автопоказ дашборда при загрузке
window.addEventListener('DOMContentLoaded', () => {
  sections.forEach((sec, i) => { sec.style.display = (i === 0) ? '' : 'none'; });
});
// Реальная рассылка через backend
function setupBroadcast() {
  const form = document.getElementById('broadcast-form');
  const status = document.getElementById('broadcast-status');
  if (!form) return;
  form.onsubmit = async (e) => {
    e.preventDefault();
    const msg = document.getElementById('broadcast-message').value.trim();
    if (!msg) { status.textContent = 'Введите текст сообщения!'; return; }
    status.textContent = 'Отправка...';
    try {
      const res = await fetch('/api/broadcast', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg })
      });
      const data = await res.json();
      if (data.ok) status.textContent = 'Рассылка отправлена!';
      else status.textContent = 'Ошибка отправки';
    } catch (e) { status.textContent = 'Ошибка'; }
  };
}

// Реальный профиль админа
async function loadProfile() {
  const el = document.getElementById('profile-info');
  el.textContent = 'Загрузка...';
  try {
    const res = await fetch('/api/profile');
    const data = await res.json();
    if (data.ok) {
      el.innerHTML = `<b>ID:</b> ${data.id}<br><b>Логин:</b> ${data.login}<br><b>Роль:</b> ${data.role}`;
    } else {
      el.textContent = 'Ошибка загрузки';
    }
  } catch (e) { el.textContent = 'Ошибка'; }
}

// Заглушки для загрузки данных
async function loadUsers() {
  const el = document.getElementById('users-list');
  el.textContent = 'Загрузка...';
  try {
    const res = await fetch('/api/users');
    const data = await res.json();
    if (data.ok && Array.isArray(data.users)) {
      el.innerHTML = data.users.length ?
        '<ul>' + data.users.map(u => `<li>ID: ${u[0]}</li>`).join('') + '</ul>' :
        'Нет пользователей';
    } else {
      el.textContent = 'Ошибка загрузки';
    }
  } catch (e) { el.textContent = 'Ошибка'; }
}
async function loadProducts() {
  const el = document.getElementById('products-list');
  el.textContent = 'Загрузка...';
  try {
    const res = await fetch('/api/admin_products');
    const data = await res.json();
    if (data.ok && Array.isArray(data.products)) {
      el.innerHTML = data.products.length ?
        '<ul>' + data.products.map(p => `<li>${p.title} (${p.category}) — ${p.price}₽, ${p.stock} шт.</li>`).join('') + '</ul>' :
        'Нет товаров';
    } else {
      el.textContent = 'Ошибка загрузки';
    }
  } catch (e) { el.textContent = 'Ошибка'; }
}
async function loadLogs() {
  const el = document.getElementById('logs-list');
  el.textContent = 'Загрузка...';
  try {
    const res = await fetch('/api/logs');
    const data = await res.json();
    if (data.ok && Array.isArray(data.logs)) {
      el.innerHTML = data.logs.length ?
        '<ul>' + data.logs.map(l => `<li>${l[4]} — <b>${l[2]}</b>: ${l[3]}</li>`).join('') + '</ul>' :
        'Нет логов';
    } else {
      el.textContent = 'Ошибка загрузки';
    }
  } catch (e) { el.textContent = 'Ошибка'; }
}
