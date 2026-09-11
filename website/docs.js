(() => {
  'use strict';
  const sidebar = document.querySelector('.docs-sidebar');
  const toggle = document.querySelector('.doc-nav-toggle');
  if (sidebar && toggle) {
    toggle.hidden = false;
    sidebar.classList.add('nav-collapsed');
    toggle.setAttribute('aria-expanded', 'false');
    toggle.addEventListener('click', () => {
      const collapsed = sidebar.classList.toggle('nav-collapsed');
      toggle.setAttribute('aria-expanded', String(!collapsed));
      toggle.textContent = collapsed ? 'Browse guides' : 'Hide guides';
    });
  }
  const search = document.querySelector('#doc-search');
  const links = [...document.querySelectorAll('#doc-nav a')];
  if (search) {
    search.hidden = false;
    search.addEventListener('input', () => {
      const query = search.value.trim().toLocaleLowerCase();
      if (query && sidebar && toggle) {
        sidebar.classList.remove('nav-collapsed');
        toggle.setAttribute('aria-expanded', 'true');
        toggle.textContent = 'Hide guides';
      }
      let count = 0;
      links.forEach(link => {
        link.hidden = !link.textContent.toLocaleLowerCase().includes(query);
        if (!link.hidden) count++;
      });
      document.querySelector('#search-status').textContent = query ? (count ? `${count} matching guides` : 'No matching guides. Try “sync”, “search”, or “clients”.') : '';
    });
  }
  document.querySelectorAll('.doc-prose pre:has(code)').forEach(block => {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'code-copy'; button.textContent = 'Copy';
    button.setAttribute('aria-label', 'Copy code example');
    const code = block.querySelector('code');
    const status = document.createElement('span');
    status.className = 'doc-copy-status'; status.setAttribute('role', 'status');
    button.addEventListener('click', async () => {
      try { await navigator.clipboard.writeText(code.textContent); button.textContent = 'Copied'; status.textContent = 'Code copied.'; }
      catch { status.textContent = 'Select this example to copy it; clipboard access is unavailable.'; }
    });
    block.classList.add('has-copy'); block.append(button); block.after(status);
  });
  document.querySelectorAll('.doc-prose table').forEach(table => {
    const wrapper = document.createElement('div');
    wrapper.className = 'table-scroll'; wrapper.tabIndex = 0;
    wrapper.setAttribute('role', 'region'); wrapper.setAttribute('aria-label', 'Reference table, scroll horizontally if needed');
    table.before(wrapper); wrapper.append(table);
  });
})();
