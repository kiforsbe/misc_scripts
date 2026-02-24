// ==UserScript==
// @name         MyAnimeList AddToList Improved
// @namespace    http://tampermonkey.net/
// @version      0.5.0
// @icon         https://myanimelist.net/favicon.ico
// @description  Adds a quick-status dropdown to every btn-anime-watch-status button on MyAnimeList season pages.
// @match        https://myanimelist.net/anime/season/*
// @match        https://myanimelist.net/anime/*
// @grant        GM_addStyle
// @run-at       document-idle
// ==/UserScript==

(function () {
  'use strict';

  // Status list and display order
  const STATUSES = [
    { id: 'plan_to_watch', label: 'Plan to Watch', num: 6 },
    { id: 'watching',      label: 'Watching',      num: 1 },
    { id: 'on_hold',       label: 'On Hold',        num: 3 },
    { id: 'completed',     label: 'Completed',      num: 2 },
    { id: 'dropped',       label: 'Dropped',        num: 4 },
  ];

  GM_addStyle(`
    /* Trigger chevron button — visually a right-side extension of the host button */
    .mal-qs-trigger {
      cursor: pointer;
      vertical-align: middle;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0 6px;
      border: none;
      background: transparent;
      line-height: 0;
      margin-left: 0;
      box-sizing: border-box;
    }
    .mal-qs-trigger:focus-visible { outline: 2px solid #4c9aff; }
    .mal-qs-trigger svg { display: block; }

    /* Floating dropdown menu */
    .mal-qs-menu {
      position: fixed;
      z-index: 99999;
      display: none;
      flex-direction: column;
      gap: 4px;
      padding: 5px;
      box-sizing: border-box;
      background: #fff;
      border: 1px solid rgba(0,0,0,0.15);
      border-radius: 6px;
      box-shadow: 0 4px 16px rgba(0,0,0,0.18);
    }
    .mal-qs-menu.open { display: flex; }

    /* Each status item — a full-width button matching the host button style */
    .mal-qs-item {
      display: block;
      width: 100%;
      box-sizing: border-box;
      text-align: left;
      cursor: pointer;
      border-radius: 4px;
      border: 1px solid transparent;
      white-space: nowrap;
    }
    .mal-qs-item:hover,
    .mal-qs-item:focus { filter: brightness(1.12); outline: none; }
    .mal-qs-item[aria-current="true"] { opacity: 0.55; cursor: default; }
    .mal-qs-item[aria-current="true"]:hover { filter: none; }

    /* Dark mode */
    @media (prefers-color-scheme: dark) {
      .mal-qs-menu { background: #1a1a1a; border-color: rgba(255,255,255,0.1); box-shadow: 0 4px 18px rgba(0,0,0,0.6); }
    }
  `);

  // -- Single global menu ---------------------------------------------------
  const menu = document.createElement('div');
  menu.className = 'mal-qs-menu';
  menu.setAttribute('role', 'listbox');
  menu.setAttribute('aria-label', 'Select watch status');
  document.body.appendChild(menu);

  let activeTrigger = null;

  function openMenu(trigger, animeId, currentStatusNum, btnStyle, notYetAired) {
    // If same trigger clicked again, toggle close
    if (activeTrigger === trigger && menu.classList.contains('open')) {
      closeMenu();
      return;
    }
    closeMenu();

    activeTrigger = trigger;
    trigger.setAttribute('aria-expanded', 'true');

    // Build items
    menu.innerHTML = '';
    STATUSES.forEach(s => {
      const btn = document.createElement('button');
      btn.className = 'mal-qs-item';
      btn.type = 'button';
      btn.textContent = s.label;
      btn.dataset.num = s.num;
      // Copy the host button visual style so items look native
      if (btnStyle) {
        btn.style.background   = btnStyle.background;
        btn.style.color        = btnStyle.color;
        btn.style.fontSize     = btnStyle.fontSize;
        btn.style.fontFamily   = btnStyle.fontFamily;
        btn.style.fontWeight   = btnStyle.fontWeight;
        btn.style.padding      = btnStyle.padding;
        btn.style.borderRadius = btnStyle.borderRadius;
        btn.style.border       = btnStyle.border;
      }
      // Disable the current status
      if (s.num === currentStatusNum) {
        btn.setAttribute('aria-current', 'true');
        btn.disabled = true;
      }
      // For not-yet-aired titles, only "Plan to Watch" is valid
      if (notYetAired && (s.num === 1 || s.num === 2 || s.num === 3 || s.num === 4)) {
        btn.disabled = true;
        btn.title = 'Not available — title has not aired yet';
        btn.style.opacity = '0.35';
        btn.style.cursor  = 'not-allowed';
      }
      btn.addEventListener('click', () => {
        applyStatus(animeId, s.num);
        closeMenu();
      });
      menu.appendChild(btn);
    });

    menu.classList.add('open');
    // Width = host button + trigger combined; right-edge-aligned in positionMenu
    const hostBtn = trigger.previousElementSibling;
    menu.style.width = hostBtn
      ? (hostBtn.offsetWidth + trigger.offsetWidth) + 'px'
      : '';
    positionMenu(trigger);
  }

  function closeMenu() {
    if (activeTrigger) {
      activeTrigger.setAttribute('aria-expanded', 'false');
      activeTrigger = null;
    }
    menu.classList.remove('open');
  }

  function positionMenu(anchor) {
    const rect = anchor.getBoundingClientRect();
    const mw   = menu.offsetWidth  || 120;
    const mh   = menu.offsetHeight || 160;
    let left   = rect.right - mw;  // right-align to trigger's right edge
    let top    = rect.bottom + 4;
    if (left < 6) left = 6;
    if (top  + mh > window.innerHeight - 6) top  = rect.top - mh - 4;
    menu.style.left = left + 'px';
    menu.style.top  = top  + 'px';
  }

  // Close on outside click
  document.addEventListener('click', (e) => {
    if (!menu.classList.contains('open')) return;
    if (e.target.closest('.mal-qs-menu') || e.target.closest('.mal-qs-trigger')) return;
    closeMenu();
  }, true);

  // Keyboard: Escape, arrow navigation
  menu.addEventListener('keydown', (e) => {
    const items = Array.from(menu.querySelectorAll('.mal-qs-item:not([disabled])'));
    const idx   = items.indexOf(document.activeElement);
    if (e.key === 'Escape')    { closeMenu(); activeTrigger && activeTrigger.focus(); }
    if (e.key === 'ArrowDown') { e.preventDefault(); (items[idx + 1] || items[0]).focus(); }
    if (e.key === 'ArrowUp')   { e.preventDefault(); (items[idx - 1] || items[items.length - 1]).focus(); }
  });

  // -- Apply status ---------------------------------------------------------
  function applyStatus(animeId, statusNum) {
    // Click the real button to trigger MAL's native dialog, then preselect status
    const hostBtn = document.querySelector(`.btn-anime-watch-status[data-id="${animeId}"]`);
    if (hostBtn) {
      hostBtn.click();
      preselectInModal(statusNum);
    } else {
      // Fallback: direct navigation
      window.location.href =
        `https://myanimelist.net/ownlist/anime/add?selected_series_id=${animeId}&status=${statusNum}`;
    }
  }

  function preselectInModal(statusNum) {
    const desired = String(statusNum);
    if (trySet(desired)) return;
    const obs = new MutationObserver(() => { if (trySet(desired)) obs.disconnect(); });
    obs.observe(document.body, { childList: true, subtree: true });
    setTimeout(() => obs.disconnect(), 5000);
  }

  function trySet(desired) {
    // 1. <select>
    const sel = document.querySelector(
      'select[name="status"], select#myinfo_status, select[name="list_status"]'
    );
    if (sel) {
      sel.value = desired;
      sel.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    }
    // 2. Radio
    for (const name of ['status', 'list_status', 'my_status']) {
      const radio = document.querySelector(
        `input[type="radio"][name="${name}"][value="${desired}"]`
      );
      if (radio) { radio.click(); return true; }
    }
    // 3. Text buttons inside dialog
    const labelMap = { '1': 'Watching', '2': 'Completed', '3': 'On Hold', '4': 'Dropped', '6': 'Plan to Watch' };
    const want = labelMap[desired];
    if (want) {
      const dialog = document.querySelector('[role="dialog"], .modal, #add-to-list-wrapper');
      const scope  = dialog || document;
      const found  = Array.from(scope.querySelectorAll('button, a, li, label')).find(
        el => (el.textContent || '').trim().toLowerCase() === want.toLowerCase()
      );
      if (found) { found.click(); return true; }
    }
    return false;
  }

  // -- Augment buttons ------------------------------------------------------
  function augmentButton(btn) {
    if (btn.dataset.malQsAugmented) return;

    const animeId = btn.dataset.id || btn.dataset.animeId || getIdFromContext(btn);
    if (!animeId) return;

    // Only mark as augmented after confirming we have an ID, so failed lookups
    // can be retried on the next scan (e.g. when the card finishes rendering).
    btn.dataset.malQsAugmented = '1';

    const currentStatusNum = getCurrentStatus(btn);

    // Connect host button visually to the trigger: flatten right corners and remove right border
    btn.style.borderTopRightRadius    = '0';
    btn.style.borderBottomRightRadius = '0';
    btn.style.borderRight             = 'none';

    // Snapshot button style to apply to menu items (before modifying btn further)
    const cs = window.getComputedStyle(btn);
    const btnStyle = {
      background:   cs.background,
      color:        cs.color,
      fontSize:     cs.fontSize,
      fontFamily:   cs.fontFamily,
      fontWeight:   cs.fontWeight,
      padding:      cs.padding,
      borderRadius: cs.borderRadius,
      border:       `${cs.borderTopWidth} ${cs.borderTopStyle} ${cs.borderTopColor}`,
    };

    // Create chevron trigger inserted right after the host button
    const trigger = document.createElement('button');
    trigger.className = 'mal-qs-trigger';
    trigger.type = 'button';
    trigger.title = 'Quick-set watch status';
    trigger.setAttribute('aria-haspopup', 'listbox');
    trigger.setAttribute('aria-expanded', 'false');
    // offsetHeight includes padding+border — use it so trigger matches host exactly
    trigger.style.height                  = btn.offsetHeight + 'px';
    trigger.style.background               = cs.background;
    trigger.style.color                    = cs.color;
    // Match host border on top/right/bottom; no left border (seamless join)
    const bdr = `${cs.borderTopWidth} ${cs.borderTopStyle} ${cs.borderTopColor}`;
    trigger.style.borderTop                = bdr;
    trigger.style.borderRight              = bdr;
    trigger.style.borderBottom             = bdr;
    trigger.style.borderLeft               = 'none';
    trigger.style.borderTopRightRadius     = cs.borderTopRightRadius;
    trigger.style.borderBottomRightRadius  = cs.borderBottomRightRadius;
    trigger.innerHTML = `<svg width="10" height="10" viewBox="0 0 20 20" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M5 7l5 5 5-5z"/></svg>`;

    const notYetAired = isNotYetAired(btn);

    trigger.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      openMenu(trigger, animeId, currentStatusNum, btnStyle, notYetAired);
    });

    btn.insertAdjacentElement('afterend', trigger);
  }

  function isNotYetAired(btn) {
    // Applies to any watch-status button (including already-set Plan to Watch)
    const card = btn.closest('.js-seasonal-anime, .seasonal-anime, li, article');
    if (!card) return false;
    // Date is in .prodsrc .info span.item (e.g. "Mar 29, 2026")
    const dateEl = card.querySelector('.prodsrc .info, .prodsrc');
    const text = (dateEl || card).textContent || '';
    const m = text.match(/([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})/);
    if (!m) return false;
    const d = new Date(m[1]);
    return !isNaN(d.getTime()) && d > new Date();
  }

  function getCurrentStatus(btn) {
    if (btn.dataset.status) return parseInt(btn.dataset.status, 10);
    const text = (btn.textContent || '').trim().toLowerCase();
    if (text === 'watching')         return 1;
    if (text === 'completed')        return 2;
    if (text === 'on hold')          return 3;
    if (text === 'dropped')          return 4;
    if (/plan.to.watch/i.test(text)) return 6;
    return 0;
  }

  function getIdFromContext(btn) {
    // Walk up the DOM looking for data-id / data-anime-id on ancestors
    let el = btn.parentElement;
    while (el && el !== document.body) {
      if (el.dataset.id)      return el.dataset.id;
      if (el.dataset.animeId) return el.dataset.animeId;
      el = el.parentElement;
    }
    // Look inside the nearest card for any link that contains an anime ID
    const card = btn.closest('div, li, article, tr');
    if (card) {
      // /ownlist/ add link (logged-out / notify)
      const ownlistLink = card.querySelector('a[href*="selected_series_id="]');
      if (ownlistLink) {
        const m = ownlistLink.href.match(/selected_series_id=(\d+)/);
        if (m) return m[1];
      }
      // Standard anime page link: /anime/12345/...
      const animeLink = card.querySelector('a[href*="/anime/"]');
      if (animeLink) {
        const m = animeLink.href.match(/\/anime\/(\d+)/);
        if (m) return m[1];
      }
    }
    return null;
  }

  // -- Scan the page --------------------------------------------------------
  function scanAndAugment(root) {
    root.querySelectorAll('.btn-anime-watch-status:not([data-mal-qs-augmented])').forEach(augmentButton);
  }

  // Initial scan
  scanAndAugment(document);

  // Observe for dynamically-added content
  new MutationObserver((mutations) => {
    for (const m of mutations) {
      for (const node of m.addedNodes) {
        if (node.nodeType !== 1) continue;
        if (node.classList && node.classList.contains('btn-anime-watch-status')) {
          augmentButton(node);
        } else if (typeof node.querySelectorAll === 'function') {
          scanAndAugment(node);
        }
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  // SPA navigation: re-scan on title change
  const titleEl = document.querySelector('head > title');
  if (titleEl) {
    new MutationObserver(() => { closeMenu(); scanAndAugment(document); })
      .observe(titleEl, { childList: true });
  }

  // Reposition open menu on scroll / resize
  window.addEventListener('scroll', () => { if (activeTrigger) positionMenu(activeTrigger); }, { passive: true });
  window.addEventListener('resize', () => { if (activeTrigger) positionMenu(activeTrigger); }, { passive: true });

  // Debug hook
  window.__malQS = { scanAndAugment, openMenu, closeMenu };

})();
