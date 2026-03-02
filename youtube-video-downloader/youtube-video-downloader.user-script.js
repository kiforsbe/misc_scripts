// ==UserScript==
// @name         YouTube Downloader Service UI
// @namespace    http://tampermonkey.net/
// @version      1.7.10
// @description  Adds a download button to YouTube pages to interact with a local youtube-video-downloader-flask-ws service.
// @author       Your Name Here
// @match        https://www.youtube.com/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=youtube.com
// @grant        GM_xmlhttpRequest
// @grant        GM_addStyle
// @connect      127.0.0.1
// @connect      localhost
// @run-at       document-idle
// ==/UserScript==

(function () {
  'use strict';

  // --- Configuration ---
  const FLASK_SERVICE_BASE_URL = "http://127.0.0.1:5000"; // Your Flask service address
  const BUTTON_POLL_INTERVAL = 1000; // Check for button container every second
  const MAX_RETRIES = 15; // Stop trying after 15 seconds if container not found
  const QUICK_DOWNLOAD_STATUS_STORAGE_KEY = 'ytdl_quick_download_status_v1';
  // --- End Configuration ---

  // Runtime state for watch-page insertion retries and SPA route tracking.
  let buttonContainerInterval = null;
  let retryCount = 0;
  let currentVideoId = null;

  // --- State for Format Caching ---
  // Cached per-page `/list_formats` result to keep dropdown open/close responsive.
  let formatDataCache = null;
  let isFetchingFormats = false;
  let formatFetchError = null;
  // --- End State ---

  // --- Styles ---
  const STYLES = `
    /* Main watch-page controls */
    .ytdl-custom-button-container { display: flex; align-items: center; margin-left: 8px; font-size: 1.4rem; color: var(--yt-spec-text-primary); background-color: var(--yt-spec-badge-chip-background); padding: 0; border: none; border-radius: 18px; cursor: pointer; height: 36px; width: fit-content; }
    .ytdl-download-button { padding: 0 16px; height: 100%; display: flex; align-items: center; border: none; background-color: transparent; color: inherit; font-family: "Roboto", "Arial", sans-serif; font-size: 1.4rem; font-weight: 500; cursor: pointer; border-right: 1px solid var(--yt-spec-10-percent-layer); }
    .ytdl-dropdown-arrow { padding: 0 8px; height: 100%; display: flex; align-items: center; border: none; background-color: transparent; color: inherit; font-size: 1.6rem; cursor: pointer; }
    .ytdl-download-button:hover, .ytdl-dropdown-arrow:hover { background-color: var(--yt-spec-badge-chip-background-hover); }

    /* Format dropdown */
    .ytdl-dropdown-menu { display: none; position: fixed; background-color: var(--yt-spec-menu-background); border: 1px solid var(--yt-spec-10-percent-layer); border-radius: 4px; box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2); z-index: 10000; min-width: 200px; max-height: 300px; overflow-y: auto; color: var(--yt-spec-text-primary); }
    .ytdl-dropdown-menu.show { display: block; }
    .ytdl-dropdown-item, .ytdl-dropdown-header { padding: 8px 12px; cursor: pointer; font-size: 1.3rem; white-space: nowrap; display: block; text-decoration: none; color: inherit; }
    .ytdl-dropdown-header { font-weight: bold; cursor: default; border-bottom: 1px solid var(--yt-spec-10-percent-layer); margin-bottom: 4px; }
    .ytdl-dropdown-item:hover { background-color: var(--yt-spec-hover-background, rgba(0, 0, 0, 0.1)); }
    .ytdl-loading-indicator { padding: 10px; text-align: center; font-style: italic; color: var(--yt-spec-text-secondary); }
    .ytdl-error-message { padding: 10px; color: var(--yt-spec-error-message-color, red); font-weight: bold; }

    /* YouTube native overrides */
    ytd-download-button-renderer { display: none !important; }
    #owner.ytd-watch-metadata { min-width: calc(25% - 6px) !important; }

    /* Thumbnail quick-download overlay */
    .ytdl-thumb-overlay { position: absolute; top: 6px; left: 6px; display: flex; flex-direction: column; gap: 6px; z-index: 2147483000; pointer-events: none; opacity: 0; transition: opacity .08s ease, background-color .12s ease; }
    a#thumbnail:hover .ytdl-thumb-overlay, .ytdl-thumb-anchor:hover .ytdl-thumb-overlay, .ytdl-thumb-anchor.ytdl-thumb-active .ytdl-thumb-overlay, ytd-rich-item-renderer:hover .ytdl-thumb-overlay, ytd-rich-grid-media:hover .ytdl-thumb-overlay, yt-lockup-view-model:hover .ytdl-thumb-overlay, ytd-grid-video-renderer:hover .ytdl-thumb-overlay, ytd-compact-video-renderer:hover .ytdl-thumb-overlay, .ytdl-thumb-overlay:hover, .ytdl-thumb-btn:hover { opacity: 1; pointer-events: auto; }
    .ytdl-thumb-btn { background: rgba(0, 0, 0, 0.8); color: #fff; border-radius: 6px; width: 34px; height: 34px; display: flex; align-items: center; justify-content: center; font-size: 14px; cursor: pointer; border: none; padding: 0; opacity: 0.9; transition: opacity .08s ease, background-color .12s ease, transform .08s ease; }
    a#thumbnail:hover .ytdl-thumb-btn, .ytdl-thumb-anchor:hover .ytdl-thumb-btn, .ytdl-thumb-overlay:hover .ytdl-thumb-btn, .ytdl-thumb-btn:hover { opacity: 1; }
    .ytdl-thumb-btn:hover { background-color: rgba(0, 0, 0, 1) !important; opacity: 1 !important; }
    .ytdl-thumb-btn:focus, .ytdl-thumb-btn:active { opacity: 1 !important; background-color: rgba(0, 0, 0, 1) !important; outline: none; }
    .ytdl-thumb-btn svg { width: 18px; height: 18px; fill: currentColor; }

    /* Thumbnail downloaded-status overlay */
    .ytdl-thumb-status { position: absolute; top: 6px; left: 6px; display: flex; flex-direction: column; gap: 6px; z-index: 2147482999; pointer-events: none; opacity: 1; transition: opacity .08s ease; }
    a#thumbnail:hover .ytdl-thumb-status, .ytdl-thumb-anchor:hover .ytdl-thumb-status, .ytdl-thumb-anchor.ytdl-thumb-active .ytdl-thumb-status, ytd-rich-item-renderer:hover .ytdl-thumb-status, ytd-rich-grid-media:hover .ytdl-thumb-status, yt-lockup-view-model:hover .ytdl-thumb-status, ytd-grid-video-renderer:hover .ytdl-thumb-status, ytd-compact-video-renderer:hover .ytdl-thumb-status { opacity: 0; }
    .ytdl-thumb-status-icon { position: relative; width: 34px; height: 34px; display: flex; align-items: center; justify-content: center; color: #000; font-size: 16px; background: rgba(255, 255, 255, 0.35); border-radius: 6px; text-shadow: 0 0 1px rgba(255, 255, 255, 0.9); }
    .ytdl-thumb-status-icon .ytdl-status-emoji { display: inline-flex; align-items: center; justify-content: center; filter: grayscale(1) brightness(0); }
    .ytdl-thumb-status-icon .ytdl-status-check { position: absolute; right: 1px; bottom: -2px; color: #37d94f; font-size: 18px; font-weight: 700; text-shadow: 0 0 2px rgba(0, 0, 0, 1), 0 0 4px rgba(0, 0, 0, 1); }

    /* Smaller quick-download UI in watch-page sidebar playlists */
    ytd-watch-flexy #secondary ytd-compact-video-renderer .ytdl-thumb-overlay,
    ytd-watch-flexy #secondary ytd-playlist-panel-video-renderer .ytdl-thumb-overlay { top: 4px; left: 4px; gap: 4px; }
    ytd-watch-flexy #secondary ytd-compact-video-renderer .ytdl-thumb-status,
    ytd-watch-flexy #secondary ytd-playlist-panel-video-renderer .ytdl-thumb-status { top: 4px; left: 4px; gap: 4px; }
    ytd-watch-flexy #secondary ytd-compact-video-renderer .ytdl-thumb-btn,
    ytd-watch-flexy #secondary ytd-playlist-panel-video-renderer .ytdl-thumb-btn { width: 22px; height: 22px; font-size: 12px; border-radius: 5px; }
    ytd-watch-flexy #secondary ytd-compact-video-renderer .ytdl-thumb-status-icon,
    ytd-watch-flexy #secondary ytd-playlist-panel-video-renderer .ytdl-thumb-status-icon { width: 22px; height: 22px; font-size: 13px; border-radius: 5px; }
    ytd-watch-flexy #secondary ytd-compact-video-renderer .ytdl-thumb-status-icon .ytdl-status-check,
    ytd-watch-flexy #secondary ytd-playlist-panel-video-renderer .ytdl-thumb-status-icon .ytdl-status-check { font-size: 15px; right: 0; bottom: -1px; }

    /* Download center */
    #ytdl-download-center { position: fixed; top: 80px; right: 20px; width: 360px; max-height: 60vh; overflow-y: auto; z-index: 10001; display: flex; flex-direction: column; gap: 8px; }
    .ytdl-download-item { background: #0f0f0f; color: #fff; padding: 10px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5); font-family: Roboto, sans-serif; font-size: 13px; }
    .ytdl-download-title { font-weight: 600; margin-bottom: 6px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .ytdl-download-msg { font-size: 12px; color: var(--yt-spec-text-secondary); margin-bottom: 6px; }
    .ytdl-download-bar-bg { background: #222; height: 10px; border-radius: 6px; overflow: hidden; }
    .ytdl-download-bar { background: #1db954; height: 100%; width: 0%; transition: width 0.3s ease; }
    .ytdl-download-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px; }
    .ytdl-download-actions button { background: transparent; color: inherit; border: 1px solid rgba(255, 255, 255, 0.08); padding: 4px 8px; border-radius: 4px; cursor: pointer; }
  `;

  GM_addStyle(STYLES);

  // --- Core Functions ---

  /**
   * Show a toast notification
   * @param {string} message - The message to display
   * @param {string} type - The type of toast: 'info', 'success', 'error', 'warning'
   * @param {number} duration - How long to show the toast in milliseconds (default: 5000)
   */
  function showToast(message, type = 'info', duration = 5000) {
    const colors = {
      info: '#fff',
      success: '#0f0',
      error: '#f00',
      warning: '#ff0'
    };

    const toast = document.createElement('div');
    toast.style.cssText = `position: fixed; top: 80px; right: 20px; background: #0f0f0f; color: ${colors[type] || colors.info}; padding: 12px 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); z-index: 10001; font-family: "Roboto", sans-serif; font-size: 14px; max-width: 400px;`;
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), duration);
  }

  /**
   * Loads persisted per-video quick-download status from localStorage.
   * @returns {Record<string, {video?: boolean, audio?: boolean, updatedAt?: number}>}
   */
  function loadQuickDownloadStatusMap() {
    try {
      const raw = localStorage.getItem(QUICK_DOWNLOAD_STATUS_STORAGE_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      return (parsed && typeof parsed === 'object') ? parsed : {};
    } catch (e) {
      console.error('Failed to load quick download status map:', e);
      return {};
    }
  }

  /**
   * Persists per-video quick-download status to localStorage.
    * @param {Record<string, {video?: boolean, audio?: boolean, updatedAt?: number}>} map - Status map keyed by YouTube video ID.
   */
  function saveQuickDownloadStatusMap(map) {
    try {
      localStorage.setItem(QUICK_DOWNLOAD_STATUS_STORAGE_KEY, JSON.stringify(map || {}));
    } catch (e) {
      console.error('Failed to save quick download status map:', e);
    }
  }

  /**
   * Extracts a YouTube video ID from supported YouTube URL formats.
    * @param {string} url - Candidate YouTube URL to parse.
   * @returns {string|null}
   */
  function extractVideoIdFromAnyUrl(url) {
    try {
      const u = new URL(url, window.location.origin);
      const host = (u.hostname || '').toLowerCase();
      if (host.endsWith('youtube.com') || host.endsWith('youtube-nocookie.com')) {
        const qv = u.searchParams.get('v');
        if (qv) return qv;
        if (u.pathname.startsWith('/embed/')) {
          const embedId = u.pathname.split('/embed/')[1]?.split('/')[0]?.trim();
          return embedId || null;
        }
        return null;
      }
      if (host === 'youtu.be') {
        const id = u.pathname.replace(/^\/+/, '').split('/')[0]?.trim();
        return id || null;
      }
      return null;
    } catch (e) {
      return null;
    }
  }

  /**
   * Returns persisted quick-download status for a video.
    * @param {string|null} videoId - YouTube video ID to lookup.
   * @returns {{video: boolean, audio: boolean}}
   */
  function getQuickDownloadStatusForVideo(videoId) {
    if (!videoId) return { video: false, audio: false };
    const map = loadQuickDownloadStatusMap();
    const row = map[videoId];
    return {
      video: !!(row && row.video),
      audio: !!(row && row.audio)
    };
  }

  /**
   * Marks a quick-download type as completed for a video and refreshes overlays.
    * @param {string|null} videoId - YouTube video ID to update.
    * @param {'video'|'audio'} kind - Download type flag to mark as completed.
   */
  function markQuickDownloadCompleted(videoId, kind) {
    if (!videoId || !kind) return;
    const map = loadQuickDownloadStatusMap();
    const row = map[videoId] && typeof map[videoId] === 'object' ? map[videoId] : {};
    row[kind] = true;
    row.updatedAt = Date.now();
    map[videoId] = row;
    saveQuickDownloadStatusMap(map);
    updateStatusOverlaysForVideo(videoId);
  }

  /**
   * Renders the thumbnail status overlay based on saved download state.
    * @param {HTMLElement} statusOverlay - Overlay element to render into.
    * @param {{video?: boolean, audio?: boolean}} status - Saved status object for the current video.
   */
  function renderStatusOverlay(statusOverlay, status) {
    if (!statusOverlay) return;
    while (statusOverlay.firstChild) {
      statusOverlay.removeChild(statusOverlay.firstChild);
    }
    const hasVideo = !!status?.video;
    const hasAudio = !!status?.audio;
    if (!hasVideo && !hasAudio) {
      statusOverlay.style.display = 'none';
      return;
    }

    /**
     * Adds one status icon row (video/audio) with a check indicator.
      * @param {string} emoji - Emoji glyph to display (video or audio icon).
     */
    const addStatusIcon = (emoji) => {
      const icon = document.createElement('div');
      icon.className = 'ytdl-thumb-status-icon';
      const emojiSpan = document.createElement('span');
      emojiSpan.className = 'ytdl-status-emoji';
      emojiSpan.textContent = emoji;
      icon.appendChild(emojiSpan);
      const check = document.createElement('span');
      check.className = 'ytdl-status-check';
      check.textContent = '✓';
      icon.appendChild(check);
      statusOverlay.appendChild(icon);
    };

    if (hasVideo) addStatusIcon('🎬');
    if (hasAudio) addStatusIcon('🎧');
    statusOverlay.style.display = 'flex';
  }

  /**
   * Updates all visible thumbnail overlays tied to a specific video ID.
    * @param {string|null} videoId - YouTube video ID whose overlays should be refreshed.
   */
  function updateStatusOverlaysForVideo(videoId) {
    if (!videoId) return;
    const status = getQuickDownloadStatusForVideo(videoId);
    const selector = `.ytdl-thumb-anchor[data-ytdl-video-id="${videoId}"] .ytdl-thumb-status`;
    const overlays = document.querySelectorAll(selector);
    overlays.forEach(overlay => renderStatusOverlay(overlay, status));
  }

  // Generate simple UUIDv4 for client_id
  /**
   * Generates a UUID v4-like identifier for local download UI tracking.
   * @returns {string}
   */
  function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  // Active UI maps keyed by client_id for in-progress download rows.
  const activeToasts = {}; // client_id -> toast row DOM handles
  const activeDownloads = {}; // client_id -> request handle (supports abort)

  /**
   * Creates a persistent download center item for a download request.
    * @param {string} clientId - Local tracking ID for this download request.
    * @param {string} title - Display title shown in the download center row.
   * @returns {{item: HTMLElement, titleEl: HTMLElement, msgEl: HTMLElement, bar: HTMLElement}}
   */
  function createPersistentDownloadToast(clientId, title) {
    // Ensure download center exists
    let center = document.getElementById('ytdl-download-center');
    if (!center) {
      center = document.createElement('div');
      center.id = 'ytdl-download-center';
      document.body.appendChild(center);
    }

    // Create item
    const item = document.createElement('div');
    item.className = 'ytdl-download-item';
    item.id = `ytdl-download-${clientId}`;

    const titleEl = document.createElement('div');
    titleEl.className = 'ytdl-download-title';
    titleEl.textContent = title || 'Downloading...';
    item.appendChild(titleEl);

    const msgEl = document.createElement('div');
    msgEl.className = 'ytdl-download-msg';
    msgEl.textContent = 'Starting...';
    item.appendChild(msgEl);

    const barBg = document.createElement('div');
    barBg.className = 'ytdl-download-bar-bg';
    const bar = document.createElement('div');
    bar.className = 'ytdl-download-bar';
    bar.style.width = '0%';
    barBg.appendChild(bar);
    item.appendChild(barBg);

    const actions = document.createElement('div');
    actions.className = 'ytdl-download-actions';
    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', () => {
      cancelBtn.disabled = true;
      updatePersistentToast(clientId, 0, 'Cancelling...', 'cancelling');
      try {
        const ctrl = activeDownloads[clientId];
        if (ctrl && typeof ctrl.abort === 'function') ctrl.abort();
      } catch (e) { console.error('Error aborting download:', e); }
    });
    const closeBtn = document.createElement('button');
    closeBtn.textContent = 'Close';
    closeBtn.addEventListener('click', () => {
      try { const ctrl = activeDownloads[clientId]; if (ctrl && typeof ctrl.abort === 'function') ctrl.abort(); } catch (e) { console.error(e); }
      item.remove();
      delete activeToasts[clientId];
    });
    actions.appendChild(cancelBtn);
    actions.appendChild(closeBtn);
    item.appendChild(actions);

    center.appendChild(item);
    activeToasts[clientId] = {item, titleEl, msgEl, bar};
    return activeToasts[clientId];
  }

  /**
   * Updates progress and message state for a persistent download item.
    * @param {string} clientId - Local tracking ID for the download row.
    * @param {number} percent - Progress percentage to reflect in the UI.
    * @param {string} message - Status message to display to the user.
    * @param {'running'|'complete'|'error'|'cancelling'} status - Lifecycle state for styling and cleanup behavior.
   */
  function updatePersistentToast(clientId, percent, message, status) {
    const handle = activeToasts[clientId];
    if (!handle) return;
    handle.msgEl.textContent = message || (status === 'complete' ? 'Complete' : '');
    try { handle.bar.style.width = (percent || 0) + '%'; } catch (e) {}
    if (status === 'complete') {
      handle.msgEl.textContent = 'Complete';
      handle.bar.style.width = '100%';
      setTimeout(() => {
        if (handle && handle.item) handle.item.remove();
        delete activeToasts[clientId];
      }, 2500);
    } else if (status === 'error') {
      handle.msgEl.textContent = `Error: ${message || 'Failed'}`;
      try { handle.bar.style.background = '#ff4d4f'; } catch (e) {}
    }
  }

  /**
   * Computes the greatest common divisor.
    * @param {number} a - First integer value.
    * @param {number} b - Second integer value.
   * @returns {number}
   */
  function gcd(a, b) {
    return b === 0 ? a : gcd(b, a % b);
  }
  /**
   * Reads the current watch-page video ID from location query params.
   * @returns {string|null}
   */
  function getVideoIdFromUrl() {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('v');
  }

  /**
   * Checks whether the current location is an individual watch page.
   * @returns {boolean}
   */
  function isWatchPageUrl() {
    try {
      return window.location.pathname === '/watch' && !!getVideoIdFromUrl();
    } catch (e) {
      return false;
    }
  }

  /**
   * Validates whether a URL points to an individual YouTube video.
    * @param {string} url - URL to validate.
   * @returns {boolean}
   */
  function isVideoUrlJS(url) {
    try {
      const u = new URL(url, window.location.origin);
      const host = (u.hostname || '').toLowerCase();
      if (host.endsWith('youtube.com') || host.endsWith('youtube-nocookie.com')) {
        if (u.searchParams.get('v')) return true;
        if (u.pathname.startsWith('/embed/')) return true;
        return false;
      }
      if (host === 'youtu.be') {
        const id = u.pathname.replace(/^\/+/, '');
        return !!id;
      }
      return false;
    } catch (e) {
      return false;
    }
  }

  /**
   * Replaces button text with a temporary loading label.
    * @param {HTMLButtonElement} button - Button element to update.
    * @param {string} defaultText - Base label shown before the spinner suffix.
   */
  function showSpinner(button, defaultText = 'Downloading... ') {
    button.textContent = defaultText + '⏳'; // Simple loading indicator
    button.disabled = true;
  }

  /**
   * Restores button text and enabled state after loading.
    * @param {HTMLButtonElement} button - Button element to reset.
    * @param {string} defaultText - Label to restore.
   */
  function hideSpinner(button, defaultText = 'Download') {
    button.textContent = defaultText;
    button.disabled = false;
  }

  /**
   * Displays a loading indicator inside the format dropdown.
    * @param {HTMLElement} menu - Dropdown container element.
   */
  function showDropdownSpinner(menu) {
    // Clear existing content first
    while (menu.firstChild) {
      menu.removeChild(menu.firstChild);
    }
    const loadingDiv = document.createElement('div');
    loadingDiv.className = 'ytdl-loading-indicator';
    loadingDiv.textContent = 'Loading formats...';
    menu.appendChild(loadingDiv);
    menu.classList.add('show'); // Ensure it's visible
  }

  /**
   * Displays an error message inside the format dropdown.
    * @param {HTMLElement} menu - Dropdown container element.
    * @param {string} message - Error text to render.
   */
  function showDropdownError(menu, message) {
    // Clear existing content first
    while (menu.firstChild) {
      menu.removeChild(menu.firstChild);
    }
    const errorDiv = document.createElement('div');
    errorDiv.className = 'ytdl-error-message';
    errorDiv.textContent = `Error: ${message}`;
    menu.appendChild(errorDiv);
    menu.classList.add('show'); // Ensure it's visible
  }

  /**
   * Starts a download request and streams progress into the download center UI.
    * @param {string} url - Source video URL passed to the backend.
    * @param {string|null} audioId - Explicit audio format ID to request, if any.
    * @param {string|null} videoId - Explicit video format ID to request, if any.
    * @param {string|null} targetFormat - Requested conversion/output format (for example `mp3`).
    * @param {string|number|null} targetAudioParams - Optional audio conversion argument (for example bitrate).
    * @param {string|number|null} targetVideoParams - Optional video conversion argument.
    * @param {string} filenameHint - Preferred fallback filename stem.
    * @param {{videoId?: string|null, kind?: 'video'|'audio'}|null} quickTrack - Optional tracking payload for persisted status updates.
   */
  function triggerDownload(url, audioId = null, videoId = null, targetFormat = null, targetAudioParams = null, targetVideoParams = null, filenameHint = 'download', quickTrack = null) {
    console.log(`Requesting download: URL=${url}, AudioID=${audioId}, VideoID=${videoId}, Target=${targetFormat}`);

    // Generate a client_id for local UI tracking and show persistent toast
    const clientId = uuidv4();
    const toastHandle = createPersistentDownloadToast(clientId, filenameHint);

    const params = new URLSearchParams();
    params.append('url', url);
    // client_id is local-only; do not send to server
    if (audioId) params.append('audio_format_id', audioId);
    if (videoId) params.append('video_format_id', videoId);
    if (targetFormat) params.append('target_format', targetFormat);
    if (targetAudioParams) params.append('target_audio_params', targetAudioParams);
    if (targetVideoParams) params.append('target_video_params', targetVideoParams);

    // Perform the download directly and stream the response to provide local progress
    const requestUrl = `${FLASK_SERVICE_BASE_URL}/download?${params.toString()}`;
    updatePersistentToast(clientId, 0, 'Starting download...', 'running');
    let contentDisposition = '';

    // Use GM_xmlhttpRequest to avoid extensions blocking page fetch()
    const gmReq = GM_xmlhttpRequest({
      method: 'GET',
      url: requestUrl,
      responseType: 'blob',
      onprogress: function (e) {
        if (e.lengthComputable) {
          const pct = Math.min(99, Math.floor((e.loaded / e.total) * 100));
          updatePersistentToast(clientId, pct, `Downloading... ${pct}%`, 'running');
        } else {
          updatePersistentToast(clientId, 50, `Downloading...`, 'running');
        }
      },
      onload: function (res) {
        try {
          if (res.status >= 200 && res.status < 300) {
            const blob = res.response;
            contentDisposition = res.responseHeaders || '';
            let filename = filenameHint + '.' + (targetFormat || (videoId ? 'mp4' : 'mp3'));
            try {
              const m = /filename\*?=(?:UTF-8'')?([^;\n]*)/i.exec(contentDisposition);
              if (m && m[1]) {
                const raw = m[1].trim().replace(/['"]/g, '');
                try { filename = decodeURIComponent(raw); } catch (e) { filename = raw; }
              }
            } catch (e) {}
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(link.href);
            updatePersistentToast(clientId, 100, `Downloaded: ${filename}`, 'complete');
            // Persist status only after successful response handling.
            if (quickTrack && quickTrack.videoId && (quickTrack.kind === 'video' || quickTrack.kind === 'audio')) {
              markQuickDownloadCompleted(quickTrack.videoId, quickTrack.kind);
            }
          } else {
            updatePersistentToast(clientId, 0, `Server error: ${res.status}`, 'error');
            showToast(`✗ Download failed: ${res.statusText || res.status}`, 'error', 8000);
          }
        } catch (err) {
          updatePersistentToast(clientId, 0, `Error: ${err.message}`, 'error');
          showToast(`✗ Download failed: ${err.message}`, 'error', 8000);
          console.error('Download onload processing error:', err);
        } finally {
          try { delete activeDownloads[clientId]; } catch (e) {}
        }
      },
      onerror: function (err) {
        updatePersistentToast(clientId, 0, `Error: Request failed`, 'error');
        showToast(`✗ Download failed: network error`, 'error', 8000);
        console.error('GM_xmlhttpRequest error:', err);
        try { delete activeDownloads[clientId]; } catch (e) {}
      },
      ontimeout: function () {
        updatePersistentToast(clientId, 0, `Error: Request timed out`, 'error');
        showToast(`✗ Download timed out`, 'error', 8000);
        try { delete activeDownloads[clientId]; } catch (e) {}
      }
    });

    // Store the GM request so cancel/close buttons can abort it
    activeDownloads[clientId] = gmReq;
  }


  /**
   * Fetches available formats for a video and optionally updates an open dropdown.
    * @param {string} url - Source video URL.
    * @param {HTMLElement} [dropdownMenu] - Optional dropdown element to immediately update with results/errors.
   */
  function fetchFormats(url, dropdownMenu) {
    // Don't fetch if already fetching or if data is cached
    if (isFetchingFormats || formatDataCache) {
      console.log("Skipping format fetch (already fetching or cached).");
      return;
    }

    // Validate URL is a single video URL
    if (!isVideoUrlJS(url)) {
      console.warn('fetchFormats called with non-video URL:', url);
      if (dropdownMenu) showDropdownError(dropdownMenu, 'Only individual YouTube video URLs are supported.');
      formatFetchError = 'Only individual YouTube video URLs are supported.';
      return;
    }

    // Validate URL is a single video URL
    if (!isVideoUrlJS(url)) {
      console.warn('fetchFormats called with non-video URL:', url);
      if (dropdownMenu) showDropdownError(dropdownMenu, 'Only individual YouTube video URLs are supported.');
      formatFetchError = 'Only individual YouTube video URLs are supported.';
      return;
    }

    console.log("Fetching formats proactively for:", url);
    isFetchingFormats = true;
    formatFetchError = null; // Clear previous error

    const params = new URLSearchParams();
    params.append('url', url);
    const requestUrl = `${FLASK_SERVICE_BASE_URL}/list_formats?${params.toString()}`;

    GM_xmlhttpRequest({
      method: "GET",
      url: requestUrl,
      responseType: 'json',
      timeout: 30000, // 30 seconds timeout for fetching formats
      onload: function (response) {
        if (response.status >= 200 && response.status < 300) {
          formatDataCache = response.response; // Store in cache
          console.log("Formats received and cached:", formatDataCache);

          // Auto-update dropdown if it's currently showing
          const menu = document.getElementById('ytdl-dropdown-menu');
          if (menu && menu.classList.contains('show') && menu.parentNode === document.body) {
            console.log("Dropdown is open, auto-updating with formats...");
            populateDropdown(menu, formatDataCache);
          }
        } else {
          formatFetchError = response.response?.error || `Server responded with status ${response.status}`;
          console.error("Error fetching formats:", response.status, response.response);

          // Auto-update dropdown with error if it's currently showing
          const menu = document.getElementById('ytdl-dropdown-menu');
          if (menu && menu.classList.contains('show') && menu.parentNode === document.body) {
            console.log("Dropdown is open, showing error...");
            showDropdownError(menu, formatFetchError);
          }
        }
        isFetchingFormats = false; // Mark fetching as complete
      },
      onerror: function (response) {
        formatFetchError = `Could not connect to service at ${FLASK_SERVICE_BASE_URL}. Is it running?`;
        console.error("GM_xmlhttpRequest error fetching formats:", response);

        // Auto-update dropdown with error if it's currently showing
        const menu = document.getElementById('ytdl-dropdown-menu');
        if (menu && menu.classList.contains('show') && menu.parentNode === document.body) {
          console.log("Dropdown is open, showing error...");
          showDropdownError(menu, formatFetchError);
        }
        isFetchingFormats = false;
      },
      ontimeout: function () {
        formatFetchError = "Request timed out fetching formats.";
        console.error("Format fetch request timed out.");

        // Auto-update dropdown with error if it's currently showing
        const menu = document.getElementById('ytdl-dropdown-menu');
        if (menu && menu.classList.contains('show') && menu.parentNode === document.body) {
          console.log("Dropdown is open, showing timeout error...");
          showDropdownError(menu, formatFetchError);
        }
        isFetchingFormats = false;
      }
    });
  }

  /**
   * Fetches formats once and resolves with format data or null on failure.
    * @param {string} url - Source video URL.
   * @returns {Promise<object|null>}
   */
  function fetchFormatsOnce(url) {
    return new Promise((resolve) => {
      try {
        const params = new URLSearchParams();
        params.append('url', url);
        const requestUrl = `${FLASK_SERVICE_BASE_URL}/list_formats?${params.toString()}`;
        GM_xmlhttpRequest({
          method: 'GET',
          url: requestUrl,
          responseType: 'json',
          timeout: 30000,
          onload: function (response) {
            if (response.status >= 200 && response.status < 300) {
              resolve(response.response);
            } else {
              console.error('fetchFormatsOnce: server error', response.status, response);
              resolve(null);
            }
          },
          onerror: function (err) {
            console.error('fetchFormatsOnce: request error', err);
            resolve(null);
          },
          ontimeout: function () {
            console.error('fetchFormatsOnce: request timed out');
            resolve(null);
          }
        });
      } catch (e) {
        console.error('fetchFormatsOnce internal error', e);
        resolve(null);
      }
    });
  }

  /**
   * Formats a bitrate value in kbps for UI labels.
    * @param {number|null|undefined} bitrate - Raw bitrate value in kbps.
   * @returns {string}
   */
  function formatBitrate(bitrate) {
    return bitrate ? `${Math.round(bitrate)}k` : '';
  }

  /**
   * Builds a compact display label for a video format.
    * @param {{width?: number, height?: number, fps?: number}} format - Format metadata with optional dimensions/FPS.
   * @returns {string}
   */
  function formatVideoDetails(format) {
    let ratioStr = '';
    let resStr = '';

    if (format.width && format.height) {
      // Calculate aspect ratio
      const commonDivisor = gcd(format.width, format.height);
      const ratioW = format.width / commonDivisor;
      const ratioH = format.height / commonDivisor;
      ratioStr = `[${ratioW}:${ratioH}]`;

      // Format resolution and FPS
      resStr = `${format.height}p`;
      if (format.fps) {
        resStr += `${format.fps}`;
      }
    } else if (format.height) {
      // Fallback if only height is known
      resStr = `${format.height}p${format.fps || ''}`;
      ratioStr = '[?:?]'; // Indicate unknown ratio
    }

    return `${ratioStr} ${resStr}`.trim(); // Combine ratio and resolution/fps
  }

  /**
   * Populates the download format dropdown with available choices.
    * @param {HTMLElement} menu - Dropdown element that receives option rows.
    * @param {any} data - Parsed backend format payload.
   */
  function populateDropdown(menu, data) {
    // 1. Clear previous items using standard DOM methods
    while (menu.firstChild) {
      menu.removeChild(menu.firstChild);
    }

    if (!data || typeof data !== 'object') {
      console.error("Invalid data passed to populateDropdown:", data);
      showDropdownError(menu, "Invalid format data received.");
      return;
    }

    const videoTitle = data.title || 'video';
    // Basic sanitization for filename hint
    const safeFilenameHint = videoTitle.replace(/[^a-zA-Z0-9\-_\.]/g, '_').substring(0, 50);
    const bestAudio = (data.audio_formats && data.audio_formats.length > 0) ? data.audio_formats[0] : null;
    const bestVideo = (data.video_formats && data.video_formats.length > 0) ? data.video_formats[0] : null; // Assuming sorted best first by yt-dlp

    // 2. Create and append new elements

    // --- Add header ---
    const header = document.createElement('div');
    header.className = 'ytdl-dropdown-header';
    header.textContent = 'Select Format';
    menu.appendChild(header);

    // --- Helper to create item ---
    /**
     * Creates a clickable dropdown option row.
      * @param {string} text - Human-readable option label.
      * @param {string|null} audioId - Audio format ID for this option, if any.
      * @param {string|null} videoId - Video format ID for this option, if any.
      * @param {string|null} targetFormat - Optional conversion target format.
      * @param {string|number|null} targetAudioParams - Optional audio conversion parameter.
      * @param {string|number|null} targetVideoParams - Optional video conversion parameter.
     * @returns {HTMLAnchorElement}
     */
    const createItem = (text, audioId, videoId, targetFormat = null, targetAudioParams = null, targetVideoParams = null) => {
      const item = document.createElement('a');
      item.href = '#';
      item.className = 'ytdl-dropdown-item';
      // Add a little space after the icon
      item.textContent = text.replace(/^(\S+)/, '$1 '); // Adds space after first non-space sequence (the icon)
      item.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        const pageVideoId = extractVideoIdFromAnyUrl(data.url);
        const trackKind = (videoId || (!audioId && !targetFormat)) ? 'video' : 'audio';
        triggerDownload(
          data.url,
          audioId,
          videoId,
          targetFormat,
          targetAudioParams,
          targetVideoParams,
          safeFilenameHint,
          { videoId: pageVideoId, kind: trackKind }
        );
        // Find the menu again by ID in case it was re-created
        const currentMenu = document.getElementById('ytdl-dropdown-menu');
        if (currentMenu && currentMenu.parentNode === document.body) {
          currentMenu.classList.remove('show');
          document.body.removeChild(currentMenu);
        }
      });
      return item;
    };

    // --- Populate with format options ---

    // 1. Best Video + Audio (Default) - Now with details
    if (bestVideo && bestAudio) {
      const videoDetails = formatVideoDetails(bestVideo);
      const text = `⭐🎬 ${videoDetails} (${bestVideo.vcodec}) + 🎧 (${formatBitrate(bestAudio.abr)} ${bestAudio.acodec})`;
      menu.appendChild(createItem(text, null, null)); // Still uses null, null to trigger default merge
    } else if (bestVideo) {
      // Fallback if only best video is found (e.g., no separate audio streams)
      const videoDetails = formatVideoDetails(bestVideo);
      let text = `⭐🎬 ${videoDetails} (${bestVideo.vcodec})`;
      if (bestVideo.acodec) {
        text += ` + 🎧 (${bestVideo.acodec})`; // If video includes audio
      } else {
        text += ' (Video Only)'; // Should be rare with default yt-dlp behavior
      }
      menu.appendChild(createItem(text, null, null)); // Still trigger default
    } else if (bestAudio) {
      // Fallback if only best audio is found (highly unlikely for default)
      const text = `⭐🎧 Best Audio (${formatBitrate(bestAudio.abr)}, ${bestAudio.acodec || '?'})`;
      menu.appendChild(createItem(text, null, null)); // Still trigger default (will likely just download audio)
    } else {
      // Absolute fallback if no formats found - unlikely
      menu.appendChild(createItem('🎬 Best Quality (Unavailable)', null, null));
    }

    // 2. Best Audio Only
    if (bestAudio) {
      const sourceBitrate = bestAudio.abr; // Get the source bitrate
      const sourceBitrateText = formatBitrate(sourceBitrate);
      const sourceCodec = bestAudio.acodec || '?';

      // --- Option: Best Audio (Original Format) ---
      const originalText = `⭐🎧 Best Audio (${sourceBitrateText}, ${sourceCodec})`;
      menu.appendChild(createItem(originalText, bestAudio.format_id, null));

      // --- Option: Best Audio -> MP3 (Source Bitrate) ---
      // Note: The backend needs to handle 'mp3' without a specific bitrate to mean 'best possible mp3' or 'source bitrate mp3'
      const mp3SourceText = `⭐🎧 Best Audio (${sourceBitrateText}, ${sourceCodec}) -> MP3 (Source)`;
      menu.appendChild(createItem(mp3SourceText, bestAudio.format_id, null, 'mp3')); // Pass 'mp3' as target

      // --- Options: Best Audio -> MP3 (Specific Lower Bitrates) ---
      const targetMp3Bitrates = [192, 128, 96]; // Desired lower bitrates

      targetMp3Bitrates.forEach(targetBr => {
        // Only add the option if the target bitrate is lower than the source bitrate
        if (sourceBitrate && targetBr < sourceBitrate) {
          // Construct the target format string (e.g., 'mp3:192')
          // IMPORTANT: Your Flask backend needs to be updated to parse this format string
          // (e.g., split by ':' to get format and bitrate)
          const mp3TargetText = `⭐🎧 Best Audio (${sourceBitrateText}, ${sourceCodec}) -> MP3 (${targetBr}k)`;
          menu.appendChild(createItem(mp3TargetText, bestAudio.format_id, null, "mp3", targetBr)); // Pass 'mp3' and target bitrate
        }
      });
    }

    // 4. Specific Video Resolutions with Best Audio (if available) or Video's own audio
    const commonResolutions = [
      2160, // 4K UHD (16:9)
      1920, // Common Portrait (e.g., 1080x1920), Square (1920x1920)
      1440, // QHD (16:9), Common Ultrawide (e.g., 3440x1440)
      1280, // Common Portrait (e.g., 720x1280)
      1200, // WUXGA (16:10), UXGA (4:3)
      1080, // Full HD (16:9), Common Ultrawide (e.g., 2560x1080), Square (1080x1080)
      1050, // WSXGA+ (16:10), SXGA+ (4:3)
      960,  // (4:3)
      900,  // WXGA+ (16:10)
      854,  // FWVGA (approx 16:9 portrait)
      800,  // WXGA (16:10)
      768,  // XGA (4:3)
      720,  // HD (16:9), Square (720x720)
      600,  // SVGA (4:3)
      480,  // SD (various ratios)
      360   // SD (various ratios)
    ];
    const addedFormats = new Set(); // Track added format_ids to avoid duplicates

    if (data.video_formats) {
      // Prioritize common resolutions first
      commonResolutions.forEach(resHeight => {
        // Define tolerance range (approx 10%)
        const minHeight = resHeight * 0.9;
        const maxHeight = resHeight * 1.1;

        // Find the best format within the tolerance range for this resolution
        // Prioritize mp4, then webm, then the first found within range.
        let chosenFormat = data.video_formats.find(f =>
            f.height >= minHeight && f.height <= maxHeight && // Check within tolerance
            f.ext === 'mp4' &&
            !addedFormats.has(f.format_id)
          )
          || data.video_formats.find(f =>
            f.height >= minHeight && f.height <= maxHeight && // Check within tolerance
            f.ext === 'webm' &&
            !addedFormats.has(f.format_id)
          )
          || data.video_formats.find(f =>
            f.height >= minHeight && f.height <= maxHeight && // Check within tolerance
            !addedFormats.has(f.format_id)
          );

        if (chosenFormat) {
          const videoDetails = formatVideoDetails(chosenFormat);
          let text = '';
          let audioId = null;
          let videoId = chosenFormat.format_id;
          let shouldAddItem = false; // Flag to control adding the item

          if (bestAudio) {
            // Offer combined format download with best separate audio
            text = `🎬 ${videoDetails} (${chosenFormat.vcodec}) + 🎧 (${formatBitrate(bestAudio.abr)} ${bestAudio.acodec})`;
            audioId = bestAudio.format_id;
            shouldAddItem = true;
          } else if (chosenFormat.acodec) {
            // If video format has audio itself (no separate best audio needed/available)
            text = `🎬 ${videoDetails} (${chosenFormat.vcodec}, ${chosenFormat.vcodec || '?'} + ${chosenFormat.acodec})`;
            // audioId remains null, videoId is set
            shouldAddItem = true;
          }
          // REMOVED: The 'else' block that previously created the "[V] Video Only" option is gone.
          // We only add items if they include audio (either separate or muxed).

          if (shouldAddItem) {
            menu.appendChild(createItem(text, audioId, videoId));
            addedFormats.add(chosenFormat.format_id);
          }
        }
      });
    }
  }


  /**
   * Creates the main watch-page download button group and dropdown behavior.
   * @returns {HTMLDivElement}
   */
  function createDownloadButton() {
    const container = document.createElement('div');
    container.className = 'ytdl-custom-button-container';
    container.id = 'ytdl-custom-button-container';

    const downloadButton = document.createElement('button');
    downloadButton.textContent = 'Download';
    downloadButton.className = 'ytdl-download-button';
    downloadButton.id = 'ytdl-download-button';
    downloadButton.title = 'Download best quality (default)';
    downloadButton.addEventListener('click', () => {
      const videoUrl = window.location.href;
      const videoTitleElement = document.querySelector('h1.ytd-watch-metadata'); // More robust selector
      const videoTitle = videoTitleElement?.textContent?.trim() || document.title.split(" - YouTube")[0] || 'youtube_video'; // Fallback title
      const safeFilenameHint = videoTitle.replace(/[^a-zA-Z0-9\-_\.]/g, '_').substring(0, 50);
      triggerDownload(
        videoUrl,
        null,
        null,
        null,
        null,
        null,
        safeFilenameHint,
        { videoId: extractVideoIdFromAnyUrl(videoUrl), kind: 'video' }
      ); // Default download
      // Ensure dropdown is hidden if open when main button is clicked
      const menu = document.getElementById('ytdl-dropdown-menu');
      if (menu && menu.classList.contains('show') && menu.parentNode === document.body) {
        menu.classList.remove('show');
        document.body.removeChild(menu); // Remove from body
      }
    });

    const dropdownArrow = document.createElement('button');
    dropdownArrow.textContent = '\u25BC'; // Down arrow ▼
    dropdownArrow.className = 'ytdl-dropdown-arrow';
    dropdownArrow.title = 'Show download options';

    // Create the dropdown menu but DON'T append it to the container yet
    const dropdownMenu = document.createElement('div');
    dropdownMenu.className = 'ytdl-dropdown-menu';
    dropdownMenu.id = 'ytdl-dropdown-menu';

    dropdownArrow.addEventListener('click', (e) => {
      e.stopPropagation(); // Prevent body click listener closing it immediately

      // Get the main container element (needed for right alignment)
      const buttonContainer = document.getElementById('ytdl-custom-button-container');
      if (!buttonContainer) {
          console.error("Could not find button container for positioning dropdown.");
          return; // Should not happen, but good practice
      }

      // Check if the menu is already shown (and attached to body)
      const isShown = dropdownMenu.classList.contains('show') && dropdownMenu.parentNode === document.body;

      if (isShown) {
        // Hide and remove from body
        dropdownMenu.classList.remove('show');
        document.body.removeChild(dropdownMenu);
      } else {
        // --- Calculate Position ---
        const arrowRect = dropdownArrow.getBoundingClientRect(); // Still needed for vertical positioning
        const containerRect = buttonContainer.getBoundingClientRect();
        const menuMaxHeight = 300; // Match CSS max-height
        const spaceBelow = window.innerHeight - arrowRect.bottom;
        const spaceAbove = arrowRect.top;
        const margin = 5; // Small margin

        // Reset position styles before calculation
        dropdownMenu.style.top = 'auto';
        dropdownMenu.style.bottom = 'auto';
        dropdownMenu.style.left = 'auto'; // Reset left
        dropdownMenu.style.right = 'auto'; // Reset right

        // --- Set Right Alignment ---
        // Align the menu's right edge with the container's right edge
        dropdownMenu.style.right = `${window.innerWidth - containerRect.right - 24}px`;
        console.log(`Dropdown right aligned to: ${window.innerWidth - containerRect.right - 24}px`);

        // Decide whether to place above or below
        if (spaceBelow >= menuMaxHeight || spaceBelow >= spaceAbove) {
          // Place below
          dropdownMenu.style.top = `${arrowRect.bottom + margin}px`;
          console.log("Dropdown positioned below arrow.");
        } else {
          // Place above
          dropdownMenu.style.bottom = `${window.innerHeight - arrowRect.top + margin}px`;
          console.log("Dropdown positioned above arrow.");
        }

        // --- Populate Content ---
        // (Clear previous content just in case it wasn't removed properly)
        while (dropdownMenu.firstChild) {
          dropdownMenu.removeChild(dropdownMenu.firstChild);
        }
        // Check cache status and populate (same logic as before)
        if (formatDataCache) {
          console.log("Populating dropdown from cache.");
          populateDropdown(dropdownMenu, formatDataCache);
        } else if (isFetchingFormats) {
          console.log("Formats are being fetched, showing spinner.");
          showDropdownSpinner(dropdownMenu); // Needs to work without assuming menu is visible
        } else if (formatFetchError) {
          console.log("Showing fetch error in dropdown.");
          showDropdownError(dropdownMenu, formatFetchError); // Needs to work standalone
        } else {
          // Fallback fetch
          console.warn("Formats not fetched yet and no error, attempting fetch now.");
          const videoUrl = window.location.href;
          showDropdownSpinner(dropdownMenu);
          fetchFormats(videoUrl);
        }

        // --- Append to body and show ---
        document.body.appendChild(dropdownMenu);
        // Use rAF to ensure styles are applied before adding 'show'
        requestAnimationFrame(() => {
          dropdownMenu.classList.add('show');
        });
      }
    });

    container.appendChild(downloadButton);
    container.appendChild(dropdownArrow);
    // Note: dropdownMenu is NOT appended to container

    // Close dropdown if clicking outside
    // Consider moving this listener setup outside createDownloadButton to avoid duplicates
    document.body.addEventListener('click', (e) => {
      const menu = document.getElementById('ytdl-dropdown-menu');
      // Check if menu exists in body, is shown, and click was outside button container AND menu
      if (menu && menu.parentNode === document.body && menu.classList.contains('show')) {
        if (!container.contains(e.target) && !menu.contains(e.target)) {
          menu.classList.remove('show');
          document.body.removeChild(menu);
        }
      }
    }, true); // Use capture phase

    return container;
  }

  /**
   * Inserts the custom download button into the active watch-page action row.
   * @returns {boolean}
   */
  function insertButton() {
    // Try a more specific and potentially stable selector for the button container row
    const actionsContainer = document.querySelector('#actions #actions-inner #menu ytd-menu-renderer');
    // Fallback selector if the first one fails (might be needed in some YT layouts)

    // Fallback selector remains the same
    const fallbackContainer = document.querySelector('#actions-inner');

    const targetContainer = actionsContainer || fallbackContainer; // Use the first one found

    const existingButton = document.getElementById('ytdl-custom-button-container');

    if (targetContainer && !existingButton) {
      const containerName = actionsContainer ? '#actions #actions-inner #menu ytd-menu-renderer' : '#actions-inner'; // Log correct name
      console.log(`Action container (${containerName}) found, inserting download button.`);
      const newButton = createDownloadButton();

      // Append the button container to the end of the target container's children
      targetContainer.appendChild(newButton);

      // No need to manually hide the original button here, CSS rule handles it.

      if (buttonContainerInterval) {
        clearInterval(buttonContainerInterval); // Stop polling
        console.log("Button inserted, polling stopped.");
      }

      // Trigger proactive format fetch
      const videoUrl = window.location.href;
      fetchFormats(videoUrl);

      return true; // Indicate success

    } else if (existingButton) {
      // Button already exists, ensure polling stops and fetch formats if needed
      // console.log("Custom download button already exists."); // Less verbose logging
      if (buttonContainerInterval) clearInterval(buttonContainerInterval);
      if (!isFetchingFormats && !formatDataCache && !formatFetchError) {
        const videoUrl = window.location.href;
        // console.log("Proactively fetching formats for existing button."); // Less verbose logging
        fetchFormats(videoUrl);
      }
      return true; // Indicate success (already exists)

    } else {
      // Container not found yet, continue polling or give up
      retryCount++;
      if (retryCount > MAX_RETRIES) {
        console.error(`Could not find button container (#menu-container #top-level-buttons-computed or #actions-inner) after ${MAX_RETRIES} retries. YouTube layout may have changed.`);
        if (buttonContainerInterval) clearInterval(buttonContainerInterval);
        return false; // Indicate failure
      }
      // console.log(`Button container not found, retrying (${retryCount}/${MAX_RETRIES})...`); // Less verbose logging
      return false; // Indicate not found yet
    }
  }

  /**
   * Converts a thumbnail href into an absolute, valid single-video URL.
    * @param {string|null} href - Raw href value from a thumbnail anchor.
   * @returns {string|null}
   */
  function parseVideoUrlFromHref(href) {
    try {
      if (!href) return null;
      const u = new URL(href, window.location.origin);
      if (isVideoUrlJS(u.href)) return u.href;
      return null;
    } catch (e) {
      return null;
    }
  }

  /**
   * Adds quick-download and status overlays to thumbnail anchors under a root node.
    * @param {Document|HTMLElement} root - Root node to scan for thumbnail anchors.
   */
  function addThumbnailIcons(root = document) {
    const anchors = root.querySelectorAll('a#thumbnail, ytd-thumbnail a.yt-simple-endpoint, a.ytp-videowall-still, a.yt-lockup-view-model__content-image[href*="/watch"], a[href*="/watch"][aria-hidden="true"][tabindex="-1"], a.yt-simple-endpoint[href*="/watch"][aria-hidden="true"], #related a.yt-lockup-view-model__content-image, ytd-watch-next-secondary-results-renderer a.yt-lockup-view-model__content-image, #related a[aria-hidden="true"][tabindex="-1"], ytd-watch-next-secondary-results-renderer a[aria-hidden="true"][tabindex="-1"]');
    anchors.forEach(a => {
      try {
        if (a.dataset.ytdlAttached) return;
        const hrefForId = a.getAttribute('href');
        if (!hrefForId) return;
        const parsedVideoUrl = parseVideoUrlFromHref(hrefForId);
        if (!parsedVideoUrl) return;
        const overlayHost = a.closest('ytd-rich-item-renderer, ytd-rich-grid-media, ytd-grid-video-renderer, ytd-compact-video-renderer, ytd-video-renderer, ytd-playlist-video-renderer, ytd-playlist-panel-video-renderer, yt-lockup-view-model') || a;
        if (overlayHost.dataset.ytdlAttached) {
          a.dataset.ytdlAttached = '1';
          return;
        }

        a.dataset.ytdlAttached = '1';
        overlayHost.dataset.ytdlAttached = '1';
        overlayHost.classList.add('ytdl-thumb-anchor');
        const parsedVideoId = parsedVideoUrl ? extractVideoIdFromAnyUrl(parsedVideoUrl) : null;
        if (parsedVideoId) {
          a.dataset.ytdlVideoId = parsedVideoId;
          overlayHost.dataset.ytdlVideoId = parsedVideoId;
        }

        const overlay = document.createElement('div');
        overlay.className = 'ytdl-thumb-overlay';

        const statusOverlay = document.createElement('div');
        statusOverlay.className = 'ytdl-thumb-status';

        /**
         * Resolves a best-effort video title from a thumbnail anchor context.
         * @param {HTMLAnchorElement} anchor - Thumbnail anchor used as the lookup context.
         * @returns {string}
         */
        const getTitleFromAnchor = (anchor) => {
          try {
            const isBad = (s) => {
              if (!s) return true;
              const t = s.trim();
              if (!t) return true;
              if (t.length < 3) return true;
              if (/^(true|false|null|undefined|\d+)$/i.test(t)) return true;
              return false;
            };

            // 1) Try anchor aria-label or title attribute first
            const aria = anchor.getAttribute('aria-label');
            if (!isBad(aria)) return aria.trim();
            if (!isBad(anchor.title)) return anchor.title.trim();

            // 2) Find the nearest renderer container that usually contains the title
            const renderer = anchor.closest('yt-lockup-view-model, ytd-watch-next-secondary-results-renderer, ytd-playlist-video-renderer, ytd-playlist-panel-video-renderer, ytd-rich-grid-media, ytd-rich-item-renderer, ytd-grid-video-renderer, ytd-compact-video-renderer, ytd-video-renderer');

            // 3) Look for common title link / formatted-string elements in renderer first
            const titleSelectors = [
              'a#video-title-link',
              'a#video-title',
              'a.yt-lockup-metadata-view-model__title',
              'h3.yt-lockup-metadata-view-model__heading-reset a.yt-lockup-metadata-view-model__title',
              'h3 a#video-title',
              'h3 a#video-title-link',
              'yt-formatted-string#video-title',
              'yt-formatted-string[id^="video-title"]',
              'yt-formatted-string.title',
              'span#video-title',
              'h3.title'
            ];

            const searchRoots = [anchor, renderer];
            for (const root of searchRoots) {
              if (!root) continue;
              for (const sel of titleSelectors) {
                try {
                  const el = root.querySelector(sel);
                  if (el) {
                    // Prefer title attribute where available (playlist/watch-later commonly uses this)
                    if (el.getAttribute && el.getAttribute('title') && !isBad(el.getAttribute('title'))) {
                      return el.getAttribute('title').trim();
                    }
                    // Prefer aria-label on title link if present
                    if (el.getAttribute && el.getAttribute('aria-label') && !isBad(el.getAttribute('aria-label'))) {
                      return el.getAttribute('aria-label').trim();
                    }
                    const txt = el.textContent?.trim();
                    if (!isBad(txt)) return txt;
                  }
                } catch (e) { /* ignore selector errors */ }
              }
            }

            // 3b) If not found yet, resolve by video id from known title links only.
            if (parsedVideoId) {
              const safeVideoId = parsedVideoId.replace(/"/g, '\\"');
              const byId = document.querySelector(
                `a#video-title[href*="v=${safeVideoId}"], a#video-title-link[href*="v=${safeVideoId}"], a.yt-lockup-metadata-view-model__title[href*="v=${safeVideoId}"]`
              );
              if (byId) {
                if (byId.getAttribute && byId.getAttribute('title') && !isBad(byId.getAttribute('title'))) {
                  return byId.getAttribute('title').trim();
                }
                if (byId.getAttribute && byId.getAttribute('aria-label') && !isBad(byId.getAttribute('aria-label'))) {
                  return byId.getAttribute('aria-label').trim();
                }
                const byIdText = byId.textContent?.trim();
                if (!isBad(byIdText)) return byIdText;
              }
            }

            // 4) try image alt inside anchor or renderer
            const img = anchor.querySelector('img') || renderer?.querySelector('img');
            if (img && !isBad(img.alt)) return img.alt.trim();

            // 5) fallback to document title
            const docT = document.title.split(' - YouTube')[0];
            return (!isBad(docT) ? docT : 'youtube_video');
          } catch (e) {
            return document.title.split(' - YouTube')[0] || 'youtube_video';
          }
        };

        /**
         * Creates a thumbnail quick-download button and binds click behavior.
         * @param {string} label - Emoji label shown in the button.
         * @param {string|null} targetFormat - Optional explicit target format to request.
         * @param {string} title - Tooltip/title attribute for the button.
         * @returns {HTMLButtonElement}
         */
        const makeBtn = (label, targetFormat, title) => {
          const btn = document.createElement('button');
          btn.className = 'ytdl-thumb-btn';
          btn.type = 'button';
          btn.title = title;
          // Use textContent to avoid Trusted Types / CSP TrustedHTML errors
          btn.textContent = label;

          // Detect audio button by title (contains 'audio') so we can resolve best audio format id
          if (title && title.toLowerCase().includes('audio')) {
            // For audio quick-download, try to resolve the best audio format id first
            btn.addEventListener('click', async (e) => {
              e.preventDefault();
              e.stopPropagation();
              const href = a.getAttribute('href');
              const videoUrl = parseVideoUrlFromHref(href) || (href ? (new URL(href, window.location.origin)).href : null);
              if (!videoUrl) {
                showToast('Could not determine video URL', 'error', 4000);
                return;
              }
              const videoTitle = getTitleFromAnchor(a) || document.title.split(' - YouTube')[0] || 'youtube_video';
              const safeFilenameHint = (videoTitle || 'youtube_video').replace(/[^a-zA-Z0-9\-_\.]/g, '_').substring(0, 50);
              try {
                const data = await fetchFormatsOnce(videoUrl);
                const bestAudioId = data?.audio_formats?.[0]?.format_id || null;
                if (bestAudioId) {
                  // Pass the audio_format_id but leave targetFormat null to let backend choose default
                  triggerDownload(
                    videoUrl,
                    bestAudioId,
                    null,
                    null,
                    null,
                    null,
                    safeFilenameHint,
                    { videoId: extractVideoIdFromAnyUrl(videoUrl), kind: 'audio' }
                  );
                } else {
                  // Fallback: request default handling (no explicit target)
                  triggerDownload(
                    videoUrl,
                    null,
                    null,
                    null,
                    null,
                    null,
                    safeFilenameHint,
                    { videoId: extractVideoIdFromAnyUrl(videoUrl), kind: 'audio' }
                  );
                }
              } catch (err) {
                console.error('Error resolving audio format for quick-download:', err);
                // Fallback behavior
                triggerDownload(
                  videoUrl,
                  null,
                  null,
                  null,
                  null,
                  null,
                  safeFilenameHint,
                  { videoId: extractVideoIdFromAnyUrl(videoUrl), kind: 'audio' }
                );
              }
            });
          } else {
            btn.addEventListener('click', (e) => {
              e.preventDefault();
              e.stopPropagation();
              const href = a.getAttribute('href');
              const videoUrl = parseVideoUrlFromHref(href) || (href ? (new URL(href, window.location.origin)).href : null);
              if (!videoUrl) {
                showToast('Could not determine video URL', 'error', 4000);
                return;
              }
              const videoTitle = getTitleFromAnchor(a) || document.title.split(' - YouTube')[0] || 'youtube_video';
              const safeFilenameHint = (videoTitle || 'youtube_video').replace(/[^a-zA-Z0-9\-_\.]/g, '_').substring(0, 50);
              // Use targetFormat as provided (will be null for quick buttons to let backend choose default)
              triggerDownload(
                videoUrl,
                null,
                null,
                targetFormat,
                null,
                null,
                safeFilenameHint,
                { videoId: extractVideoIdFromAnyUrl(videoUrl), kind: 'video' }
              );
            });
          }
          return btn;
        };

        // Pass null as targetFormat so backend defaults are used; audio button still resolves best audio id
        const videoBtn = makeBtn('🎬', null, 'Quick download (video)');
        const audioBtn = makeBtn('🎧', null, 'Quick download (audio)');

        overlay.appendChild(videoBtn);
        overlay.appendChild(audioBtn);
        renderStatusOverlay(statusOverlay, getQuickDownloadStatusForVideo(parsedVideoId));

        const hoverRoot = overlayHost.closest('yt-lockup-view-model, ytd-rich-item-renderer, ytd-rich-grid-media, ytd-grid-video-renderer, ytd-compact-video-renderer, ytd-video-renderer, ytd-playlist-video-renderer, ytd-playlist-panel-video-renderer') || overlayHost;
        let isPointerTracking = false;
        const isPointInsideRect = (x, y, rect) => x >= rect.left && x <= rect.right && y >= rect.top && y <= rect.bottom;
        const stopPointerTracking = () => {
          if (!isPointerTracking) return;
          isPointerTracking = false;
          document.removeEventListener('pointermove', onPointerMove, true);
          document.removeEventListener('scroll', onScrollOrResize, true);
          window.removeEventListener('resize', onScrollOrResize, true);
        };
        const onPointerMove = (ev) => {
          try {
            if (!document.documentElement.contains(hoverRoot)) {
              setOverlayActive(false);
              return;
            }
            const rect = hoverRoot.getBoundingClientRect();
            if (!isPointInsideRect(ev.clientX, ev.clientY, rect)) {
              setOverlayActive(false);
            }
          } catch (e) {
            setOverlayActive(false);
          }
        };
        const onScrollOrResize = () => {
          try {
            if (!document.documentElement.contains(hoverRoot)) {
              setOverlayActive(false);
              return;
            }
            if (!overlayHost.classList.contains('ytdl-thumb-active')) return;
            const rect = hoverRoot.getBoundingClientRect();
            const cx = rect.left + rect.width / 2;
            const cy = rect.top + rect.height / 2;
            if (!isPointInsideRect(cx, cy, rect)) {
              setOverlayActive(false);
            }
          } catch (e) {
            setOverlayActive(false);
          }
        };
        const startPointerTracking = () => {
          if (isPointerTracking) return;
          isPointerTracking = true;
          document.addEventListener('pointermove', onPointerMove, true);
          document.addEventListener('scroll', onScrollOrResize, true);
          window.addEventListener('resize', onScrollOrResize, true);
        };
        const setOverlayActive = (isActive) => {
          if (isActive) {
            overlayHost.classList.add('ytdl-thumb-active');
            startPointerTracking();
          } else {
            overlayHost.classList.remove('ytdl-thumb-active');
            stopPointerTracking();
          }
        };

        a.addEventListener('pointerenter', () => setOverlayActive(true));
        a.addEventListener('pointerleave', () => {});
        overlay.addEventListener('pointerenter', () => setOverlayActive(true));
        overlay.addEventListener('pointerleave', () => {});
        if (hoverRoot !== a) {
          hoverRoot.addEventListener('pointerenter', () => setOverlayActive(true));
          hoverRoot.addEventListener('pointerleave', () => {});
        }

        // Ensure host is positioned and place overlay relative to the visible thumbnail area.
        const parent = overlayHost;
        if (getComputedStyle(parent).position === 'static') {
          parent.style.position = 'relative';
        }
        if (parent !== a) {
          const parentRect = parent.getBoundingClientRect();
          const anchorRect = a.getBoundingClientRect();
          const top = Math.max(0, Math.round(anchorRect.top - parentRect.top + 6));
          const left = Math.max(0, Math.round(anchorRect.left - parentRect.left + 6));
          overlay.style.top = `${top}px`;
          overlay.style.left = `${left}px`;
          statusOverlay.style.top = `${top}px`;
          statusOverlay.style.left = `${left}px`;
        }
        parent.appendChild(statusOverlay);
        parent.appendChild(overlay);
      } catch (err) {
        console.error('Error attaching ytdl thumbnail icons:', err);
      }
    });
  }

  // Initial scan + observer to handle lazy-rendered and dynamically inserted thumbnails.
  try {
    addThumbnailIcons(document);
    const thumbObserver = new MutationObserver((mutations) => {
      let shouldRescan = false;
      for (const m of mutations) {
        if (m.type === 'childList' && m.addedNodes && m.addedNodes.length) {
          shouldRescan = true;
          break;
        }
        if (m.type === 'attributes' && m.attributeName === 'href') {
          shouldRescan = true;
          break;
        }
      }
      if (shouldRescan) addThumbnailIcons(document);
    });
    thumbObserver.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['href'] });
  } catch (e) {
    console.error('Thumbnail observer setup failed:', e);
  }

  /**
   * Handles YouTube SPA navigation by resetting button and cache state when video changes.
   */
  function handleUrlChange() {
    const newVideoId = getVideoIdFromUrl();
    const onWatchPage = isWatchPageUrl();

    // Always rescan thumbnails after SPA navigation updates.
    addThumbnailIcons(document);

    if (newVideoId !== currentVideoId) {
      console.log(`URL changed to video ID: ${newVideoId}. Resetting button and cache.`);
      currentVideoId = newVideoId;
      retryCount = 0; // Reset retry count for the new page

      // Remove existing button if present
      const existingButton = document.getElementById('ytdl-custom-button-container');
      if (existingButton) {
        existingButton.remove();
      }

      // --- START: Remove Menu from Body ---
      const existingMenu = document.getElementById('ytdl-dropdown-menu');
      if (existingMenu && existingMenu.parentNode === document.body) {
        console.log("Removing dropdown menu from body due to URL change.");
        document.body.removeChild(existingMenu);
      }
      // --- END: Remove Menu from Body ---

      // Clear cache and reset fetch state
      formatDataCache = null;
      isFetchingFormats = false;
      formatFetchError = null;
      console.log("Format cache and fetch state cleared.");

      // Restart the polling process for watch pages only.
      if (buttonContainerInterval) {
        clearInterval(buttonContainerInterval);
        buttonContainerInterval = null;
      }
      if (onWatchPage) {
        buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
        insertButton(); // Try immediately
      }
    } else {
      // Optional: Check if button exists even on same URL
      const existingButton = document.getElementById('ytdl-custom-button-container');
      const existingMenu = document.getElementById('ytdl-dropdown-menu'); // Check menu too
      if (!existingButton && onWatchPage) {
        console.log("Button missing on same video ID, restarting poll.");
        retryCount = 0;
        // Clean up menu if it somehow got left behind
        if (existingMenu && existingMenu.parentNode === document.body) {
          document.body.removeChild(existingMenu);
        }
        formatDataCache = null;
        isFetchingFormats = false;
        formatFetchError = null;
        if (buttonContainerInterval) clearInterval(buttonContainerInterval);
        buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
        insertButton();
      } else if (!onWatchPage) {
        if (buttonContainerInterval) {
          clearInterval(buttonContainerInterval);
          buttonContainerInterval = null;
        }
      }
    }
  }

  // --- Initialization ---

  console.log("YouTube Downloader Service UI script running.");

  // Boot for current page, then keep button state synced as YouTube navigates without reload.
  currentVideoId = getVideoIdFromUrl();
  if (isWatchPageUrl()) {
    buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
    insertButton(); // Try immediately on load
  }

  // Title mutations are a lightweight route-change signal in YouTube's SPA navigation model.
  const titleObserver = new MutationObserver(handleUrlChange);
  const titleElement = document.querySelector('head > title');
  if (titleElement) {
    titleObserver.observe(titleElement, { childList: true });
  } else {
    console.warn("Could not find <title> element to observe for navigation.");
  }

  // Additional YouTube SPA hooks to reliably handle Home/Subscriptions/Playlist updates.
  window.addEventListener('yt-navigate-finish', () => {
    setTimeout(() => addThumbnailIcons(document), 0);
    handleUrlChange();
  }, true);

  document.addEventListener('yt-page-data-updated', () => {
    setTimeout(() => addThumbnailIcons(document), 0);
  }, true);
})();
