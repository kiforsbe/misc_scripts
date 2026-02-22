// ==UserScript==
// @name         YouTube Downloader Service UI
// @namespace    http://tampermonkey.net/
// @version      1.2
// @description  Adds a download button to YouTube pages to interact with a local youtube-video-downloader-flask-ws service.
// @author       Your Name Here
// @match        https://www.youtube.com/watch?v=*
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
  // --- End Configuration ---

  let buttonContainerInterval = null;
  let retryCount = 0;
  let currentVideoId = null;

  // --- State for Format Caching ---
  let formatDataCache = null;
  let isFetchingFormats = false;
  let formatFetchError = null;
  // --- End State ---

  // --- Styles ---
  GM_addStyle(`
        .ytdl-custom-button-container {
            display: flex;
            align-items: center;
            margin-left: 8px; /* Adjust spacing as needed */
            /* position: relative; */ /* <-- REMOVE THIS */
            font-size: 1.4rem;
            color: var(--yt-spec-text-primary);
            background-color: var(--yt-spec-badge-chip-background);
            padding: 0;
            border: none;
            border-radius: 18px; /* Match YouTube style */
            cursor: pointer;
            height: 36px; /* Match YouTube button height */
            /* overflow: hidden; */ /* REMOVED THIS LINE */
            width: fit-content; /* <--- ADD THIS LINE */
        }
        .ytdl-download-button {
            padding: 0 16px;
            height: 100%;
            display: flex;
            align-items: center;
            border: none;
            background-color: transparent;
            color: inherit;
            font-family: "Roboto","Arial",sans-serif;
            font-size: 1.4rem;
            font-weight: 500;
            cursor: pointer;
            border-right: 1px solid var(--yt-spec-10-percent-layer); /* Separator */
        }
        .ytdl-download-button:hover {
            background-color: var(--yt-spec-badge-chip-background-hover);
        }
        .ytdl-dropdown-arrow {
            padding: 0 8px;
            height: 100%;
            display: flex;
            align-items: center;
            border: none;
            background-color: transparent;
            color: inherit;
            font-size: 1.6rem;
            cursor: pointer;
        }
        .ytdl-dropdown-arrow:hover {
            background-color: var(--yt-spec-badge-chip-background-hover);
        }
        .ytdl-dropdown-menu {
            display: none; /* Hidden by default */
            /* position: absolute; */ /* <-- CHANGE */
            position: fixed;      /* <-- TO FIXED */
            /* Remove top/bottom/left - will be set dynamically */
            background-color: var(--yt-spec-menu-background);
            border: 1px solid var(--yt-spec-10-percent-layer);
            border-radius: 4px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 10000; /* Even higher z-index */
            min-width: 200px;
            max-height: 300px;
            overflow-y: auto; /* Keep this for scrolling within the menu */
            color: var(--yt-spec-text-primary);
        }
        .ytdl-dropdown-menu.show {
            display: block;
        }
        .ytdl-dropdown-item, .ytdl-dropdown-header {
            padding: 8px 12px;
            cursor: pointer;
            font-size: 1.3rem;
            white-space: nowrap;
            display: block; /* Make it block level */
            text-decoration: none; /* Remove underline if it's an anchor */
            color: inherit; /* Inherit text color */
        }
        .ytdl-dropdown-header {
          font-weight: bold;
          cursor: default;
          border-bottom: 1px solid var(--yt-spec-10-percent-layer);
          margin-bottom: 4px;
        }
        .ytdl-dropdown-item:hover {
            /* Use YouTube's standard hover background */
            background-color: var(--yt-spec-hover-background, rgba(0, 0, 0, 0.1)); /* Added fallback */
            /* Text color remains inherited (var(--yt-spec-text-primary)), ensuring contrast */
        }
        .ytdl-loading-indicator {
            padding: 10px;
            text-align: center;
            font-style: italic;
            color: var(--yt-spec-text-secondary);
        }
        .ytdl-error-message {
            padding: 10px;
            color: var(--yt-spec-error-message-color, red); /* Use YT variable or fallback */
            font-weight: bold;
        }
        /* Hide original YT download button */
        ytd-download-button-renderer {
            display: none !important;
        }

        #owner.ytd-watch-metadata {
            min-width: calc(25% - 6px) !important;
        }
    `);

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

  // Generate simple UUIDv4 for client_id
  function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  // Persistent download toasts and polling management
  const activeToasts = {}; // client_id -> toast element
  const activeDownloads = {}; // client_id -> AbortController for active fetches

  function createPersistentDownloadToast(clientId, title) {
    const id = `ytdl-toast-${clientId}`;
    // Remove existing if present
    const existing = document.getElementById(id);
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = id;
    container.className = 'ytdl-persistent-toast';
    container.style.cssText = 'position: fixed; top: 80px; right: 20px; background: #0f0f0f; color: #fff; padding: 10px 12px; border-radius:8px; z-index:10001; width: 320px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); font-family: Roboto, sans-serif;';

    const titleEl = document.createElement('div');
    titleEl.style.fontWeight = '600';
    titleEl.style.marginBottom = '6px';
    titleEl.textContent = title || 'Downloading...';
    container.appendChild(titleEl);

    const msgEl = document.createElement('div');
    msgEl.className = 'ytdl-toast-msg';
    msgEl.style.fontSize = '12px';
    msgEl.style.marginBottom = '8px';
    msgEl.textContent = 'Starting...';
    container.appendChild(msgEl);

    const barBg = document.createElement('div');
    barBg.style.cssText = 'background:#222; height:10px; border-radius:6px; overflow:hidden;';
    const bar = document.createElement('div');
    bar.className = 'ytdl-toast-bar';
    bar.style.cssText = 'background:#1db954; height:100%; width:0%; transition: width 0.3s ease;';
    barBg.appendChild(bar);
    container.appendChild(barBg);

    const actions = document.createElement('div');
    actions.style.cssText = 'margin-top:8px; display:flex; justify-content:flex-end; gap:8px;';
    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.style.cssText = 'background:transparent; color:inherit; border:1px solid rgba(255,255,255,0.08); padding:4px 8px; border-radius:4px; cursor:pointer;';
    cancelBtn.addEventListener('click', () => {
      // Abort the local fetch if active
      cancelBtn.disabled = true;
      updatePersistentToast(clientId, 0, 'Cancelling...', 'cancelling');
      try {
        const ctrl = activeDownloads[clientId];
        if (ctrl) {
          ctrl.abort();
        }
      } catch (e) {
        console.error('Error aborting download:', e);
      }
    });
    actions.appendChild(cancelBtn);
    const closeBtn = document.createElement('button');
    closeBtn.textContent = 'Close';
    closeBtn.style.cssText = 'background:transparent; color:inherit; border:1px solid rgba(255,255,255,0.08); padding:4px 8px; border-radius:4px; cursor:pointer;';
    closeBtn.addEventListener('click', () => {
      // Abort local download if active, then remove toast
      try {
        const ctrl = activeDownloads[clientId];
        if (ctrl) ctrl.abort();
      } catch (e) {
        console.error('Error aborting download on close:', e);
      }
      container.remove();
    });
    actions.appendChild(closeBtn);
    container.appendChild(actions);

    document.body.appendChild(container);
    activeToasts[clientId] = {container, titleEl, msgEl, bar};
    return activeToasts[clientId];
  }

  function updatePersistentToast(clientId, percent, message, status) {
    const handle = activeToasts[clientId];
    if (!handle) return;
    handle.msgEl.textContent = message || (status === 'complete' ? 'Complete' : '');
    handle.bar.style.width = (percent || 0) + '%';
    if (status === 'complete') {
      handle.msgEl.textContent = 'Complete';
      setTimeout(() => {
        if (handle && handle.container) handle.container.remove();
        delete activeToasts[clientId];
      }, 2500);
    } else if (status === 'error') {
      handle.msgEl.textContent = `Error: ${message || 'Failed'}`;
      handle.bar.style.background = '#ff4d4f';
    }
  }

  // Polling removed: downloads are handled synchronously via fetch.

  function gcd(a, b) {
    return b === 0 ? a : gcd(b, a % b);
  }
  function getVideoIdFromUrl() {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('v');
  }

  function showSpinner(button, defaultText = 'Downloading... ') {
    button.textContent = defaultText + '⏳'; // Simple loading indicator
    button.disabled = true;
  }

  function hideSpinner(button, defaultText = 'Download') {
    button.textContent = defaultText;
    button.disabled = false;
  }

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

  function triggerDownload(url, audioId = null, videoId = null, targetFormat = null, targetAudioParams = null, targetVideoParams = null, filenameHint = 'download') {
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


  function fetchFormats(url, dropdownMenu) {
    // Don't fetch if already fetching or if data is cached
    if (isFetchingFormats || formatDataCache) {
      console.log("Skipping format fetch (already fetching or cached).");
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

  function formatBitrate(bitrate) {
    return bitrate ? `${Math.round(bitrate)}k` : '';
  }

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
    const createItem = (text, audioId, videoId, targetFormat = null, targetAudioParams = null, targetVideoParams = null) => {
      const item = document.createElement('a');
      item.href = '#';
      item.className = 'ytdl-dropdown-item';
      // Add a little space after the icon
      item.textContent = text.replace(/^(\S+)/, '$1 '); // Adds space after first non-space sequence (the icon)
      item.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        triggerDownload(data.url, audioId, videoId, targetFormat, targetAudioParams, targetVideoParams, safeFilenameHint);
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
      triggerDownload(videoUrl, null, null, null, safeFilenameHint); // Default download
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

  function handleUrlChange() {
    const newVideoId = getVideoIdFromUrl();
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

      // Restart the polling process
      if (buttonContainerInterval) clearInterval(buttonContainerInterval);
      buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
      insertButton(); // Try immediately
    } else {
      // Optional: Check if button exists even on same URL
      const existingButton = document.getElementById('ytdl-custom-button-container');
      const existingMenu = document.getElementById('ytdl-dropdown-menu'); // Check menu too
      if (!existingButton && buttonContainerInterval) {
        console.log("Button missing on same video ID, restarting poll.");
        retryCount = 0;
        // Clean up menu if it somehow got left behind
        if (existingMenu && existingMenu.parentNode === document.body) {
          document.body.removeChild(existingMenu);
        }
        formatDataCache = null;
        isFetchingFormats = false;
        formatFetchError = null;
        buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
        insertButton();
      }
    }
  }

  // --- Initialization ---

  console.log("YouTube Downloader Service UI script running (v1.2).");

  // Initial setup
  currentVideoId = getVideoIdFromUrl();
  buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
  insertButton(); // Try immediately on load

  // YouTube uses dynamic loading (SPA), so we need to detect navigation
  // Using MutationObserver on the <title> element is a common way
  const titleObserver = new MutationObserver(handleUrlChange);
  const titleElement = document.querySelector('head > title');
  if (titleElement) {
    titleObserver.observe(titleElement, { childList: true });
  } else {
    console.warn("Could not find <title> element to observe for navigation.");
  }
})();
