// ==UserScript==
// @name         YouTube Downloader Service UI
// @namespace    http://tampermonkey.net/
// @version      1.1
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

  function triggerDownload(url, audioId = null, videoId = null, targetFormat = null, filenameHint = 'download') {
    console.log(`Requesting download: URL=${url}, AudioID=${audioId}, VideoID=${videoId}, Target=${targetFormat}`);

    const downloadButton = document.getElementById('ytdl-download-button');
    if (downloadButton) showSpinner(downloadButton);

    const params = new URLSearchParams();
    params.append('url', url);
    if (audioId) params.append('audio_format_id', audioId);
    if (videoId) params.append('video_format_id', videoId);
    if (targetFormat) params.append('target_format', targetFormat);

    const requestUrl = `${FLASK_SERVICE_BASE_URL}/download?${params.toString()}`;

    GM_xmlhttpRequest({
      method: "GET",
      url: requestUrl,
      responseType: 'blob', // Important for file download
      timeout: 3600000, // 1 hour timeout for potentially long downloads/conversions
      onload: function (response) {
        if (downloadButton) hideSpinner(downloadButton);

        if (response.status >= 200 && response.status < 300) {
          const blob = response.response;
          let filename = filenameHint;

          // Try to get filename from Content-Disposition header
          const disposition = response.responseHeaders.match(/Content-Disposition.*filename\*?=(?:UTF-8'')?([^;\n]*)/i);
          if (disposition && disposition[1]) {
            try {
              // Trim whitespace (including \r) and remove quotes before decoding
              const rawFilename = disposition[1].trim().replace(/['"]/g, '');
              filename = decodeURIComponent(rawFilename);
            } catch (e) {
              console.warn("Could not decode filename from header:", disposition[1], e);
              // Use a safe fallback if decoding fails
              filename = filenameHint + '.' + (targetFormat || (videoId ? 'mp4' : 'mp3'));
            }
          } else {
            // Basic fallback if header is missing
            filename = filenameHint + '.' + (targetFormat || (videoId ? 'mp4' : 'mp3'));
            console.warn("Content-Disposition header missing or invalid, using fallback filename:", filename);
          }


          // Create a link and simulate a click to download the blob
          const link = document.createElement('a');
          link.href = URL.createObjectURL(blob);
          link.download = filename;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
          URL.revokeObjectURL(link.href); // Clean up
          console.log("Download initiated for:", filename);
        } else {
          // Try to parse error from JSON response if possible
          response.blob.text().then(text => {
            try {
              const errorJson = JSON.parse(text);
              alert(`Download failed: ${errorJson.error || `Server responded with status ${response.status}`}`);
              console.error("Download error response:", errorJson);
            } catch (e) {
              alert(`Download failed: Server responded with status ${response.status}. Check Tampermonkey console and Flask service logs.`);
              console.error("Download failed. Status:", response.status, "Response Text:", text);
            }
          }).catch(e => {
            alert(`Download failed: Server responded with status ${response.status}. Check Tampermonkey console and Flask service logs.`);
            console.error("Download failed. Status:", response.status, "Could not read error response body:", e);
          });
        }
      },
      onerror: function (response) {
        if (downloadButton) hideSpinner(downloadButton);
        alert(`Error connecting to download service at ${FLASK_SERVICE_BASE_URL}. Is it running? \nDetails: ${response.error || 'Network error'}`);
        console.error("GM_xmlhttpRequest error:", response);
      },
      ontimeout: function () {
        if (downloadButton) hideSpinner(downloadButton);
        alert("The download request timed out. The server might be busy or the file is very large.");
        console.error("Download request timed out.");
      }
    });
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
        } else {
          formatFetchError = response.response?.error || `Server responded with status ${response.status}`;
          console.error("Error fetching formats:", response.status, response.response);
        }
        isFetchingFormats = false; // Mark fetching as complete
      },
      onerror: function (response) {
        formatFetchError = `Could not connect to service at ${FLASK_SERVICE_BASE_URL}. Is it running?`;
        console.error("GM_xmlhttpRequest error fetching formats:", response);
        isFetchingFormats = false;
      },
      ontimeout: function () {
        formatFetchError = "Request timed out fetching formats.";
        console.error("Format fetch request timed out.");
        isFetchingFormats = false;
      }
    });
  }

  function formatBitrate(bitrate) {
    return bitrate ? `${Math.round(bitrate)}k` : '';
  }

  function formatResolution(format) {
    let res = '';
    if (format.width && format.height) {
      res += `${format.height}p`;
      if (format.fps) {
        res += `${format.fps}`;
      }
    }
    return res;
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

    // 2. Create and append new elements

    // --- Add header ---
    const header = document.createElement('div');
    header.className = 'ytdl-dropdown-header';
    header.textContent = 'Select Format';
    menu.appendChild(header);

    // --- Helper to create item ---
    const createItem = (text, audioId, videoId, targetFormat = null) => {
      const item = document.createElement('a'); // Use anchor for better semantics
      item.href = '#'; // Prevent page jump
      item.className = 'ytdl-dropdown-item';
      item.textContent = text;
      item.addEventListener('click', (e) => {
        e.preventDefault(); // Prevent anchor jump
        e.stopPropagation(); // Prevent closing menu immediately if not intended
        triggerDownload(data.url, audioId, videoId, targetFormat, safeFilenameHint);
        menu.classList.remove('show'); // Hide menu after click
      });
      return item;
    };

    // --- Populate with format options ---

    // 1. Best Video + Audio (Default)
    menu.appendChild(createItem('Best Quality (Video+Audio)', null, null));

    // 2. Best Video Only
    if (data.video_formats && data.video_formats.length > 0) {
      const bestVideo = data.video_formats[0]; // Assuming sorted best first by yt-dlp
      const text = `Best Video Only (${formatResolution(bestVideo)}, ${bestVideo.ext}, ${bestVideo.vcodec || ''})`;
      menu.appendChild(createItem(text, null, bestVideo.format_id));
    }

    // 3. Best Audio Only
    if (data.audio_formats && data.audio_formats.length > 0) {
      const bestAudio = data.audio_formats[0]; // Assuming sorted best first
      const text = `Best Audio Only (${formatBitrate(bestAudio.abr)}, ${bestAudio.ext}, ${bestAudio.acodec || ''})`;
      menu.appendChild(createItem(text, bestAudio.format_id, null));

      // 4. Best Audio converted to MP3
      const textMp3 = `Best Audio (-> MP3) (${formatBitrate(bestAudio.abr)}, ${bestAudio.acodec || ''})`;
      menu.appendChild(createItem(textMp3, bestAudio.format_id, null, 'mp3'));
    }

    // 5. Specific Video Resolutions with Best Audio
    const commonResolutions = [1080, 720, 480, 360];
    const addedResolutions = new Set();
    const bestAudio = (data.audio_formats && data.audio_formats.length > 0) ? data.audio_formats[0] : null;

    if (data.video_formats) {
      data.video_formats.forEach(vf => {
        if (vf.height && commonResolutions.includes(vf.height) && !addedResolutions.has(vf.height)) {
          // Prefer mp4 if available for this resolution, otherwise take first found
          let chosenFormat = vf;
          const mp4Version = data.video_formats.find(f => f.height === vf.height && f.ext === 'mp4');
          if (mp4Version) {
            chosenFormat = mp4Version;
          }

          if (bestAudio) {
            // Offer combined format download
            const text = `${formatResolution(chosenFormat)} (${chosenFormat.ext}) + Best Audio (${formatBitrate(bestAudio.abr)} ${bestAudio.ext})`;
            menu.appendChild(createItem(text, bestAudio.format_id, chosenFormat.format_id));
            addedResolutions.add(vf.height);
          } else if (chosenFormat.acodec) {
            // If video format has audio itself (no separate best audio needed/available)
            const text = `${formatResolution(chosenFormat)} (${chosenFormat.ext}, ${chosenFormat.vcodec || ''}+${chosenFormat.acodec})`;
            menu.appendChild(createItem(text, null, chosenFormat.format_id));
            addedResolutions.add(vf.height);
          } else {
            // Offer video-only if no audio available at all
            const text = `${formatResolution(chosenFormat)} Video Only (${chosenFormat.ext}, ${chosenFormat.vcodec || ''})`;
            menu.appendChild(createItem(text, null, chosenFormat.format_id));
            addedResolutions.add(vf.height);
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

      // Check if the menu is already shown (and attached to body)
      const isShown = dropdownMenu.classList.contains('show') && dropdownMenu.parentNode === document.body;

      if (isShown) {
        // Hide and remove from body
        dropdownMenu.classList.remove('show');
        document.body.removeChild(dropdownMenu);
      } else {
        // --- Calculate Position ---
        const arrowRect = dropdownArrow.getBoundingClientRect(); // Position relative to the arrow
        const menuMaxHeight = 300; // Match CSS max-height
        const spaceBelow = window.innerHeight - arrowRect.bottom;
        const spaceAbove = arrowRect.top;
        const margin = 5; // Small margin

        // Reset position styles before calculation
        dropdownMenu.style.top = 'auto';
        dropdownMenu.style.bottom = 'auto';
        dropdownMenu.style.left = `${arrowRect.left}px`; // Align left with arrow

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
