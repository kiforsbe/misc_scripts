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

  // --- Styles ---
  GM_addStyle(`
        .ytdl-custom-button-container {
            display: flex;
            align-items: center;
            margin-left: 8px; /* Adjust spacing as needed */
            position: relative; /* For dropdown positioning */
            font-size: 1.4rem;
            color: var(--yt-spec-text-primary);
            background-color: var(--yt-spec-badge-chip-background);
            padding: 0;
            border: none;
            border-radius: 18px; /* Match YouTube style */
            cursor: pointer;
            height: 36px; /* Match YouTube button height */
            /* overflow: hidden; */ /* REMOVED THIS LINE */
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
            position: absolute;
            top: 100%; /* Position below the button */
            left: 0;
            background-color: var(--yt-spec-menu-background);
            border: 1px solid var(--yt-spec-10-percent-layer);
            border-radius: 4px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 9999; /* INCREASED z-index significantly */
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
    `);

  // --- Core Functions ---

  function getVideoIdFromUrl() {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('v');
  }

  function showSpinner(button) {
    button.textContent = '⏳'; // Simple loading indicator
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
    console.log("Fetching formats for:", url);
    showDropdownSpinner(dropdownMenu);

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
          const data = response.response;
          console.log("Formats received:", data);
          populateDropdown(dropdownMenu, data);
        } else {
          const errorMsg = response.response?.error || `Server responded with status ${response.status}`;
          showDropdownError(dropdownMenu, errorMsg);
          console.error("Error fetching formats:", response.status, response.response);
        }
      },
      onerror: function (response) {
        showDropdownError(dropdownMenu, `Could not connect to service at ${FLASK_SERVICE_BASE_URL}. Is it running?`);
        console.error("GM_xmlhttpRequest error fetching formats:", response);
      },
      ontimeout: function () {
        showDropdownError(dropdownMenu, "Request timed out fetching formats.");
        console.error("Format fetch request timed out.");
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

    // --- Raw Format Lists (Optional - keep commented if too verbose) ---
    /*
    // Add logic similar to above using createElement/appendChild if uncommented
    */

    // 3. Ensure the menu is visible *after* populating
    // This should already be handled if showDropdownSpinner was called,
    // but adding it here ensures visibility even if loading was instant.
    menu.classList.add('show');
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
    });

    const dropdownArrow = document.createElement('button');
    dropdownArrow.textContent = '\u25BC'; // Down arrow ▼
    dropdownArrow.className = 'ytdl-dropdown-arrow';
    dropdownArrow.title = 'Show download options';

    const dropdownMenu = document.createElement('div');
    dropdownMenu.className = 'ytdl-dropdown-menu';
    dropdownMenu.id = 'ytdl-dropdown-menu';

    dropdownArrow.addEventListener('click', (e) => {
      e.stopPropagation(); // Prevent triggering body click listener immediately
      if (dropdownMenu.classList.contains('show')) {
        dropdownMenu.classList.remove('show');
      } else {
        const videoUrl = window.location.href;
        showDropdownSpinner(dropdownMenu); // Make menu visible with loading indicator
        fetchFormats(videoUrl, dropdownMenu); // Fetch formats (will replace spinner content on success/error)
      }
    });

    container.appendChild(downloadButton);
    container.appendChild(dropdownArrow);
    container.appendChild(dropdownMenu); // Append menu to container

    // Close dropdown if clicking outside
    document.body.addEventListener('click', (e) => {
      if (!container.contains(e.target) && dropdownMenu.classList.contains('show')) {
        dropdownMenu.classList.remove('show');
      }
    }, true); // Use capture phase

    return container;
  }

  function insertButton() {
    // Try to find the container for buttons below the video title
    // This selector might change with YouTube updates
    const actionsContainer = document.querySelector('#actions #menu.ytd-watch-metadata'); // More specific selector
    const existingButton = document.getElementById('ytdl-custom-button-container');

    if (actionsContainer && !existingButton) {
      console.log("Action container found, inserting download button.");
      const newButton = createDownloadButton();

      // Find the original download button and hide it (or insert relative to it)
      const originalDownloadButton = actionsContainer.querySelector('ytd-download-button-renderer');
      if (originalDownloadButton) {
        console.log("Hiding original YT download button.");
        originalDownloadButton.style.display = 'none';
        // Insert our button before or after it, or just append to container

        //actionsContainer.insertBefore(newButton, originalDownloadButton.nextSibling); // Place after original
        actionsContainer.appendChild(newButton);
      } else {
        // Fallback: Append to the actions container if original not found
        console.log("Original YT download button not found, appending to actions container.");
        actionsContainer.appendChild(newButton);
      }


      if (buttonContainerInterval) {
        clearInterval(buttonContainerInterval); // Stop polling
        console.log("Button inserted, polling stopped.");
      }
      return true; // Indicate success
    } else if (existingButton) {
      console.log("Custom download button already exists.");
      if (buttonContainerInterval) clearInterval(buttonContainerInterval);
      return true; // Already exists, count as success
    }

    retryCount++;
    if (retryCount > MAX_RETRIES) {
      console.error("Could not find button container after multiple retries. YouTube layout may have changed.");
      if (buttonContainerInterval) clearInterval(buttonContainerInterval);
      return false; // Indicate failure
    }

    console.log(`Button container not found, retrying (${retryCount}/${MAX_RETRIES})...`);
    return false; // Indicate not found yet
  }

  function handleUrlChange() {
    const newVideoId = getVideoIdFromUrl();
    if (newVideoId !== currentVideoId) {
      console.log(`URL changed to video ID: ${newVideoId}. Resetting button.`);
      currentVideoId = newVideoId;
      retryCount = 0; // Reset retry count for the new page

      // Remove existing button if present
      const existingButton = document.getElementById('ytdl-custom-button-container');
      if (existingButton) {
        existingButton.remove();
      }

      // Restart the polling process
      if (buttonContainerInterval) clearInterval(buttonContainerInterval);
      buttonContainerInterval = setInterval(insertButton, BUTTON_POLL_INTERVAL);
      insertButton(); // Try immediately first
    }
  }

  // --- Initialization ---

  console.log("YouTube Downloader Service UI script running.");

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
    // Fallback: Less reliable, check URL periodically (might miss fast navigations)
    // setInterval(handleUrlChange, 2000);
  }

  // Alternative: Observe the player element for changes (might be more robust)
  let playerObserver = null;
  function observePlayer() {
    const playerElement = document.getElementById('movie_player');
    if (playerElement && !playerObserver) {
      console.log("Player element found, observing for changes.");
      playerObserver = new MutationObserver(handleUrlChange);
      // Observe attributes that change during navigation (like video data)
      // This is heuristic and might need adjustment
      playerObserver.observe(playerElement, { attributes: true, attributeFilter: ['class'] }); // Example: Observe class changes
    } else if (!playerElement) {
      // If player isn't ready yet, try again shortly
      setTimeout(observePlayer, 500);
    }
  }
  // observePlayer(); // Start observing the player - uncomment if title observation is unreliable


})();
