// ==UserScript==
// @name         Plex Playlist Play Status
// @namespace    http://tampermonkey.net/
// @version      1.1
// @description  Show simple triangle indicators for items in Plex playlists
// @match        https://app.plex.tv/*
// @match        http://localhost:32400/web/*
// @match        http://127.0.0.1:32400/web/*
// @match        http://*.plex.direct:32400/web/*
// @match        https://*.plex.direct:32400/web/*
// @grant        none
// @run-at       document-start
// ==/UserScript==

(function () {
  "use strict";

  // Configuration
  const config = {
    unwatchedColor: "#FF9800", // Orange for unwatched
    watchedColor: "#333333", // Black for watched
    triangleSize: "20px",
    updateInterval: 2000,
    apiTimeout: 5000,
  };

  // Cache for API responses
  const apiCache = new Map();
  const cacheTimeout = 30000;

  // Extract Plex server info from current URL
  function getPlexServerInfo() {
    try {
      const url = new URL(window.location.href);
      const hash = url.hash;

      const serverMatch = hash.match(/\/server\/([^\/]+)/);
      const serverId = serverMatch ? serverMatch[1] : null;

      let baseURL;
      if (window.location.hostname === "app.plex.tv") {
        baseURL = `https://app.plex.tv/api/v2`;
      } else {
        baseURL = `${window.location.protocol}//${window.location.host}`;
      }

      const token = localStorage.getItem("myPlexAccessToken") || sessionStorage.getItem("myPlexAccessToken") || new URLSearchParams(window.location.search).get("X-Plex-Token");

      return { serverId, baseURL, token };
    } catch (error) {
      console.warn("Error extracting Plex server info:", error);
      return { serverId: null, baseURL: null, token: null };
    }
  }

  // Get metadata key from playlist item element
  function getMetadataKey(element) {
    try {
      const metadataLink = element.querySelector('[data-testid="metadataTitleLink"]') || element.querySelector('[href*="/metadata/"]') || element.querySelector('a[href*="key="]');

      if (metadataLink) {
        const href = metadataLink.getAttribute("href");

        const keyMatch = href.match(/key=([^&]+)/);
        if (keyMatch) {
          return decodeURIComponent(keyMatch[1]);
        }

        const metadataMatch = href.match(/\/metadata\/(\d+)/);
        if (metadataMatch) {
          return `/library/metadata/${metadataMatch[1]}`;
        }
      }

      const dataKey = element.querySelector("[data-key]");
      if (dataKey) {
        return dataKey.getAttribute("data-key");
      }

      return null;
    } catch (error) {
      console.warn("Error extracting metadata key:", error);
      return null;
    }
  }

  // Make API request to Plex server
  async function makeAPIRequest(url, token) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          "X-Plex-Token": token,
          Accept: "application/json",
        },
        timeout: config.apiTimeout,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      console.warn("API request failed:", error);
      return null;
    }
  }

  // Get watch status from Plex API
  async function getWatchStatusFromAPI(metadataKey, serverInfo) {
    try {
      if (!metadataKey || !serverInfo.token) {
        return null;
      }

      const cacheKey = `${metadataKey}-${serverInfo.serverId}`;
      const cached = apiCache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < cacheTimeout) {
        return cached.data;
      }

      let apiURL;
      if (serverInfo.baseURL.includes("app.plex.tv")) {
        apiURL = `${serverInfo.baseURL}/servers/${serverInfo.serverId}${metadataKey}`;
      } else {
        apiURL = `${serverInfo.baseURL}${metadataKey}`;
      }

      const response = await makeAPIRequest(apiURL, serverInfo.token);

      if (response && response.MediaContainer && response.MediaContainer.Metadata) {
        const metadata = response.MediaContainer.Metadata[0];

        const watchStatus = {
          viewCount: metadata.viewCount || 0,
          viewOffset: metadata.viewOffset || 0,
          duration: metadata.duration || 0,
          lastViewedAt: metadata.lastViewedAt || null,
        };

        apiCache.set(cacheKey, {
          data: watchStatus,
          timestamp: Date.now(),
        });

        return watchStatus;
      }

      return null;
    } catch (error) {
      console.warn("Error getting watch status from API:", error);
      return null;
    }
  }

  // Calculate play status from API data (simplified to just watched/unwatched)
  function calculatePlayStatus(watchStatus) {
    if (!watchStatus) {
      return "unwatched";
    }

    const { viewCount, viewOffset, duration } = watchStatus;

    // If viewCount > 0, it's been watched
    if (viewCount && viewCount > 0) {
      return "watched";
    }

    // If there's significant progress (90%+), consider it watched
    if (viewOffset && viewOffset > 0 && duration && duration > 0) {
      const percentage = (viewOffset / duration) * 100;
      if (percentage >= 90) {
        return "watched";
      }
    }

    return "unwatched";
  }

  // CSS for triangle indicators
  const css = `
    .plex-triangle-status {
        position: absolute;
        top: 0;
        left: 0;
        width: 0;
        height: 0;
        border-style: solid;
        border-width: ${config.triangleSize} ${config.triangleSize} 0 0;
        z-index: 1000;
        pointer-events: none;
    }
    
    .plex-triangle-status.unwatched {
        border-color: ${config.unwatchedColor} transparent transparent transparent;
    }
    
    .plex-triangle-status.watched {
        border-color: ${config.watchedColor} transparent transparent transparent;
    }
    
    .PlaylistItemRow-overlay-YLAKP9,
    [class*="PlaylistItemRow-overlay-"] {
        position: relative;
    }
    
    .PlaylistItemMetadata-cardContainer-lvyC5k,
    [class*="PlaylistItemMetadata-cardContainer-"],
    [class*="PlaylistItemMetadata-card-"],
    [class*="MetadataPosterButtonCard-card-"] {
        position: relative;
    }
  `;

  // Inject CSS
  function injectCSS() {
    const style = document.createElement("style");
    style.textContent = css;
    document.head.appendChild(style);
  }

  // Get play status from Plex API (enhanced fallback method)
  async function getPlayStatus(element) {
    try {
      // First, try to get status from Plex API
      const metadataKey = getMetadataKey(element);
      const serverInfo = getPlexServerInfo();

      if (metadataKey && serverInfo.token) {
        const watchStatus = await getWatchStatusFromAPI(metadataKey, serverInfo);
        if (watchStatus) {
          return calculatePlayStatus(watchStatus);
        }
      }

      // Fallback to DOM inspection if API fails
      return getPlayStatusFromDOM(element);
    } catch (error) {
      console.warn("Error getting play status:", error);
      return getPlayStatusFromDOM(element);
    }
  }

  // Fallback method to get play status from DOM elements
  function getPlayStatusFromDOM(element) {
    try {
      const metadataContainer = element.querySelector('[class*="PlaylistItemMetadata-container-"]') || element.querySelector('[class*="PlaylistItemRow-metadataContainer-"]') || element;

      const watchedIndicator =
        metadataContainer.querySelector('[data-testid="watchedIndicator"]') ||
        metadataContainer.querySelector('[class*="watched-indicator"]') ||
        metadataContainer.querySelector('[aria-label*="watched"]') ||
        metadataContainer.querySelector('[class*="WatchedIndicator"]');

      if (watchedIndicator) {
        return "watched";
      }

      // Check for progress bar with high completion
      const progressBar = metadataContainer.querySelector('[role="progressbar"]');
      if (progressBar) {
        const ariaValueNow = progressBar.getAttribute("aria-valuenow");
        const ariaValueMax = progressBar.getAttribute("aria-valuemax");
        if (ariaValueNow && ariaValueMax) {
          const percentage = (parseFloat(ariaValueNow) / parseFloat(ariaValueMax)) * 100;
          if (percentage >= 90) return "watched";
        }
      }

      // Check for view offset data
      const viewOffset = metadataContainer.querySelector("[data-view-offset]");
      if (viewOffset) {
        const offset = parseInt(viewOffset.getAttribute("data-view-offset"));
        const duration = parseInt(viewOffset.getAttribute("data-duration"));
        if (offset && duration) {
          const percentage = (offset / duration) * 100;
          if (percentage >= 90) return "watched";
        }
      }

      return "unwatched";
    } catch (error) {
      console.warn("Error getting play status from DOM:", error);
      return "unwatched";
    }
  }

  // Create triangle indicator element
  function createTriangleIndicator(status) {
    const indicator = document.createElement("div");
    indicator.className = `plex-triangle-status ${status}`;
    return indicator;
  }

  // Add triangle indicators to playlist items
  async function addTriangleIndicators() {
    const selectors = ['[data-testid="playlistItem"]', '[class*="PlaylistItemRow-overlay-"]', '[class*="PlaylistItemRow-container-"]', '[class*="PlaylistItemDragSource-container-"]'];

    let items = [];
    for (const selector of selectors) {
      const found = document.querySelectorAll(selector);
      if (found.length > 0) {
        items = [...found];
        break;
      }
    }

    if (items.length === 0) {
      items = document.querySelectorAll('div[class*="PlaylistItem"]');
    }

    const promises = Array.from(items).map(async (item) => {
      // Skip if already processed
      if (item.querySelector(".plex-triangle-status")) {
        return;
      }

      // Find the poster/card container
      const cardContainer = item.querySelector('[class*="PlaylistItemMetadata-cardContainer-"]') || item.querySelector('[class*="PlaylistItemMetadata-card-"]') || item.querySelector('[class*="MetadataPosterButtonCard-card-"]') || item;

      // Make sure the container is relatively positioned
      if (!cardContainer.style.position) {
        cardContainer.style.position = "relative";
      }

      // Get play status
      const status = await getPlayStatus(item);

      // Create and add triangle indicator
      const indicator = createTriangleIndicator(status);
      cardContainer.appendChild(indicator);
    });

    await Promise.all(promises);
  }

  // Update existing indicators
  async function updateTriangleIndicators() {
    const indicators = document.querySelectorAll(".plex-triangle-status");

    const promises = Array.from(indicators).map(async (indicator) => {
      const container = indicator.parentElement;
      const item = container.closest('[data-testid="playlistItem"]') || container.closest('[class*="PlaylistItemRow-"]') || container.closest('[class*="PlaylistItem"]');

      if (item) {
        const status = await getPlayStatus(item);
        indicator.className = `plex-triangle-status ${status}`;
      }
    });

    await Promise.all(promises);
  }

  // Initialize when page loads
  function initialize() {
    injectCSS();

    // Initial run
    setTimeout(addTriangleIndicators, 1000);

    // Set up periodic updates
    setInterval(() => {
      addTriangleIndicators();
      updateTriangleIndicators();
    }, config.updateInterval);

    // Listen for navigation changes in Plex
    let lastUrl = location.href;
    new MutationObserver(() => {
      const url = location.href;
      if (url !== lastUrl) {
        lastUrl = url;
        setTimeout(addTriangleIndicators, 1000);
      }
    }).observe(document, { subtree: true, childList: true });
  }

  // Wait for DOM to be ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initialize);
  } else {
    initialize();
  }

  // Also initialize when the page is fully loaded
  window.addEventListener("load", () => {
    setTimeout(initialize, 500);
  });
})();
