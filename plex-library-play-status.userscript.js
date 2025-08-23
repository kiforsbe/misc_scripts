// ==UserScript==
// @name         Plex Unplayed Items Pip Indicator
// @namespace    http://tampermonkey.net/
// @version      1.1
// @description  Show a pip indicator left of filename for unplayed items in Plex library table view (based on Playlist Play Status logic)
// @author       You
// @match        https://app.plex.tv/*
// @match        http://localhost:32400/web/*
// @match        http://127.0.0.1:32400/web/*
// @match        http://*.plex.direct:32400/web/*
// @match        https://*.plex.direct:32400/web/*
// @grant        none
// @run-at       document-end
// ==/UserScript==

(function () {
  "use strict";

  // Configuration based on common Plex userscript patterns
  const CONFIG = {
    // Selectors for different Plex view types
    selectors: {
      // Table view rows
      tableRows: '[data-testid="table-row"], [class*="MetadataTableRow"], [role="row"]:not([class*="header"])',

      // Title cells in table view
      titleCells: '[data-testid="metadataTitleLink"], [class*="titleCell"], .MetadataTableItemTitle, td:first-child > div, td:first-child > a',

      // Play status indicators that Plex uses
      playedIndicators: '[class*="played"], [class*="watched"], [aria-label*="layed"], .PlayedIndicator, [data-qa-id*="played"]',
      unplayedIndicators: '[class*="unplayed"], [class*="unwatched"], [aria-label*="nplayed"]',

      // Progress indicators
      progressBars: '[role="progressbar"], [class*="progress"], [class*="Progress"]',
    },

    // Pip styling
    pipStyle: {
      width: "6px",
      height: "6px",
      backgroundColor: "#e5a00d", // Plex orange
      borderRadius: "50%",
      display: "inline-block",
      marginRight: "6px",
      verticalAlign: "middle",
      flexShrink: "0",
      position: "relative",
      top: "-1px",
    },
  };

  // Cache for tracking processed items to avoid duplicates
  const processedItems = new WeakSet();

  // Create pip element
  function createPip() {
    const pip = document.createElement("span");
    pip.className = "plex-unplayed-pip";
    pip.setAttribute("title", "Unplayed");
    pip.setAttribute("data-pip", "true");

    Object.assign(pip.style, CONFIG.pipStyle);

    return pip;
  }

  // Enhanced play status detection based on Plex's internal mechanisms
  function getPlayStatus(row) {
    // Method 1: Check for explicit played/unplayed indicators
    const playedElement = row.querySelector(CONFIG.selectors.playedIndicators);
    if (playedElement) {
      return "played";
    }

    const unplayedElement = row.querySelector(CONFIG.selectors.unplayedIndicators);
    if (unplayedElement) {
      return "unplayed";
    }

    // Method 2: Check progress bars (most reliable method)
    const progressBars = row.querySelectorAll(CONFIG.selectors.progressBars);
    for (const progressBar of progressBars) {
      const ariaValueNow = progressBar.getAttribute("aria-valuenow");
      const ariaValueMax = progressBar.getAttribute("aria-valuemax");

      if (ariaValueNow !== null) {
        const progress = parseFloat(ariaValueNow);
        const max = parseFloat(ariaValueMax) || 100;

        if (progress === 0) return "unplayed";
        if (progress >= max * 0.9) return "played"; // 90%+ considered played
        return "partial";
      }

      // Check for CSS-based progress
      const progressFill = progressBar.querySelector('[class*="fill"], [class*="bar"]');
      if (progressFill) {
        const width = progressFill.style.width;
        if (width === "0%" || width === "0px") return "unplayed";
        if (parseFloat(width) >= 90) return "played";
        return "partial";
      }
    }

    // Method 3: Check data attributes commonly used by Plex
    const dataAttrs = ["data-played", "data-watched", "data-viewcount", "data-view-count"];
    for (const attr of dataAttrs) {
      const value = row.getAttribute(attr);
      if (value === "true" || (value && parseInt(value) > 0)) {
        return "played";
      }
      if (value === "false" || value === "0") {
        return "unplayed";
      }
    }

    // Method 4: Check for Plex's viewCount in nested elements
    const viewCountElements = row.querySelectorAll('[class*="viewCount"], [class*="ViewCount"]');
    for (const element of viewCountElements) {
      const text = element.textContent.trim();
      const count = parseInt(text);
      if (!isNaN(count)) {
        return count > 0 ? "played" : "unplayed";
      }
    }

    // Method 5: Look for specific Plex class patterns
    const rowClasses = row.className || "";
    if (rowClasses.includes("played") || rowClasses.includes("watched")) {
      return "played";
    }
    if (rowClasses.includes("unplayed") || rowClasses.includes("unwatched")) {
      return "unplayed";
    }

    // Method 6: Check aria-labels for accessibility indicators
    const ariaLabel = row.getAttribute("aria-label") || "";
    if (ariaLabel.toLowerCase().includes("played") || ariaLabel.toLowerCase().includes("watched")) {
      return "played";
    }
    if (ariaLabel.toLowerCase().includes("unplayed") || ariaLabel.toLowerCase().includes("unwatched")) {
      return "unplayed";
    }

    // Default: assume unplayed if no clear indicators found
    return "unplayed";
  }

  // Add or remove pip based on play status
  function updateRowPip(row) {
    if (processedItems.has(row)) {
      return; // Already processed this exact row element
    }

    const titleCell = row.querySelector(CONFIG.selectors.titleCells);
    if (!titleCell) return;

    const playStatus = getPlayStatus(row);
    const existingPip = titleCell.querySelector(".plex-unplayed-pip");

    if (playStatus === "unplayed") {
      if (!existingPip) {
        const pip = createPip();
        // Insert at the very beginning of the title cell
        if (titleCell.firstChild) {
          titleCell.insertBefore(pip, titleCell.firstChild);
        } else {
          titleCell.appendChild(pip);
        }
      }
    } else {
      // Remove pip for played/partial items
      if (existingPip) {
        existingPip.remove();
      }
    }

    processedItems.add(row);
  }

  // Process all visible table rows
  function processAllRows() {
    const rows = document.querySelectorAll(CONFIG.selectors.tableRows);

    rows.forEach((row) => {
      // Skip header rows
      if (row.getAttribute("role") === "columnheader" || row.querySelector("th") || row.matches('[class*="header"]')) {
        return;
      }

      try {
        updateRowPip(row);
      } catch (e) {
        console.debug("Plex Unplayed Pip: Error processing row:", e);
      }
    });
  }

  // Debounced processing to avoid excessive calls
  let processingTimeout;
  function debouncedProcess(delay = 300) {
    clearTimeout(processingTimeout);
    processingTimeout = setTimeout(processAllRows, delay);
  }

  // Enhanced mutation observer for Plex's dynamic content
  function createMutationObserver() {
    const observer = new MutationObserver((mutations) => {
      let shouldProcess = false;

      for (const mutation of mutations) {
        if (mutation.type === "childList") {
          // Check for new table rows or table containers
          for (const node of mutation.addedNodes) {
            if (node.nodeType === Node.ELEMENT_NODE) {
              if (node.matches && (node.matches(CONFIG.selectors.tableRows) || node.matches('[class*="MetadataTable"]') || node.matches('[data-testid*="table"]') || node.matches('[class*="TableContainer"]'))) {
                shouldProcess = true;
                break;
              }

              if (node.querySelector && (node.querySelector(CONFIG.selectors.tableRows) || node.querySelector('[class*="MetadataTable"]'))) {
                shouldProcess = true;
                break;
              }
            }
          }
        } else if (mutation.type === "attributes") {
          // Watch for attribute changes that might indicate play status changes
          const target = mutation.target;
          if (target.matches && target.matches(CONFIG.selectors.tableRows)) {
            shouldProcess = true;
          }
        }
      }

      if (shouldProcess) {
        debouncedProcess(200);
      }
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["class", "data-played", "data-watched", "aria-valuenow"],
    });

    return observer;
  }

  // Handle Plex navigation changes (SPA)
  function handleNavigationChange() {
    let currentURL = location.href;

    const checkNavigation = () => {
      if (currentURL !== location.href) {
        currentURL = location.href;
        // Clear processed items cache on navigation
        processedItems.clear();
        debouncedProcess(1000); // Longer delay for navigation
      }
    };

    setInterval(checkNavigation, 1000);

    // Also listen for popstate events
    window.addEventListener("popstate", () => {
      processedItems.clear();
      debouncedProcess(1000);
    });
  }

  // Initialize the script
  function initialize() {
    console.log("Plex Unplayed Pip Indicator: Initializing...");

    // Initial processing with delay for Plex to render
    debouncedProcess(1500);

    // Set up mutation observer
    createMutationObserver();

    // Handle navigation
    handleNavigationChange();

    // Periodic refresh to catch any missed updates
    setInterval(() => {
      processedItems.clear(); // Clear cache periodically
      debouncedProcess(100);
    }, 30000); // Every 30 seconds

    console.log("Plex Unplayed Pip Indicator: Initialized successfully");
  }

  // Start when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initialize);
  } else {
    // Page already loaded, wait for Plex to render
    setTimeout(initialize, 1000);
  }

  // Expose utilities for debugging
  window.plexUnplayedPipDebug = {
    config: CONFIG,
    processAllRows,
    getPlayStatus,
    processedCount: () => processedItems.size,
  };
})();
