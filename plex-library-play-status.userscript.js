// ==UserScript==
// @name         Plex Unplayed Items Pip Indicator
// @namespace    http://tampermonkey.net/
// @version      1.2
// @description  Show a pip indicator left of filename for unplayed items in Plex library table view (based on Playlist Play Status logic)
// @author       Kim Forsberg
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

  // Configuration based on common Plex userscript patterns
  const CONFIG = {
    // Selectors for different Plex view types - scoped to library container
    selectors: {
      // Main library container
      libraryContainer: '[class*="DirectoryListPageContent-listContainer"]',
      
      // Table view rows within library container
      tableRows: '[class*="ListRow-row"], [class*="ListRow-alternateRow"]',

      // Title cells in table view - use the data-testid which is more stable
      titleCells: '[data-testid="metadataTitleLink"]',

      // Table headers - scoped to library
      tableHeaders: '[class*="DirectoryListTableHeader-tableHeader"]',
      
      // Header links within the table header
      headerLinks: 'a[role="link"]',
      
      // Generic table cells
      tableCells: '[class*="TableCell-tableCell"]',
    },

    // Update intervals and timing
    updateInterval: 2000, // Check for changes every 2 seconds
    debounceDelay: 300,   // Debounce processing calls
    headerCacheTime: 5000, // Cache header scan for 5 seconds

    // Pip styling
    pipStyle: {
      unplayed: {
        width: "10px",
        height: "10px",
        backgroundColor: "#e5a00d", // Plex orange - solid
        borderRadius: "50%",
        display: "inline-block",
        marginRight: "8px",
        verticalAlign: "middle",
        flexShrink: "0",
        position: "relative",
        top: "-1px",
      },
      played: {
        width: "10px",
        height: "10px",
        backgroundColor: "transparent", // Hollow
        border: "2px solid #999999", // Light gray border
        borderRadius: "50%",
        display: "inline-block",
        marginRight: "8px",
        verticalAlign: "middle",
        flexShrink: "0",
        position: "relative",
        top: "-1px",
        boxSizing: "border-box",
      },
    },
  };

  // Cache for column index to avoid re-scanning headers
  let dateViewedColumnIndex = -1;
  let lastHeaderScan = 0;

  // Cache for tracking processed items to avoid duplicates
  const processedItems = new WeakSet();

  // Track current URL and page state
  let currentURL = '';
  let currentLibraryContainer = null;
  let updateIntervalId = null;

  // Create pip element
  function createPip(playStatus) {
    const pip = document.createElement("span");
    pip.className = `plex-${playStatus}-pip`;
    pip.setAttribute("title", playStatus === "unplayed" ? "Unplayed" : "Played");
    pip.setAttribute("data-pip", playStatus);

    Object.assign(pip.style, CONFIG.pipStyle[playStatus]);

    return pip;
  }

  // Check if current page is a library page that needs processing
  function isLibraryPage() {
    return window.location.hash.includes('library/sections/') && 
           !window.location.hash.includes('/collections/') &&
           !window.location.hash.includes('/playlists/');
  }

  // Detect if page content has changed
  function hasPageChanged() {
    if (currentURL !== window.location.href) {
      currentURL = window.location.href;
      return true;
    }

    // Check if library container has changed (new content loaded)
    const libraryContainer = document.querySelector(CONFIG.selectors.libraryContainer);
    if (libraryContainer !== currentLibraryContainer) {
      currentLibraryContainer = libraryContainer;
      return true;
    }

    return false;
  }

  // Main logic: check if current page needs processing and update indicators
  function checkCurrentPage() {
    if (!isLibraryPage()) {
      // Not a library page, clear any existing interval
      if (updateIntervalId) {
        clearInterval(updateIntervalId);
        updateIntervalId = null;
      }
      return;
    }

    // Check if page has changed
    if (hasPageChanged()) {
      // Reset caches when page changes
      processedItems.clear();
      dateViewedColumnIndex = -1;
      lastHeaderScan = 0;
      
      // Process the new page content
      debouncedProcess(500); // Longer delay for page changes
    }

    // Start regular checking if not already running
    if (!updateIntervalId) {
      updateIntervalId = setInterval(() => {
        if (isLibraryPage()) {
          processAllRows();
        }
      }, CONFIG.updateInterval);
    }
  }

  // Debounced processing to avoid excessive calls
  let processingTimeout;
  function debouncedProcess(delay = CONFIG.debounceDelay) {
    clearTimeout(processingTimeout);
    processingTimeout = setTimeout(processAllRows, delay);
  }

  // Remove the complex mutation observer in favor of simpler interval checking
  function startPageMonitoring() {
    // Initial check
    checkCurrentPage();
    
    // Listen for hash changes (navigation)
    window.addEventListener('hashchange', checkCurrentPage);
    
    // Periodic check for page changes
    setInterval(checkCurrentPage, CONFIG.updateInterval);
  }

  // Find the "Date Viewed" column index by scanning table headers within library container
  function findDateViewedColumnIndex() {
    const now = Date.now();
    // Only rescan headers based on config time
    if (dateViewedColumnIndex !== -1 && now - lastHeaderScan < CONFIG.headerCacheTime) {
      return dateViewedColumnIndex;
    }

    // Reset the index
    dateViewedColumnIndex = -1;
    lastHeaderScan = now;

    // Look for the library container first
    const libraryContainer = document.querySelector(CONFIG.selectors.libraryContainer);
    if (!libraryContainer) {
      return -1;
    }

    // Look for the table header container within the library
    const headerContainer = libraryContainer.querySelector(CONFIG.selectors.tableHeaders);
    if (!headerContainer) {
      return -1;
    }

    // Find all header links that represent columns
    const headerLinks = headerContainer.querySelectorAll(CONFIG.selectors.headerLinks);
    
    headerLinks.forEach((header, index) => {
      // Look for the span with title="Date Viewed" within this header
      const titleSpan = header.querySelector('span[title="Date Viewed"]');
      if (titleSpan) {
        dateViewedColumnIndex = index;
        return;
      }
      
      // Alternative: check if the header text itself is "Date Viewed"
      const headerText = header.textContent.trim().toLowerCase();
      if (headerText === 'date viewed') {
        dateViewedColumnIndex = index;
        return;
      }
    });

    return dateViewedColumnIndex;
  }

  // Generic method to get table cells from a row (excluding action cells)
  function getTableCellsFromRow(row) {
    // Get all table cells within this row
    const allCells = row.querySelectorAll(CONFIG.selectors.tableCells);
    
    // Filter out non-content cells based on their position and content
    return Array.from(allCells).filter(cell => {
      // Skip cells that contain select buttons
      if (cell.closest('[class*="selectButton"]') || 
          cell.matches('[class*="selectButton"]')) return false;
      
      // Skip the play cell
      if (cell.matches('[class*="playCell"]')) return false;
      
      // Skip the actions cell (has edit/more buttons)
      if (cell.matches('[class*="actionsCell"]') || 
          cell.querySelector('svg[id*="edit"]') || 
          cell.querySelector('svg[id*="more"]')) return false;
      
      return true;
    });
  }

  // Simplified play status detection using dynamic column detection
  function getPlayStatus(row) {
    // Find the Date Viewed column index from headers
    const columnIndex = findDateViewedColumnIndex();
    
    // Get the content cells from this specific row (excluding action/select cells)
    const contentCells = getTableCellsFromRow(row);
    
    if (columnIndex !== -1 && contentCells.length > columnIndex) {
      // Get the cell at the Date Viewed column index
      const dateViewedCell = contentCells[columnIndex];
      const dateViewedText = dateViewedCell.textContent.trim().toLowerCase();
      
      console.debug('Checking row cell at index', columnIndex, ':', dateViewedText);
      
      if (dateViewedText === 'unplayed') {
        return 'unplayed';
      } else if (dateViewedText && dateViewedText !== 'unplayed') {
        // Any other text (like dates, "23 minutes ago", etc.) means it's been played
        return 'played';
      }
    }

    // Fallback: search all content cells in this row for "Unplayed" text
    console.debug('Column index not found or invalid, searching all cells in row');
    for (const cell of contentCells) {
      const cellText = cell.textContent.trim().toLowerCase();
      if (cellText === 'unplayed') {
        return 'unplayed';
      }
    }

    // Default: assume played if no "Unplayed" indicator found
    return 'played';
  }

  // Add or remove pip based on play status
  function updateRowPip(row) {
    if (processedItems.has(row)) {
      return; // Already processed this exact row element
    }

    const titleCell = row.querySelector(CONFIG.selectors.titleCells);
    if (!titleCell) return;

    const playStatus = getPlayStatus(row);
    const existingUnplayedPip = titleCell.querySelector(".plex-unplayed-pip");
    const existingPlayedPip = titleCell.querySelector(".plex-played-pip");

    // Remove any existing pips first
    if (existingUnplayedPip) {
      existingUnplayedPip.remove();
    }
    if (existingPlayedPip) {
      existingPlayedPip.remove();
    }

    // Add the appropriate pip based on play status
    if (playStatus === "unplayed" || playStatus === "played") {
      const pip = createPip(playStatus);
      // Insert at the very beginning of the title cell
      if (titleCell.firstChild) {
        titleCell.insertBefore(pip, titleCell.firstChild);
      } else {
        titleCell.appendChild(pip);
      }
    }

    processedItems.add(row);
  }

  // Process all visible table rows within the library container
  function processAllRows() {
    // Find the library container first
    const libraryContainer = document.querySelector(CONFIG.selectors.libraryContainer);
    if (!libraryContainer) {
      return;
    }

    // Find rows only within the library container
    const rows = libraryContainer.querySelectorAll(CONFIG.selectors.tableRows);

    rows.forEach((row) => {
      try {
        updateRowPip(row);
      } catch (e) {
        console.debug("Plex Unplayed Pip: Error processing row:", e);
      }
    });
  }

  // Initialize the script
  function initialize() {
    console.log("Plex Unplayed Pip Indicator: Initializing...");

    // Start monitoring for page changes
    startPageMonitoring();

    console.log("Plex Unplayed Pip Indicator: Initialized successfully");
  }

  // Start when DOM is ready or immediately if already loaded
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initialize);
  } else {
    initialize();
  }

  // Expose utilities for debugging
  window.plexUnplayedPipDebug = {
    config: CONFIG,
    processAllRows,
    getPlayStatus,
    checkCurrentPage,
    isLibraryPage,
    processedCount: () => processedItems.size,
  };
})();