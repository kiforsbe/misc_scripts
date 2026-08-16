// ==UserScript==
// @name         Plex Playlist Play Status
// @version      1.2
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

  // Get current users data from localStorage
  function getCurrentUsersData() {
    try {
      return JSON.parse(localStorage.users)?.users || [];
    } catch {
      return [];
    }
  }

  // Get current serverId from URL hash
  function getCurrentServerId() {
    return window.location.hash
      .split('/server/').pop()
      .split?.('/').shift();
  }

  // Get current playlistId from URL hash
  function getCurrentPlaylistId() {
    return decodeURIComponent(window.location.hash)
      .split('key=/playlists/').pop()
      .split?.('&').shift();
  }

  // Get all server details for all users
  function getServerDetailsForAll() {
    return Object.fromEntries(
      getCurrentUsersData()
        .filter(user => user.servers.length)
        .map(userWithServers => userWithServers.servers)
        .flat()
        .map(server => [ server.machineIdentifier, server ])
    );
  }

  // Get server details for a specific serverId
  function getServerDetailsForId(serverId) {
    return getServerDetailsForAll()[serverId];
  }

  // Fetch playlist XML from server
  function getPlaylist(serverId, playlistId) {
    const serverDetails = getServerDetailsForId(serverId);
    if (!serverDetails) return Promise.resolve(null);
    const url = `${serverDetails.connections[0]?.uri || 'https://localhost:32400'}/playlists/${playlistId}/items?includeExternalMedia=1&X-Plex-Token=${serverDetails.accessToken}`;
    return fetch(url)
      .then(response => response.text())
      .then(responseText => (new DOMParser()).parseFromString(responseText, 'application/xml'));
  }

  // Add style element to the page
  function addStyle(css) {
    let shouldAddToHead = false;
    let style = document.querySelector('#plex-playlist-played-css');
    if (style == null) {
      style = document.createElement('style');
      style.id = 'plex-playlist-played-css';
      shouldAddToHead = true;
    }
    style.textContent = css;
    if (shouldAddToHead) {
      document.head.append(style);
    }
  }

  // Generate CSS for unwatched and watched playlist items
  function getCssForPlaylist(serverId, dom) {
    const videosNotPlayed = Array.from(dom.querySelectorAll('Video:not([viewCount])'));
    const videosPlayed = Array.from(dom.querySelectorAll('Video[viewCount]'));

    // Unwatched (orange)
    const cssUnwatched = videosNotPlayed
      .map(video => ({
        key: video.attributes['key'].value,
        type: video.attributes['type'].value,
      }))
      .map(attrs => {
        const selector = `a[data-testid="metadataTitleLink"][href*="/server/${serverId}"][href*="${encodeURIComponent(attrs.key)}&"]`;
        return `${selector}::after {
          content: '';
          position: absolute;
          ${attrs.type === 'episode'
            ? `left: 155px; top: 16px;`
            : `left: 141px; top: 7px;`
          }
          width: 0px;
          height: 0px;
          border: 8px solid #e5a00d;
          border-color: #e5a00d #e5a00d transparent transparent;
          filter: drop-shadow(0px 1px 0px rgba(0,0,0,0.5));
          pointer-events: none;
        }`;
      })
      .join('\n');

    // Watched (gray)
    const cssWatched = videosPlayed
      .map(video => ({
        key: video.attributes['key'].value,
        type: video.attributes['type'].value,
      }))
      .map(attrs => {
        const selector = `a[data-testid="metadataTitleLink"][href*="/server/${serverId}"][href*="${encodeURIComponent(attrs.key)}&"]`;
        return `${selector}::after {
          content: '';
          position: absolute;
          ${attrs.type === 'episode'
            ? `left: 155px; top: 16px;`
            : `left: 141px; top: 7px;`
          }
          width: 0px;
          height: 0px;
          border: 8px solid #000;
          border-color: #000 #000 transparent transparent;
          filter: drop-shadow(0px 1px 0px rgba(0,0,0,0.5));
          pointer-events: none;
        }`;
      })
      .join('\n');

    return cssUnwatched + '\n' + cssWatched;
  }

  // Main logic: check if current page is a playlist and update indicators
  function checkCurrentPage() {
    if (window.location.hash.includes(`key=${encodeURIComponent('/playlists/')}`)) {
      const serverId = getCurrentServerId();
      const playlistId = getCurrentPlaylistId();
      getPlaylist(serverId, playlistId).then(dom => {
        if (!dom) return;
        const css = getCssForPlaylist(serverId, dom);
        addStyle(css);
      });
    }
  }

  // Listen for hash changes (navigation)
  window.addEventListener('hashchange', checkCurrentPage);

  // Initial check on load
  checkCurrentPage();
})();
