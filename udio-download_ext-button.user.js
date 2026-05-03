// ==UserScript==
// @name         Add Download Button to Udio and Flow Music Song Pages
// @namespace    http://tampermonkey.net/
// @version      0.9
// @description  Adds a download button on Udio and Flow Music song pages, using metadata from the current track page
// @match        https://www.udio.com/*
// @match        https://www.flowmusic.app/*
// @icon         https://udio.com/favicon.ico
// @run-at       document-idle
// ==/UserScript==

(function() {
  'use strict';

  const CUSTOM_BUTTON_SELECTOR = '[data-download-ext-button="true"]';
    const DEBUG = true;
    let activeObserver = null;
    let lastUrl = window.location.href;

    function debugLog(event, details) {
            if (!DEBUG) {
                    return;
            }

            console.debug('[download-ext]', event, details || '');
    }

    function getCurrentSongIdFromPath() {
            const match = window.location.pathname.match(/\/song[s]?\/([^/?#]+)/);
            return match ? match[1] : null;
    }

  function getMetadata(key) {
      const selectors = [
          `meta[property="${key}"]`,
          `meta[name="${key}"]`
      ];

      for (const selector of selectors) {
          const meta = document.querySelector(selector);
          if (meta) {
              return meta.getAttribute('content');
          }
      }

      return null;
  }

  function getCanonicalUrl() {
      return document.querySelector('link[rel="canonical"]')?.getAttribute('href') || getMetadata('og:url') || window.location.href;
  }

  function flowMetadataMatchesCurrentSong() {
      const currentSongId = getCurrentSongIdFromPath();
      if (!currentSongId) {
          return true;
      }

      const canonical = getCanonicalUrl();
      const audioUrl = getMetadata('og:audio') || '';

      return canonical.includes(currentSongId) || audioUrl.includes(currentSongId);
  }

  function getCreationYearFromText(prefix) {
      const titledNode = Array.from(document.querySelectorAll('[title]'))
          .find(node => node.getAttribute('title')?.startsWith(prefix));

      if (!titledNode) {
          return null;
      }

      const dateMatch = titledNode.textContent.match(/\d{4}/);
      return dateMatch ? dateMatch[0] : null;
  }

  function getFlowSongData() {
      const nextData = window.__NEXT_DATA__;
      const queries = nextData?.props?.sdc?.queryClient?.queries;

      if (!Array.isArray(queries)) {
          return null;
      }

      const clipQuery = queries.find(query => Array.isArray(query?.queryKey) && query.queryKey[0] === 'clip');
      return clipQuery?.state?.data || null;
  }

  function getUdioSongData() {
      for (const script of Array.from(document.scripts)) {
          const text = script.textContent || '';
          if (!text.includes('song_path')) {
              continue;
          }

          const songPathMatch = text.match(/\\?"song_path\\?":\\?"([^"\\]+)\\?"/);
          const imagePathMatch = text.match(/\\?"image_path\\?":\\?"([^"\\]+)\\?"/);
          const promptMatch = text.match(/\\?"prompt\\?":\\?"([^]*?)\\?",\\?"likes\\?":/);

          if (songPathMatch) {
              return {
                  songPath: songPathMatch[1],
                  imagePath: imagePathMatch ? imagePathMatch[1] : null,
                  prompt: promptMatch ? promptMatch[1] : null
              };
          }
      }

      return null;
  }

  function parseUdioArtistAndTitle(ogTitle) {
      if (!ogTitle) {
          return { artist: null, title: null };
      }

      const parts = ogTitle.split(' - ');
      if (parts.length < 2) {
          return { artist: null, title: null };
      }

      return {
          artist: parts[0].trim(),
          title: parts[1].split(' | ')[0].trim()
      };
  }

  function parseFlowArtistAndTitle() {
      const songData = getFlowSongData();
      const ogTitle = getMetadata('og:title') || '';
      const match = ogTitle.match(/^(.*) by ([^]+)$/);

      return {
          artist: songData?.author?.username || getMetadata('music:musician') || (match ? match[2].trim() : null),
          title: songData?.title || (match ? match[1].trim() : ogTitle.trim()) || null
      };
  }

  function getUdioLyrics() {
      const lyricsHeader = Array.from(document.querySelectorAll('div')).find(div => div.textContent.trim() === 'Lyrics');
      if (!lyricsHeader) {
          return null;
      }

      return lyricsHeader.nextElementSibling ? lyricsHeader.nextElementSibling.textContent.trim() : '';
  }

  function getFlowLyrics() {
      const songData = getFlowSongData();
      if (songData?.lyrics?.value?.text) {
          return songData.lyrics.value.text;
      }

      const lyricsSection = Array.from(document.querySelectorAll('div'))
          .find(node => node.textContent.trim() === 'Lyrics')
          ?.parentElement;

      return lyricsSection ? lyricsSection.textContent.replace(/^Lyrics/, '').trim() : null;
  }

  function getFlowActionButton() {
      return document.querySelector('button[aria-label="Share"]');
  }

  function getUdioActionButton() {
      const shareButton = Array.from(document.querySelectorAll('button'))
          .find(button => button.textContent.trim() === 'Share');

      return shareButton || document.querySelector('button[title^="Open track actions for"]');
  }

  const SITE_CONFIGS = {
      'www.udio.com': {
          album: 'Udio',
          requireMp3Url: false,
          getAnchorButton: getUdioActionButton,
          getButtonContainer: anchorButton => anchorButton.parentElement,
          insertButton: (container, button, anchorButton) => {
              container.insertBefore(button, anchorButton.nextSibling);
          },
          getMetadata: () => {
              const songData = getUdioSongData();
              const ogTitle = getMetadata('og:title');
              const parsed = parseUdioArtistAndTitle(ogTitle);

              return {
                  mp3Url: songData?.songPath || getMetadata('og:audio') || '',
                  imageUrl: songData?.imagePath || getMetadata('og:image') || '',
                  videoUrl: '',
                  artist: parsed.artist || '',
                  title: parsed.title || '',
                  year: getCreationYearFromText('Created at') || '',
                  canonical: getCanonicalUrl(),
                  description: songData?.prompt || getMetadata('og:description') || '',
                  lyrics: getUdioLyrics() || ''
              };
          },
          isReady: () => {
              const metadata = SITE_CONFIGS['www.udio.com'].getMetadata();
              return Boolean(
                  SITE_CONFIGS['www.udio.com'].getAnchorButton() &&
                  metadata.title &&
                  metadata.year &&
                  metadata.lyrics !== null
              );
          }
      },
      'www.flowmusic.app': {
          album: 'Flow Music',
          requireMp3Url: false,
          getAnchorButton: getFlowActionButton,
          getButtonContainer: anchorButton => anchorButton.parentElement?.parentElement || null,
          insertButton: (container, button) => {
              container.appendChild(button);
          },
          getMetadata: () => {
              const songData = getFlowSongData();
              const parsed = parseFlowArtistAndTitle();
              const createdAt = songData?.created_at;

              return {
                  mp3Url: songData?.audio_url || getMetadata('og:audio') || '',
                  imageUrl: songData?.image_url || getMetadata('og:image') || '',
                  videoUrl: songData?.video_url || getMetadata('og:video') || '',
                  artist: parsed.artist || '',
                  title: parsed.title || '',
                  year: createdAt ? new Date(createdAt).getUTCFullYear().toString() : '',
                  canonical: getCanonicalUrl(),
                  description: songData?.operation?.sound_prompt || getMetadata('og:description') || '',
                  lyrics: getFlowLyrics() || ''
              };
          },
          isReady: () => {
              const metadata = SITE_CONFIGS['www.flowmusic.app'].getMetadata();
              return Boolean(
                  getFlowActionButton() &&
                  flowMetadataMatchesCurrentSong() &&
                  metadata.mp3Url &&
                  metadata.artist &&
                  metadata.title
              );
          }
      }
  };

  function getSiteConfig() {
      return SITE_CONFIGS[window.location.hostname] || null;
  }

  function getButtonContainer(anchorButton) {
      const siteConfig = getSiteConfig();
      return siteConfig?.getButtonContainer ? siteConfig.getButtonContainer(anchorButton) : anchorButton.parentElement;
  }

  function buildDownloadRequestState(siteConfig) {
      if (!siteConfig) {
          debugLog('build-state:no-site-config');
          return null;
      }

      const metadata = siteConfig.getMetadata();
      if (siteConfig.requireMp3Url !== false && !metadata.mp3Url) {
          debugLog('build-state:missing-mp3', {
              host: window.location.hostname,
              metadata
          });
          return null;
      }

      const params = new URLSearchParams({
          mp3_url: metadata.mp3Url,
          image_url: metadata.imageUrl || '',
          video_url: metadata.videoUrl || '',
          artist: metadata.artist || '',
          title: metadata.title || '',
          year: metadata.year || '',
          album: siteConfig.album,
          canonical: metadata.canonical || '',
          description: metadata.description || '',
          lyrics: metadata.lyrics || ''
      });

      return {
          metadata,
          params,
          url: `http://localhost:5000/api/download_ext?${params.toString()}`
      };
  }

  function buildDownloadUrl(siteConfig) {
      const requestState = buildDownloadRequestState(siteConfig);
      if (!requestState) {
          return null;
      }

      debugLog('build-url:success', {
          host: window.location.hostname,
          songId: getCurrentSongIdFromPath(),
          metadata: requestState.metadata,
          url: requestState.url
      });
      return requestState.url;
  }

  function createDownloadButton(anchorButton) {
      const button = document.createElement('button');
      button.className = anchorButton.className;
      button.setAttribute('data-download-ext-button', 'true');
      button.setAttribute('data-download-ext-url', window.location.href);
      button.type = 'button';
      button.title = 'Download track';
      button.style.width = 'auto';
      button.style.minWidth = '3rem';
      button.style.paddingInline = '0.9rem';

      const label = document.createElement('span');
      label.textContent = 'Download';
      button.appendChild(label);

      button.addEventListener('click', () => {
          const siteConfig = getSiteConfig();
          const requestState = buildDownloadRequestState(siteConfig);
          const href = requestState?.url || null;
          debugLog('button:click', {
              currentUrl: window.location.href,
              buttonUrl: button.getAttribute('data-download-ext-url') || '',
              host: window.location.hostname,
              songId: getCurrentSongIdFromPath(),
              href,
              mp3_url: requestState?.params.get('mp3_url') || '',
              image_url: requestState?.params.get('image_url') || '',
              video_url: requestState?.params.get('video_url') || '',
              artist: requestState?.params.get('artist') || '',
              title: requestState?.params.get('title') || '',
              year: requestState?.params.get('year') || '',
              album: requestState?.params.get('album') || '',
              canonical: requestState?.params.get('canonical') || '',
              description: requestState?.params.get('description') || '',
              lyrics: requestState?.params.get('lyrics') || ''
          });
          if (href) {
              window.location.assign(href);
          }
      });

      return button;
  }

  function removeDownloadButtons() {
      const buttons = Array.from(document.querySelectorAll(CUSTOM_BUTTON_SELECTOR));
      debugLog('button:remove-all', {
          count: buttons.length,
          currentUrl: window.location.href
      });
      for (const button of buttons) {
          button.remove();
      }
  }

  function addDownloadButton() {
      const siteConfig = getSiteConfig();
      if (!siteConfig) {
          debugLog('button:add:no-site-config');
          return;
      }

      const existingButton = siteConfig.getAnchorButton();
      if (!existingButton) {
          debugLog('button:add:no-anchor', {
              host: window.location.hostname,
              currentUrl: window.location.href
          });
          return;
      }

      const buttonContainer = getButtonContainer(existingButton);
      if (!buttonContainer || buttonContainer.querySelector(CUSTOM_BUTTON_SELECTOR)) {
          debugLog('button:add:skip-existing-or-missing-container', {
              hasContainer: Boolean(buttonContainer),
              hasExistingButton: Boolean(buttonContainer?.querySelector(CUSTOM_BUTTON_SELECTOR)),
              currentUrl: window.location.href
          });
          return;
      }

      const downloadUrl = buildDownloadUrl(siteConfig);
      if (!downloadUrl) {
          debugLog('button:add:no-download-url', {
              host: window.location.hostname,
              currentUrl: window.location.href
          });
          return;
      }

      const button = createDownloadButton(existingButton);
      debugLog('button:add:insert', {
          host: window.location.hostname,
          currentUrl: window.location.href,
          anchorAria: existingButton.getAttribute('aria-label'),
          anchorText: existingButton.textContent.trim(),
          containerTag: buttonContainer.tagName,
          containerClass: buttonContainer.className
      });

      if (siteConfig.insertButton) {
          siteConfig.insertButton(buttonContainer, button, existingButton);
          return;
      }

      buttonContainer.insertBefore(button, existingButton.nextSibling);
  }

  function waitForExistingButton() {
      if (!getSiteConfig()) {
          debugLog('wait:no-site-config', {
              currentUrl: window.location.href
          });
          return;
      }

      if (activeObserver) {
          activeObserver.disconnect();
          activeObserver = null;
      }

      const ensureDownloadButton = () => {
          const siteConfig = getSiteConfig();
          if (!siteConfig) {
              debugLog('ensure:no-site-config', {
                  currentUrl: window.location.href
              });
              return;
          }

          const metadata = siteConfig.getMetadata();
          const ready = siteConfig.isReady();
          debugLog('ensure:state', {
              host: window.location.hostname,
              currentUrl: window.location.href,
              currentSongId: getCurrentSongIdFromPath(),
              ready,
              metadata,
              hasAnchor: Boolean(siteConfig.getAnchorButton()),
              hasExistingButton: Boolean(document.querySelector(CUSTOM_BUTTON_SELECTOR))
          });

          if (siteConfig.isReady()) {
              addDownloadButton();
          }
      };

      ensureDownloadButton();

      activeObserver = new MutationObserver((mutations, obs) => {
          ensureDownloadButton();
      });

      activeObserver.observe(document.body, {
          childList: true,
          subtree: true
      });
  }

  function syncForCurrentUrl() {
      if (window.location.href === lastUrl) {
          return;
      }

      debugLog('url:changed', {
          previousUrl: lastUrl,
          nextUrl: window.location.href
      });
      lastUrl = window.location.href;
      removeDownloadButtons();
      waitForExistingButton();
  }

  function installLocationChangeListener() {
      const dispatchLocationChange = () => {
          debugLog('url:dispatch-locationchange', {
              currentUrl: window.location.href
          });
          window.dispatchEvent(new Event('locationchange'));
      };

      const originalPushState = history.pushState;
      history.pushState = function(...args) {
          const result = originalPushState.apply(this, args);
          dispatchLocationChange();
          return result;
      };

      const originalReplaceState = history.replaceState;
      history.replaceState = function(...args) {
          const result = originalReplaceState.apply(this, args);
          dispatchLocationChange();
          return result;
      };

      window.addEventListener('popstate', dispatchLocationChange);
      window.addEventListener('locationchange', syncForCurrentUrl);
  }

    debugLog('init', {
            currentUrl: window.location.href,
            host: window.location.hostname
    });
  waitForExistingButton();
  installLocationChangeListener();
})();
