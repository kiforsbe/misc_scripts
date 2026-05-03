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

  function createDownloadButton(anchorButton, href) {
      const button = document.createElement('button');
      button.className = anchorButton.className;
      button.setAttribute('data-download-ext-button', 'true');
      button.type = 'button';
      button.title = 'Download track';
      button.style.width = 'auto';
      button.style.minWidth = '3rem';
      button.style.paddingInline = '0.9rem';

      const label = document.createElement('span');
      label.textContent = 'Download';
      button.appendChild(label);

      button.addEventListener('click', () => {
          window.location.assign(href);
      });

      return button;
  }

  function addDownloadButton() {
      const siteConfig = getSiteConfig();
      if (!siteConfig) {
          return;
      }

      const existingButton = siteConfig.getAnchorButton();
      if (!existingButton) {
          return;
      }

      const buttonContainer = getButtonContainer(existingButton);
      if (!buttonContainer || buttonContainer.querySelector(CUSTOM_BUTTON_SELECTOR)) {
          return;
      }

      const metadata = siteConfig.getMetadata();
      if (siteConfig.requireMp3Url !== false && !metadata.mp3Url) {
          return;
      }

      const params = new URLSearchParams({
          mp3_url: metadata.mp3Url,
          image_url: metadata.imageUrl || '',
          artist: metadata.artist || '',
          title: metadata.title || '',
          year: metadata.year || '',
          album: siteConfig.album,
          canonical: metadata.canonical || '',
          description: metadata.description || '',
          lyrics: metadata.lyrics || ''
      });

      const downloadUrl = `http://localhost:5000/api/download_ext?${params.toString()}`;
      const button = createDownloadButton(existingButton, downloadUrl);

      if (siteConfig.insertButton) {
          siteConfig.insertButton(buttonContainer, button, existingButton);
          return;
      }

      buttonContainer.insertBefore(button, existingButton.nextSibling);
  }

  function waitForExistingButton() {
      const siteConfig = getSiteConfig();
      if (!siteConfig) {
          return;
      }

      if (siteConfig.isReady()) {
          addDownloadButton();
          return;
      }

      const observer = new MutationObserver((mutations, obs) => {
          if (siteConfig.isReady()) {
              addDownloadButton();
              obs.disconnect();
          }
      });

      observer.observe(document.body, {
          childList: true,
          subtree: true
      });
  }

  waitForExistingButton();
  window.addEventListener('popstate', waitForExistingButton);
})();
