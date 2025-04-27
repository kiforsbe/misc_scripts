// ==UserScript==
// @name         Add Download Button to Riffusion Song Pages
// @namespace    http://tampermonkey.net/
// @version      0.7
// @description  Adds a download button on Riffusion song pages, styled to match the UI
// @match        https://www.riffusion.com/song/*
// @icon         https://www.riffusion.com/favicon.ico
// @run-at       document-idle
// ==/UserScript==

(function() {
  'use strict';

  // Function to find the VibeUseButton
  function getVibeUseButton() {
      return document.querySelector('[data-sentry-component="VibeUseButton"]');
  }

  // Function to extract metadata from the page
  function getMetadata(property) {
      const meta = document.querySelector(`meta[property="${property}"]`);
      return meta ? meta.getAttribute('content') : null;
  }

  // Function to get the canonical URL
  function getCanonicalUrl() {
      const link = document.querySelector('link[rel="canonical"]');
      return link ? link.getAttribute('href') : window.location.href;
  }

  // Function to parse title and artist
  function parseTitleAndArtist() {
      const ogTitle = getMetadata('og:title');
      if (!ogTitle) return { artist: null, title: null };
      
      const parts = ogTitle.split(' by ');
      if (parts.length < 2) return { artist: null, title: parts[0].trim() };
      
      return { 
          title: parts[0].trim(),
          artist: parts[1].trim()
      };
  }

  // Function to get year from relative or absolute date
  function getCreationYear() {
      // Try to find date info on the page
      const dateElements = Array.from(document.querySelectorAll('.text-secondary.text-sm'));
      for (const elem of dateElements) {
          const text = elem.textContent.trim();
          
          // If it's a relative date like "4 months ago"
          if (text.includes('ago')) {
              const currentDate = new Date();
              
              // Parse the time unit and value
              const match = text.match(/(\d+)\s+(\w+)\s+ago/);
              if (match) {
                  const value = parseInt(match[1], 10);
                  const unit = match[2].toLowerCase();
                  
                  // Subtract the appropriate amount of time
                  if (unit.includes('year') && value >= 1) {
                      return (currentDate.getFullYear() - value).toString();
                  } else if (unit.includes('month') && value >= 1) {
                      let year = currentDate.getFullYear();
                      let monthsAgo = value;
                      
                      // If months ago extends into previous year(s)
                      if (monthsAgo > currentDate.getMonth()) {
                          const yearsToSubtract = Math.floor(monthsAgo / 12);
                          year -= yearsToSubtract;
                      }
                      
                      return year.toString();
                  } else if (unit.includes('week') || unit.includes('day') || unit.includes('hour') || unit.includes('minute')) {
                      // For shorter time periods, use current year
                      return currentDate.getFullYear().toString();
                  }
              }
          }
          
          // If it contains a 4-digit year (e.g., "Jan 15, 2023")
          const yearMatch = text.match(/\b(19|20)\d{2}\b/);
          if (yearMatch) {
              return yearMatch[0];
          }
          
          // If it's a date without year format (e.g., "Jan 15")
          const monthMatch = text.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i);
          if (monthMatch) {
              return new Date().getFullYear().toString();
          }
      }
      
      // Fallback to current year if no date information found
      return new Date().getFullYear().toString();
  }

  // Function to get description/tags
  function getDescription() {
      return getMetadata('og:description') || '';
  }

  // Function to get lyrics with proper line breaks
  function getLyrics() {
      const lyricsView = document.querySelector('[data-sentry-component="LyricsView"]');
      if (!lyricsView) return '';
      
      // Clone the node to work with it without affecting the DOM
      const clone = lyricsView.cloneNode(true);
      let text = '';
      
      // Process all child nodes to handle text and <br> elements
      function processNode(node) {
          // For text nodes, add their content
          if (node.nodeType === Node.TEXT_NODE) {
              text += node.textContent;
          } 
          // For <br> elements, add a newline
          else if (node.nodeName === 'BR') {
              text += '\n';
          } 
          // For element nodes, process their children
          else if (node.nodeType === Node.ELEMENT_NODE) {
              // Handle block-level elements by adding newlines
              const style = window.getComputedStyle(node);
              const isBlock = style.display === 'block' || style.display === 'flex' || 
                              style.display === 'grid' || node.tagName === 'DIV' || 
                              node.tagName === 'P';
              
              // Add a newline before block elements if not at the start
              if (isBlock && text.length > 0 && !text.endsWith('\n')) {
                  text += '\n';
              }
              
              // Process all children recursively
              for (const child of node.childNodes) {
                  processNode(child);
              }
              
              // Add a newline after block elements
              if (isBlock && !text.endsWith('\n')) {
                  text += '\n';
              }
          }
      }
      
      // Start processing from the root
      for (const child of clone.childNodes) {
          processNode(child);
      }
      
      // Clean up multiple newlines and trim
      return text.replace(/\n{3,}/g, '\n\n').trim();
  }

  // Function to create and add the download button
  function addDownloadButton() {
      const vibeUseButton = getVibeUseButton();
      if (!vibeUseButton) {
          console.error('VibeUseButton not found');
          setTimeout(addDownloadButton, 1000); // Retry after 1 second
          return;
      }

      // Get the parent div that contains the buttons
      const buttonContainer = vibeUseButton.closest('.flex.h-10.flex-row');
      if (!buttonContainer) {
          console.error('Button container not found');
          return;
      }

      // Create button with the same style as other action buttons
      const button = document.createElement('button');
      button.className = 'aspect-square h-full rounded-full bg-secondary text-primary hover:bg-secondary-hover disabled:hover:bg-secondary transition-colors disabled:opacity-50';
      button.title = 'Download';
      
      // Create an SVG icon for the download button
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('aria-hidden', 'true');
      svg.setAttribute('focusable', 'false');
      svg.setAttribute('class', 'svg-inline--fa fa-download');
      svg.setAttribute('role', 'img');
      svg.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
      svg.setAttribute('viewBox', '0 0 512 512');
      
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('fill', 'currentColor');
      path.setAttribute('d', 'M288 32c0-17.7-14.3-32-32-32s-32 14.3-32 32V274.7l-73.4-73.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l128 128c12.5 12.5 32.8 12.5 45.3 0l128-128c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L288 274.7V32zM64 352c-35.3 0-64 28.7-64 64v32c0 35.3 28.7 64 64 64H448c35.3 0 64-28.7 64-64V416c0-35.3-28.7-64-64-64H346.5l-45.3 45.3c-25 25-65.5 25-90.5 0L165.5 352H64zm368 56a24 24 0 1 1 0 48 24 24 0 1 1 0-48z');
      
      svg.appendChild(path);
      button.appendChild(svg);

      const audioUrl = getMetadata('og:audio');
      const imageUrl = getMetadata('og:image');
      const { artist, title } = parseTitleAndArtist();
      const year = getCreationYear();
      const canonical = getCanonicalUrl();
      const description = getDescription();
      const lyrics = getLyrics();

      // Create a click event handler for the button
      button.addEventListener('click', function(e) {
          e.preventDefault();
          
          const params = new URLSearchParams({
              mp3_url: audioUrl || '',
              image_url: imageUrl || '',
              artist: artist || '',
              title: title || '',
              year: year || '',
              album: 'Riffusion',
              canonical: canonical || '',
              description: description || '',
              lyrics: lyrics || ''
          });
          
          const downloadUrl = `http://localhost:5000/api/download_ext?${params.toString()}`;
          window.open(downloadUrl);
      });

      // Insert after the VibeUseButton
      buttonContainer.insertBefore(button, vibeUseButton.nextSibling);
  }

  // Function to wait for the vibe use button to appear
  function waitForElements() {
      const observer = new MutationObserver((mutations, obs) => {
          const vibeUseButton = getVibeUseButton();
          const lyricsView = document.querySelector('[data-sentry-component="LyricsView"]');
          
          // Continue observing until both important elements are loaded
          if (vibeUseButton && (lyricsView !== null)) {
              addDownloadButton();
              obs.disconnect();
          }
      });

      observer.observe(document.body, {
          childList: true,
          subtree: true
      });
  }

  // Run the script
  if (window.location.href.match(/https:\/\/www\.riffusion\.com\/song\/.*/)) {
      waitForElements();
      // Also handle navigation changes
      window.addEventListener('popstate', waitForElements);
  }
})();
