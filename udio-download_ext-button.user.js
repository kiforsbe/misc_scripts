// ==UserScript==
// @name         Add Download Button to Udio Song Pages
// @namespace    http://tampermonkey.net/
// @version      0.5
// @description  Adds a download button next to the existing download button on Udio song pages
// @match        https://www.udio.com/*
// @icon         https://udio.com/favicon.ico
// @run-at       document-idle
// ==/UserScript==

(function() {
    'use strict';

    // Function to extract metadata from the page
    function getMetadata(property) {
        const meta = document.querySelector(`meta[property="${property}"]`);
        return meta ? meta.getAttribute('content') : null;
    }

    // Function to get the canonical URL
    function getCanonicalUrl() {
        const link = document.querySelector('link[rel="canonical"]');
        return link ? link.getAttribute('href') : null;
    }

    // Function to get the creation year
    function getCreationYear() {
        const createdAtDiv = Array.from(document.querySelectorAll('div[title]'))
            .find(div => div.getAttribute('title').startsWith('Created at'));
        if (createdAtDiv) {
            const dateMatch = createdAtDiv.textContent.match(/\d{4}/);
            return dateMatch ? dateMatch[0] : null;
        }
        return null;
    }

    // Function to parse artist and title
    function parseArtistAndTitle(ogTitle) {
        if (!ogTitle) return { artist: null, title: null };
        const parts = ogTitle.split(' - ');
        if (parts.length < 2) return { artist: null, title: null };
        const artist = parts[0].trim();
        const title = parts[1].split(' | ')[0].trim();
        return { artist, title };
    }

    // Function to get lyrics
    function getLyrics() {
        const lyricsHeader = Array.from(document.querySelectorAll('div')).find(div => div.textContent.trim() === 'Lyrics');
        if (lyricsHeader && lyricsHeader.nextElementSibling) {
            return lyricsHeader.nextElementSibling.textContent.trim();
        }
        return null;
    }

    // Function to create and add the download button
    function addDownloadButton() {
        const existingButton = document.querySelector('button[title="Download media"]');
        if (!existingButton) {
            console.error('Existing download button not found');
            return;
        }

        const button = document.createElement('a');
        button.textContent = 'Download with Metadata';
        button.style.cssText = `
            padding: 10px 20px;
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            margin-left: 10px;
        `;

        const mp3Url = getMetadata('og:audio:url');
        const imageUrl = getMetadata('og:image');
        const ogTitle = getMetadata('og:title');
        const { artist, title } = parseArtistAndTitle(ogTitle);
        const year = getCreationYear();
        const canonical = getCanonicalUrl();
        const lyrics = getLyrics();

        const params = new URLSearchParams({
            mp3_url: mp3Url,
            image_url: imageUrl || '',
            artist: artist || '',
            title: title || '',
            year: year || '',
            canonical: canonical || '',
            lyrics: lyrics || ''
        });

        button.href = `http://localhost:5000/api/download_ext?${params.toString()}`;

        existingButton.parentNode.insertBefore(button, existingButton.nextSibling);
    }

    // Function to wait for the existing button to appear
    function waitForExistingButton() {
        const observer = new MutationObserver((mutations, obs) => {
            const existingButton = document.querySelector('button[title="Download media"]');
            if (existingButton && getCreationYear() && getLyrics()) {
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
    waitForExistingButton();
    window.addEventListener('popstate', waitForExistingButton);
})();