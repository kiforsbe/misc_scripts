// ==UserScript==
// @name         Highlight Video Links
// @namespace    http://tampermonkey.net/
// @version      2025-01-26
// @description  Prevent Reddit from resetting video links
// @match        https://old.reddit.com/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=reddit.com
// ==/UserScript==

(function() {
    'use strict';

    function isRedditVideoLink(link) {
        return link.href && link.href.includes('v.redd.it');
    }

    function modifyVideoLink(element) {
        const parentDiv = element.closest('div[data-url*="v.redd.it"]');
        if (parentDiv) {
            const permalink = parentDiv.getAttribute('data-permalink');
            const dataURL = parentDiv.getAttribute('data-url');
            const hlsPlaylistUrl = `${dataURL}/HLSPlaylist.m3u8`;
            const titleDiv = parentDiv.querySelector('a.title');
            const title = titleDiv ? titleDiv.textContent.replace(/[/\\?%*:|"<>]/g, '-') : 'reddit-video';
            const id = parentDiv.getAttribute('id').replace('thing_t3_', '');

            const targetLink = `http://localhost:5000/convert?url=${hlsPlaylistUrl}&alt_url=${permalink}&video_id=${id}&title=${title}`;

            // Remove outbound link attributes
            element.removeAttribute('data-outbound-url');
            element.removeAttribute('data-outbound-expiration');

            // Completely remove click event listeners
            const oldElement = element;
            const newElement = oldElement.cloneNode(true);
            oldElement.parentNode.replaceChild(newElement, oldElement);

            // Set new href
            newElement.href = targetLink;

            // Visual highlight
            if (isRedditVideoLink(newElement)) {
                newElement.style.backgroundColor = 'yellow';
                newElement.style.color = 'black';
            }

            return newElement;
        }
        return element;
    }

    function updateRedditVideoLinks() {
        document.querySelectorAll('.thumbnail, a.title').forEach(modifyVideoLink);
    }

    // Initial run when page loads
    window.addEventListener('load', updateRedditVideoLinks);
})();