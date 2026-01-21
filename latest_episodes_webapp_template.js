// Latest Episodes Webapp JavaScript
//
// Navigation Features:
// - URL-based navigation with query parameters
// - Browser back/forward button support
// - Direct episode linking via URLs
// - Shareable episode links
// - Automatic scrolling to selected episodes
// - Filter state preservation in URLs
//
// URL Format: ?series=SeriesName&season=1&episode=5&search=term&watchStatus=watched
// Legacy Hash Format: #episode:SeriesName:1:5

class LatestEpisodesApp {
    constructor() {
        this.data = EPISODES_DATA;
        this.filteredEpisodes = [];
        this.selectedEpisode = null;
        this.searchTerm = '';
        this.watchStatusFilter = 'all';
        this.malStatusFilter = 'all';
        this.seriesFilter = 'all';
        this.groupBy = 'none';
        this.popupTimeout = null;
        this.hideTimeout = null;
        this.currentPopupIndex = -1;
        this.isNavigating = false; // Flag to prevent infinite loops during navigation
        this.init();
    }
    
    selectEpisodeByPath(filePath) {
        const episodeIndex = this.filteredEpisodes.findIndex(ep => ep.file_path === filePath);
        if (episodeIndex !== -1) {
            this.selectEpisode(episodeIndex);
            // Scroll to the selected episode in the list
            setTimeout(() => {
                const selectedItem = document.querySelector(`[data-index="${episodeIndex}"]`);
                if (selectedItem) {
                    selectedItem.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }
            }, 100);
        } else {
            // Episode not in current filtered view, need to find it in full dataset
            const fullEpisodeIndex = this.data.episodes.findIndex(ep => ep.file_path === filePath);
            if (fullEpisodeIndex !== -1) {
                // Clear filters to show the episode
                this.searchTerm = '';
                this.watchStatusFilter = 'all';
                this.malStatusFilter = 'all';
                this.seriesFilter = 'all';
                
                // Update UI
                document.getElementById('search-input').value = '';
                document.getElementById('watch-status-filter').value = 'all';
                document.getElementById('mal-status-filter').value = 'all';
                document.getElementById('series-filter').value = 'all';
                
                // Re-filter and find the episode
                this.filterAndDisplayEpisodes();
                const newIndex = this.filteredEpisodes.findIndex(ep => ep.file_path === filePath);
                if (newIndex !== -1) {
                    this.selectEpisode(newIndex);
                    // Scroll to the selected episode in the list
                    setTimeout(() => {
                        const selectedItem = document.querySelector(`[data-index="${newIndex}"]`);
                        if (selectedItem) {
                            selectedItem.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        }
                    }, 200);
                }
            }
        }
    }
    
    escapeHtml(text) {
        if (typeof text !== 'string') return text || '';
        return text.replace(/[&<>"']/g, match => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[match]));
    }
    
    init() {
        this.setupEventListeners();
        this.setupNavigation();
        this.updateHeaderStats();
        this.populateSeriesFilter();
        this.filterAndDisplayEpisodes();
        this.handleInitialNavigation();
    }
    
    setupEventListeners() {
        // Search functionality
        const searchInput = document.getElementById('search-input');
        searchInput.addEventListener('input', (e) => {
            this.searchTerm = e.target.value.toLowerCase();
            this.filterAndDisplayEpisodes();
        });
        
        // Watch status filter
        const watchStatusFilter = document.getElementById('watch-status-filter');
        watchStatusFilter.addEventListener('change', (e) => {
            this.watchStatusFilter = e.target.value;
            this.filterAndDisplayEpisodes();
        });
        
        // MAL status filter
        const malStatusFilter = document.getElementById('mal-status-filter');
        malStatusFilter.addEventListener('change', (e) => {
            this.malStatusFilter = e.target.value;
            this.filterAndDisplayEpisodes();
        });
        
        // Series filter
        const seriesFilter = document.getElementById('series-filter');
        seriesFilter.addEventListener('change', (e) => {
            this.seriesFilter = e.target.value;
            this.filterAndDisplayEpisodes();
        });
        
        // Group by filter
        const groupBySelect = document.getElementById('group-by-select');
        groupBySelect.addEventListener('change', (e) => {
            this.groupBy = e.target.value;
            this.filterAndDisplayEpisodes();
        });
        
        // Keyboard navigation
        document.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
                e.preventDefault();
                this.navigateList(e.key === 'ArrowDown' ? 1 : -1);
            }
        });
    }

    setupNavigation() {
        // Handle browser back/forward navigation
        window.addEventListener('popstate', (e) => {
            if (this.isNavigating) return;
            this.handleNavigation(e.state);
        });

        // Handle hash changes (for older browser compatibility)
        window.addEventListener('hashchange', (e) => {
            if (this.isNavigating) return;
            this.handleHashNavigation();
        });
    }
    
    updateHeaderStats() {
        const summary = this.data.summary;
        
        document.getElementById('total-episodes').textContent = summary.total_episodes;
        document.getElementById('unique-series').textContent = summary.unique_series;
        document.getElementById('watched-count').textContent = summary.watch_status_distribution.watched || 0;
        document.getElementById('date-range').textContent = summary.date_range_days;
    }
    
    populateSeriesFilter() {
        const seriesFilter = document.getElementById('series-filter');
        const seriesSet = new Set();
        
        this.data.episodes.forEach(episode => {
            const seriesTitle = episode.metadata.title;
            if (seriesTitle) {
                seriesSet.add(seriesTitle);
            }
        });
        
        const sortedSeries = Array.from(seriesSet).sort();
        sortedSeries.forEach(series => {
            const option = document.createElement('option');
            option.value = series;
            option.textContent = series;
            seriesFilter.appendChild(option);
        });
    }
    
    filterAndDisplayEpisodes() {
        this.filteredEpisodes = this.data.episodes.filter(episode => {
            // Search filter
            if (this.searchTerm) {
                const searchableText = [
                    episode.metadata.title,
                    episode.file_name,
                    episode.metadata.episode_title
                ].join(' ').toLowerCase();
                
                if (!searchableText.includes(this.searchTerm)) {
                    return false;
                }
            }
            
            // Watch status filter
            if (this.watchStatusFilter !== 'all') {
                const watchStatus = this.getEpisodeWatchStatus(episode);
                if (watchStatus !== this.watchStatusFilter) {
                    return false;
                }
            }
            
            // MAL status filter
            if (this.malStatusFilter !== 'all') {
                const malStatus = this.getEpisodeMalStatus(episode);
                if (malStatus !== this.malStatusFilter) {
                    return false;
                }
            }
            
            // Series filter
            if (this.seriesFilter !== 'all') {
                if (episode.metadata.title !== this.seriesFilter) {
                    return false;
                }
            }
            
            return true;
        });
        
        this.renderEpisodesList();
    }
    
    getEpisodeWatchStatus(episode) {
        const plex = episode.plex_watch_status;
        if (!plex) return 'unknown';
        
        if (plex.watched) return 'watched';
        if (plex.progress_percent > 0) return 'partially_watched';
        return 'not_watched';
    }
    
    getEpisodeMalStatus(episode) {
        const mal = episode.myanimelist_watch_status;
        if (!mal || !mal.my_status) return 'no_mal_data';
        
        return mal.my_status.toLowerCase().replace(' ', '_').replace('-', '_');
    }
    
    renderEpisodesList() {
        const container = document.getElementById('episodes-list');
        
        if (this.filteredEpisodes.length === 0) {
            container.innerHTML = `
                <div class="text-center p-4 text-muted">
                    <i class="bi bi-search"></i>
                    <p>No episodes match your filters</p>
                </div>
            `;
            return;
        }
        
        if (this.groupBy === 'series') {
            this.renderGroupedEpisodesList();
        } else {
            this.renderFlatEpisodesList();
        }
    }
    
    renderFlatEpisodesList() {
        const container = document.getElementById('episodes-list');
        container.innerHTML = this.filteredEpisodes.map((episode, index) => {
            return this.renderEpisodeItem(episode, index);
        }).join('');
    }
    
    renderGroupedEpisodesList() {
        const container = document.getElementById('episodes-list');
        
        // Group episodes by series
        const groupedEpisodes = this.filteredEpisodes.reduce((groups, episode, index) => {
            const seriesTitle = episode.metadata.title;
            if (!groups[seriesTitle]) {
                groups[seriesTitle] = [];
            }
            groups[seriesTitle].push({ episode, index });
            return groups;
        }, {});
        
        // Sort series by the latest episode download date (newest first)
        const sortedSeries = Object.keys(groupedEpisodes).sort((a, b) => {
            const latestEpisodeA = Math.max(...groupedEpisodes[a].map(item => item.episode.download_timestamp));
            const latestEpisodeB = Math.max(...groupedEpisodes[b].map(item => item.episode.download_timestamp));
            return latestEpisodeB - latestEpisodeA; // Newest first
        });
        
        let html = '';
        sortedSeries.forEach(seriesTitle => {
            const episodes = groupedEpisodes[seriesTitle];
            const firstEpisode = episodes[0].episode;
            const seriesRating = this.getSeriesGroupRating(firstEpisode);
            const totalEpisodes = firstEpisode.series_metadata && firstEpisode.series_metadata.total_episodes;
            const episodeCountText = totalEpisodes ? `${episodes.length}/${totalEpisodes} episodes` : `${episodes.length} episode${episodes.length !== 1 ? 's' : ''}`;
            const seriesTags = firstEpisode.series_metadata && firstEpisode.series_metadata.tags && firstEpisode.series_metadata.tags.length > 0 ? firstEpisode.series_metadata.tags : null;
            
            html += `
                <div class="series-group">
                    <div class="series-group-header">
                        <div class="series-group-main">
                            <div class="series-group-title">
                                <span>${this.escapeHtml(seriesTitle)}</span>
                            </div>
                            <div class="series-group-right">
                                ${seriesRating ? `<span class="series-rating">${seriesRating}</span>` : ''}
                                <span class="series-group-count">${episodeCountText}</span>
                            </div>
                        </div>
                        ${seriesTags ? `
                            <div class="series-tags">
                                ${seriesTags.slice(0, 5).map(tag => `<span class="series-tag">${this.escapeHtml(tag)}</span>`).join('')}
                                ${seriesTags.length > 5 ? `<span class="series-tag">+${seriesTags.length - 5}</span>` : ''}
                            </div>
                        ` : ''}
                    </div>
            `;
            
            episodes.forEach(({ episode, index }) => {
                html += this.renderEpisodeItem(episode, index);
            });
            
            html += '</div>';
        });
        
        container.innerHTML = html;
    }
    
    renderEpisodeItem(episode, index) {
        const watchStatus = this.getEpisodeWatchStatus(episode);
        const downloadDate = new Date(episode.download_date);
        const episodeTitle = episode.metadata.episode_title || `Episode ${episode.metadata.episode}`;
        const seriesEpisodes = this.getSeriesEpisodes(episode.metadata.title);
        const episodeCount = seriesEpisodes.length;
        const thumbnailData = this.getThumbnailData(episode);
        const staticUrl = thumbnailData && thumbnailData.static_thumbnail ? thumbnailData.static_thumbnail.replace(/\\/g, '/') : null;
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;
        
        return `
            <div class="episode-item ${watchStatus}" onclick="app.selectEpisode(${index})" data-index="${index}"
                 onmouseenter="app.showEpisodePopup(event, ${index})" onmouseleave="app.hideEpisodePopup()">
                <div class="episode-thumbnail">
                    ${staticUrl ? 
                        `<img src="file:///${staticUrl}" alt="Episode thumbnail" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width: 100%; height: 100%; border-radius: 0.375rem;" onmouseenter="if(this.dataset.animUrl) this.src='file:///${animUrl}'" onmouseleave="if(this.dataset.animUrl) this.src='file:///${staticUrl}'" data-anim-url="${animUrl || ''}">` : 
                        ''}
                    <div style="${staticUrl ? 'display: none;' : 'display: flex;'} width: 100%; height: 100%; align-items: center; justify-content: center;">
                        ${episode.metadata.episode || '?'}
                    </div>
                </div>
                <div class="episode-content">
                    <div class="episode-series">
                        ${this.groupBy === 'series' ? '' : this.escapeHtml(episode.metadata.title)}
                    </div>
                    <!--- <div class="episode-title">
                        ${this.escapeHtml(episodeTitle)}
                    </div> --->
                    <div class="episode-meta">
                        <span>Episode ${episode.metadata.episode}${this.getEpisodeCountDisplay(episode, episodeCount)}</span>
                        <span>${this.formatFileSize(episode.file_size)}</span>
                    </div>
                    ${this.renderRatingInfo(episode, 'compact')}
                    <div class="episode-date">${downloadDate.toLocaleDateString()}</div>
                </div>
            </div>
        `;
    }
    
    selectEpisode(index, updateUrl = true) {
        // Update visual selection
        document.querySelectorAll('.episode-item').forEach(item => {
            item.classList.remove('selected');
        });

        const selectedItem = document.querySelector(`[data-index="${index}"]`);
        if (selectedItem) {
            selectedItem.classList.add('selected');
            this.selectedEpisode = this.filteredEpisodes[index];
            this.renderEpisodeDetails();
            
            // Update URL and browser history
            if (updateUrl && !this.isNavigating) {
                this.updateUrlForEpisode(this.selectedEpisode);
            }
        }
    }
    
    renderEpisodeDetails() {
        if (!this.selectedEpisode) return;

        const episode = this.selectedEpisode;
        const container = document.getElementById('episode-details');

        const downloadDate = new Date(episode.download_date);
        const episodeTitle = episode.metadata.episode_title || `Episode ${episode.metadata.episode}`;
        const seriesTitle = episode.metadata.title;
        const thumbnailData = this.getThumbnailData(episode);
        const staticUrl = thumbnailData && thumbnailData.static_thumbnail ? thumbnailData.static_thumbnail.replace(/\\/g, '/') : null;
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;

        // Plex link logic
        let plexLinkHtml = '';
        const plex = episode.plex_watch_status;
        if (plex && plex.server_hash && plex.metadata_item_id) {
            const plexUrl = `http://127.0.0.1:32400/web/index.html#!/server/${plex.server_hash}/details?key=%2Flibrary%2Fmetadata%2F${plex.metadata_item_id}`;
            plexLinkHtml = `<a class="plex-link external-link" href="${plexUrl}" target="_blank" rel="noopener" title="Open in Plex">
                <span class="plex-icon"><svg width="1em" height="1em" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;"><circle cx="16" cy="16" r="16" fill="#282a2d"/><path d="M12.5 8v16l10-8-10-8z" fill="#e5a00d"/></svg></span>
                Plex
            </a>`;
        }

        let detailsHtml = `
            <div class="details-header" style="position: relative;">
                ${staticUrl ? `
                    <div class="episode-detail-thumbnail">
                        <img src="file:///${staticUrl}" alt="Episode thumbnail" 
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" 
                             onmouseenter="if(this.dataset.animUrl) this.src='file:///${animUrl}'" 
                             onmouseleave="if(this.dataset.animUrl) this.src='file:///${staticUrl}'" 
                             data-anim-url="${animUrl || ''}">
                        <div style="display: none; width: 100%; height: 100%; align-items: center; justify-content: center;">
                            ${episode.metadata.episode || '?'}
                        </div>
                    </div>
                ` : ''}
                <h2 class="details-title">${this.escapeHtml(seriesTitle)}</h2>
                <p class="details-subtitle">Season ${episode.metadata.season || 1}, Episode ${episode.metadata.episode}</p>
                ${episodeTitle !== `Episode ${episode.metadata.episode}` ? `<p class="episode-subtitle">${this.escapeHtml(episodeTitle)}</p>` : ''}
                ${plexLinkHtml}
            </div>
            
            <div class="details-grid">
                <div class="details-card">
                    <h4 class="card-title">
                        <i class="bi bi-info-circle"></i>
                        Episode Information
                    </h4>
                    <p><strong>Downloaded:</strong> ${downloadDate.toLocaleString()}</p>
                    <p><strong>File Size:</strong> ${this.formatFileSize(episode.file_size)}</p>
                    <p><strong>Episode:</strong> S${episode.metadata.season || 1}E${episode.metadata.episode}</p>
        `;
        
        // Add episode metadata if available
        if (episode.episode_metadata) {
            const ep = episode.episode_metadata;
            if (ep.air_date) {
                const airDate = new Date(ep.air_date);
                detailsHtml += `<p><strong>Air Date:</strong> ${airDate.toLocaleDateString()}</p>`;
            }
            if (ep.plot) {
                detailsHtml += `<p><strong>Plot:</strong> ${this.escapeHtml(ep.plot)}</p>`;
            }
        }
        
        // Add rating information
        detailsHtml += this.renderRatingInfo(episode, 'detailed');
        
        detailsHtml += `</div>`;
        
        // Watch Status Card
        detailsHtml += `
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-eye"></i>
                    Watch Status
                </h4>
        `;
        
        // Plex watch status
        if (episode.plex_watch_status) {
            const plex = episode.plex_watch_status;
            detailsHtml += `
                <p><strong>Plex Status:</strong> ${plex.watched ? 'Watched' : 'Not Watched'}</p>
                ${plex.progress_percent > 0 ? `
                    <div class="progress-container">
                        <div class="progress">
                            <div class="progress-bar" style="width: ${plex.progress_percent}%"></div>
                        </div>
                        <small>${plex.progress_percent.toFixed(1)}% watched</small>
                    </div>
                ` : ''}
                ${plex.last_watched ? `<p><strong>Last Watched:</strong> ${new Date(plex.last_watched).toLocaleString()}</p>` : ''}
            `;
            // Plex link button
            if (plex.server_hash && plex.metadata_item_id) {
                const plexUrl = `http://127.0.0.1:32400/web/index.html#!/server/${plex.server_hash}/details?key=%2Flibrary%2Fmetadata%2F${plex.metadata_item_id}`;
                detailsHtml += `<div class="external-links" style="margin-top:0.5rem;"><a class="plex-link external-link" href="${plexUrl}" target="_blank" rel="noopener" title="Open in Plex"><span class="plex-icon"><svg width="1em" height="1em" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;"><circle cx="16" cy="16" r="16" fill="#282a2d"/><path d="M12.5 8v16l10-8-10-8z" fill="#e5a00d"/></svg></span>Plex</a></div>`;
            }
        } else {
            detailsHtml += `<p>No Plex watch data available</p>`;
        }
        
        detailsHtml += `</div>`;
        
        // Series Information Card
        if (episode.series_metadata) {
            const series = episode.series_metadata;
            detailsHtml += `
                <div class="details-card">
                    <h4 class="card-title">
                        <i class="bi bi-collection-play"></i>
                        Series Information
                    </h4>
                    <p><strong>Title:</strong> ${this.escapeHtml(series.title)}</p>
                    ${series.year ? `<p><strong>Year:</strong> ${series.year}</p>` : ''}
                    ${this.renderSeriesRatingInfo(episode)}
                    ${series.total_episodes ? `<p><strong>Total Episodes:</strong> ${series.total_episodes}</p>` : ''}
                    ${series.genres && series.genres.length > 0 ? `<p><strong>Genres:</strong> ${series.genres.join(', ')}</p>` : ''}
                    ${series.tags && series.tags.length > 0 ? this.renderTagsWithPopup(series.tags) : ''}
                    ${series.plot ? `<p><strong>Plot:</strong> ${this.escapeHtml(series.plot)}</p>` : ''}
            `;
            
            // External link section
            if (episode.source_url) {
                // Determine the source type from URL
                const isMAL = episode.source_url.includes('myanimelist.net');
                const isIMDB = episode.source_url.includes('imdb.com');
                const linkClass = isMAL ? 'mal-link' : (isIMDB ? 'imdb-link' : 'external-link');
                const linkText = isMAL ? 'MyAnimeList' : (isIMDB ? 'IMDb' : 'View Source');
                
                detailsHtml += `
                    <div class="external-links">
                        <a href="${episode.source_url}" target="_blank" class="external-link ${linkClass}">
                            <i class="bi bi-box-arrow-up-right"></i>
                            ${linkText}
                        </a>
                    </div>
                `;
            }
            
            detailsHtml += `</div>`;
        }
        
        // MyAnimeList Watch Status Card
        if (episode.myanimelist_watch_status) {
            const mal = episode.myanimelist_watch_status;
            detailsHtml += `
                <div class="details-card">
                    <h4 class="card-title">
                        <i class="bi bi-list-check"></i>
                        MyAnimeList Status
                    </h4>
                    <span class="status-badge status-${mal.my_status.toLowerCase().replace(' ', '-').replace(' ', '_')}">${mal.my_status}</span>
                    <p><strong>Episodes Watched:</strong> ${mal.my_watched_episodes} / ${mal.series_episodes || '?'}</p>
                    ${mal.my_score > 0 ? `<p><strong>Your Score:</strong> ${parseFloat(mal.my_score).toFixed(1)}/10</p>` : ''}
                    ${mal.progress_percent > 0 ? `
                        <div class="progress-container">
                            <div class="progress">
                                <div class="progress-bar" style="width: ${mal.progress_percent}%"></div>
                            </div>
                            <small>${mal.progress_percent.toFixed(1)}% completed</small>
                        </div>
                    ` : ''}
                </div>
            `;
        }
        
        detailsHtml += `</div>`;
        
        // Series Episodes Section
        const seriesEpisodes = this.getSeriesEpisodes(episode.metadata.title);
        if (seriesEpisodes.length > 1) {
            detailsHtml += `
                <div class="series-episodes">
                    <h4><i class="bi bi-collection-play"></i> Other Episodes in Series</h4>
                    <div class="series-episodes-grid">
            `;
            
            seriesEpisodes.forEach(ep => {
                const isCurrentEpisode = ep.file_path === episode.file_path;
                const epWatchStatus = this.getEpisodeWatchStatus(ep);
                const epThumbnailData = this.getThumbnailData(ep);
                const epStaticUrl = epThumbnailData && epThumbnailData.static_thumbnail ? epThumbnailData.static_thumbnail.replace(/\\/g, '/') : null;
                const epAnimUrl = epThumbnailData && epThumbnailData.animated_thumbnail ? epThumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;
                const epTitle = ep.metadata.episode_title || `Episode ${ep.metadata.episode}`;
                const epDownloadDate = new Date(ep.download_date);
                
                detailsHtml += `
                    <div class="series-episode-card ${isCurrentEpisode ? 'current' : ''}" 
                         onclick="${isCurrentEpisode ? '' : `app.selectEpisodeByPath('${ep.file_path.replace(/'/g, "\\\'")}'); event.stopPropagation();`}" 
                         style="${isCurrentEpisode ? '' : 'cursor: pointer;'}">
                        <div class="series-episode-thumbnail">
                            ${epStaticUrl ? 
                                `<img src="file:///${epStaticUrl}" alt="Episode thumbnail" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width: 100%; height: 100%; border-radius: 0.25rem;" onmouseenter="if(this.dataset.animUrl) this.src='file:///${epAnimUrl}'" onmouseleave="if(this.dataset.animUrl) this.src='file:///${epStaticUrl}'" data-anim-url="${epAnimUrl || ''}">` : 
                                ''}
                            <div style="${epStaticUrl ? 'display: none;' : 'display: flex;'} width: 100%; height: 100%; align-items: center; justify-content: center;">
                                ${ep.metadata.episode || '?'}
                            </div>
                        </div>
                        <div class="series-episode-info">
                            <div class="series-episode-title">
                                <span class="watch-indicator ${epWatchStatus}" style="margin-right: 0.25rem;"></span>
                                ${this.escapeHtml(epTitle)}
                            </div>
                            <div class="series-episode-meta">
                                S${ep.metadata.season || 1}E${ep.metadata.episode} • ${epDownloadDate.toLocaleDateString()}
                            </div>
                        </div>
                    </div>
                `;
            });
            
            detailsHtml += `
                    </div>
                </div>
            `;
        }
        
        // File Information
        detailsHtml += `
            <div class="file-info">
                <h5><i class="bi bi-file-play"></i> File Information</h5>
                <div class="file-path">${this.escapeHtml(episode.file_path)}</div>
                <div style="margin-top: 1rem;">
                    <button class="btn btn-outline-primary btn-sm" onclick="app.copyEpisodeUrl()" title="Copy shareable link to this episode">
                        <i class="bi bi-share"></i> Share Episode
                    </button>
                </div>
            </div>
        `;
        
        container.innerHTML = detailsHtml;
    }
    
    navigateList(direction) {
        if (this.filteredEpisodes.length === 0) return;
        
        let currentIndex = -1;
        if (this.selectedEpisode) {
            currentIndex = this.filteredEpisodes.findIndex(ep => ep.file_path === this.selectedEpisode.file_path);
        }
        
        let newIndex = currentIndex + direction;
        if (newIndex < 0) newIndex = this.filteredEpisodes.length - 1;
        if (newIndex >= this.filteredEpisodes.length) newIndex = 0;
        
        this.selectEpisode(newIndex);
        this.scrollToEpisode(newIndex);
    }
    
    getThumbnailData(episode) {
        // Build thumbnail map if not already done
        if (!this.thumbnailMap) {
            this.thumbnailMap = {};
            if (this.data.thumbnails && Array.isArray(this.data.thumbnails)) {
                this.data.thumbnails.forEach(thumb => {
                    if (thumb && thumb.video) {
                        this.thumbnailMap[thumb.video] = thumb;
                    }
                });
            }
        }
        
        // Try to find thumbnail by exact path match
        const filePath = episode.file_path;
        if (this.thumbnailMap[filePath]) {
            return this.thumbnailMap[filePath];
        }
        
        // Try to match by basename (fallback)
        const basename = filePath.split(/[/\\]/).pop();
        for (const path in this.thumbnailMap) {
            if (path.split(/[/\\]/).pop() === basename) {
                return this.thumbnailMap[path];
            }
        }
        
        return null;
    }
    
    getSeriesEpisodes(seriesTitle) {
        return this.data.episodes.filter(ep => ep.metadata.title === seriesTitle)
            .sort((a, b) => {
                const aSeason = a.metadata.season || 1;
                const bSeason = b.metadata.season || 1;
                if (aSeason !== bSeason) return aSeason - bSeason;
                return (a.metadata.episode || 0) - (b.metadata.episode || 0);
            });
    }
    
    getEpisodeCountDisplay(episode, availableCount) {
        const totalEpisodes = episode.series_metadata && episode.series_metadata.total_episodes;
        if (availableCount > 1) {
            if (totalEpisodes && totalEpisodes !== availableCount) {
                return ` of ${availableCount} (${totalEpisodes} total)`;
            } else {
                return ` of ${availableCount}`;
            }
        }
        return totalEpisodes && totalEpisodes > 1 ? ` (${totalEpisodes} total)` : '';
    }
    
    renderTagsWithPopup(tags, maxVisible = 6) {
        if (!tags || tags.length === 0) return '';
        
        const visibleTags = tags.slice(0, maxVisible);
        const hiddenTags = tags.slice(maxVisible);
        const hiddenCount = hiddenTags.length;
        
        const visibleTagsText = visibleTags.join(', ');
        const hiddenTagsText = hiddenTags.join(', ');
        
        if (tags.length <= maxVisible) {
            return `<p><strong>Tags:</strong> ${this.escapeHtml(visibleTagsText)}</p>`;
        }
        
        return `
            <p><strong>Tags:</strong> 
                ${this.escapeHtml(visibleTagsText)}${hiddenCount > 0 ? `, <span class="tags-more" onmouseover="app.showTagsPopup(event, '${this.escapeHtml(hiddenTagsText).replace(/'/g, "\\'")}', ${hiddenCount})" onmouseout="app.hideTagsPopup()" style="color: var(--primary-color); cursor: help; text-decoration: underline;">+${hiddenCount} more</span>` : ''}
            </p>
        `;
    }
    
    // Rating display functions
    renderRatingInfo(episode, style = 'compact') {
        let ratingHtml = '';
        const hasEpisodeRating = episode.episode_metadata && episode.episode_metadata.rating && episode.episode_metadata.rating > 0;
        const hasUserScore = episode.myanimelist_watch_status && episode.myanimelist_watch_status.my_score && episode.myanimelist_watch_status.my_score > 0;
        
        if (!hasEpisodeRating && !hasUserScore) return '';
        
        if (style === 'compact') {
            const ratings = [];
            if (hasEpisodeRating) {
                const communityScore = parseFloat(episode.episode_metadata.rating).toFixed(1);
                ratings.push(`⭐ ${communityScore}/10`);
            }
            if (hasUserScore) {
                const userScore = parseFloat(episode.myanimelist_watch_status.my_score).toFixed(1);
                ratings.push(`👤 ${userScore}/10`);
            }
            if (ratings.length > 0) {
                ratingHtml = `<div class="episode-ratings">${ratings.join(' • ')}</div>`;
            }
        } else if (style === 'detailed') {
            if (hasEpisodeRating) {
                const communityScore = parseFloat(episode.episode_metadata.rating).toFixed(1);
                ratingHtml += `<p><strong>Rating:</strong> <span class="community-score">⭐ ${communityScore}/10</span></p>`;
            }
            if (hasUserScore) {
                const userScore = parseFloat(episode.myanimelist_watch_status.my_score).toFixed(1);
                ratingHtml += `<p><strong>Your Score:</strong> <span class="user-score">👤 ${userScore}/10</span></p>`;
            }
        }
        
        return ratingHtml;
    }
    
    renderSeriesRatingInfo(episode) {
        let ratingHtml = '';
        const hasCommunityRating = episode.series_metadata && episode.series_metadata.rating && episode.series_metadata.rating > 0;
        const hasUserScore = episode.myanimelist_watch_status && episode.myanimelist_watch_status.my_score && episode.myanimelist_watch_status.my_score > 0;
        
        if (hasCommunityRating) {
            const communityScore = parseFloat(episode.series_metadata.rating).toFixed(1);
            ratingHtml += `<p><strong>Rating:</strong> <span class="community-score">⭐ ${communityScore}/10</span></p>`;
        }
        if (hasUserScore) {
            const userScore = parseFloat(episode.myanimelist_watch_status.my_score).toFixed(1);
            ratingHtml += `<p><strong>Your Score:</strong> <span class="user-score">👤 ${userScore}/10</span></p>`;
        }
        
        return ratingHtml;
    }
    
    getSeriesGroupRating(episode) {
        const ratings = [];
        if (episode.series_metadata && episode.series_metadata.rating && episode.series_metadata.rating > 0) {
            const communityScore = parseFloat(episode.series_metadata.rating).toFixed(1);
            ratings.push(`⭐${communityScore}`);
        }
        if (episode.myanimelist_watch_status && episode.myanimelist_watch_status.my_score && episode.myanimelist_watch_status.my_score > 0) {
            const userScore = parseFloat(episode.myanimelist_watch_status.my_score).toFixed(1);
            ratings.push(`👤${userScore}`);
        }
        return ratings.length > 0 ? ratings.join(' ') : null;
    }

    // Navigation methods
    updateUrlForEpisode(episode) {
        if (!episode) return;
        
        const urlParams = new URLSearchParams();
        
        // Add episode identification
        urlParams.set('series', encodeURIComponent(episode.metadata.title));
        urlParams.set('season', episode.metadata.season || 1);
        urlParams.set('episode', episode.metadata.episode);
        
        // Add current filters to maintain state
        if (this.searchTerm) urlParams.set('search', this.searchTerm);
        if (this.watchStatusFilter !== 'all') urlParams.set('watchStatus', this.watchStatusFilter);
        if (this.malStatusFilter !== 'all') urlParams.set('malStatus', this.malStatusFilter);
        if (this.seriesFilter !== 'all') urlParams.set('seriesFilter', this.seriesFilter);
        if (this.groupBy !== 'none') urlParams.set('groupBy', this.groupBy);
        
        const newUrl = `${window.location.pathname}?${urlParams.toString()}`;
        const state = {
            series: episode.metadata.title,
            season: episode.metadata.season || 1,
            episode: episode.metadata.episode,
            filters: {
                search: this.searchTerm,
                watchStatus: this.watchStatusFilter,
                malStatus: this.malStatusFilter,
                seriesFilter: this.seriesFilter,
                groupBy: this.groupBy
            }
        };
        
        this.isNavigating = true;
        history.pushState(state, '', newUrl);
        this.isNavigating = false;
    }
    
    handleInitialNavigation() {
        const urlParams = new URLSearchParams(window.location.search);
        const hash = window.location.hash;
        
        // Handle URL parameters
        if (urlParams.has('series') && urlParams.has('episode')) {
            this.applyFiltersFromUrl(urlParams);
            this.navigateToEpisodeFromUrl(urlParams);
        }
        // Handle hash-based navigation (legacy support)
        else if (hash) {
            this.handleHashNavigation();
        }
    }
    
    handleNavigation(state) {
        if (!state) {
            // No state, just reset to default view
            this.resetToDefaultView();
            return;
        }
        
        this.isNavigating = true;
        
        // Apply filters from state
        if (state.filters) {
            this.applyFilters(state.filters);
        }
        
        // Navigate to specific episode
        if (state.series && state.episode) {
            this.navigateToEpisode(state.series, state.season || 1, state.episode);
        }
        
        this.isNavigating = false;
    }
    
    handleHashNavigation() {
        const hash = window.location.hash.substring(1); // Remove #
        if (!hash) return;
        
        // Parse hash format: #episode:title:season:episode
        const parts = hash.split(':');
        if (parts.length >= 4 && parts[0] === 'episode') {
            const series = decodeURIComponent(parts[1]);
            const season = parseInt(parts[2]) || 1;
            const episode = parseInt(parts[3]);
            
            this.navigateToEpisode(series, season, episode);
        }
    }
    
    applyFiltersFromUrl(urlParams) {
        // Apply filters from URL parameters
        if (urlParams.has('search')) {
            this.searchTerm = urlParams.get('search');
            document.getElementById('search-input').value = this.searchTerm;
        }
        if (urlParams.has('watchStatus')) {
            this.watchStatusFilter = urlParams.get('watchStatus');
            document.getElementById('watch-status-filter').value = this.watchStatusFilter;
        }
        if (urlParams.has('malStatus')) {
            this.malStatusFilter = urlParams.get('malStatus');
            document.getElementById('mal-status-filter').value = this.malStatusFilter;
        }
        if (urlParams.has('seriesFilter')) {
            this.seriesFilter = urlParams.get('seriesFilter');
            document.getElementById('series-filter').value = this.seriesFilter;
        }
        if (urlParams.has('groupBy')) {
            this.groupBy = urlParams.get('groupBy');
            document.getElementById('group-by-select').value = this.groupBy;
        }
        
        // Re-filter episodes with new criteria
        this.filterAndDisplayEpisodes();
    }
    
    applyFilters(filters) {
        this.searchTerm = filters.search || '';
        this.watchStatusFilter = filters.watchStatus || 'all';
        this.malStatusFilter = filters.malStatus || 'all';
        this.seriesFilter = filters.seriesFilter || 'all';
        this.groupBy = filters.groupBy || 'none';
        
        // Update UI controls
        document.getElementById('search-input').value = this.searchTerm;
        document.getElementById('watch-status-filter').value = this.watchStatusFilter;
        document.getElementById('mal-status-filter').value = this.malStatusFilter;
        document.getElementById('series-filter').value = this.seriesFilter;
        document.getElementById('group-by-select').value = this.groupBy;
        
        // Re-filter episodes
        this.filterAndDisplayEpisodes();
    }
    
    navigateToEpisodeFromUrl(urlParams) {
        const series = decodeURIComponent(urlParams.get('series'));
        const season = parseInt(urlParams.get('season')) || 1;
        const episode = parseInt(urlParams.get('episode'));
        
        this.navigateToEpisode(series, season, episode);
    }
    
    navigateToEpisode(seriesTitle, season, episodeNumber) {
        // Find the episode in filtered results
        const episodeIndex = this.filteredEpisodes.findIndex(ep => 
            ep.metadata.title === seriesTitle && 
            (ep.metadata.season || 1) === season && 
            ep.metadata.episode === episodeNumber
        );
        
        if (episodeIndex !== -1) {
            // Episode found in filtered results
            this.selectEpisode(episodeIndex, false); // Don't update URL again
            this.scrollToEpisode(episodeIndex);
        } else {
            // Episode not found, might be filtered out
            // Try to find in all episodes
            const allEpisodeIndex = this.data.episodes.findIndex(ep => 
                ep.metadata.title === seriesTitle && 
                (ep.metadata.season || 1) === season && 
                ep.metadata.episode === episodeNumber
            );
            
            if (allEpisodeIndex !== -1) {
                // Episode exists but is filtered out, clear filters and try again
                this.resetFilters();
                this.filterAndDisplayEpisodes();
                
                // Try to find again after clearing filters
                const newIndex = this.filteredEpisodes.findIndex(ep => 
                    ep.metadata.title === seriesTitle && 
                    (ep.metadata.season || 1) === season && 
                    ep.metadata.episode === episodeNumber
                );
                
                if (newIndex !== -1) {
                    this.selectEpisode(newIndex, false);
                    this.scrollToEpisode(newIndex);
                }
            }
        }
    }
    
    scrollToEpisode(index) {
        // Wait for DOM to be updated, then scroll
        setTimeout(() => {
            const episodeItem = document.querySelector(`[data-index="${index}"]`);
            if (episodeItem) {
                const episodesList = document.getElementById('episodes-list');
                const itemRect = episodeItem.getBoundingClientRect();
                const listRect = episodesList.getBoundingClientRect();
                
                // Calculate scroll position to center the episode
                const scrollTop = episodesList.scrollTop + itemRect.top - listRect.top - (listRect.height / 2) + (itemRect.height / 2);
                
                episodesList.scrollTo({
                    top: Math.max(0, scrollTop),
                    behavior: 'smooth'
                });
            }
        }, 100);
    }
    
    resetFilters() {
        this.searchTerm = '';
        this.watchStatusFilter = 'all';
        this.malStatusFilter = 'all';
        this.seriesFilter = 'all';
        this.groupBy = 'none';
        
        // Update UI
        document.getElementById('search-input').value = '';
        document.getElementById('watch-status-filter').value = 'all';
        document.getElementById('mal-status-filter').value = 'all';
        document.getElementById('series-filter').value = 'all';
        document.getElementById('group-by-select').value = 'none';
    }
    
    resetToDefaultView() {
        this.resetFilters();
        this.filterAndDisplayEpisodes();
        
        // Clear selection
        document.querySelectorAll('.episode-item').forEach(item => {
            item.classList.remove('selected');
        });
        this.selectedEpisode = null;
        
        // Show welcome message
        const container = document.getElementById('episode-details');
        container.innerHTML = `
            <div class="welcome-message">
                <i class="bi bi-play-circle display-1 text-muted"></i>
                <h3 class="text-muted">Select an episode to view details</h3>
                <p class="text-muted">Choose an episode from the list on the left to see detailed information about the episode and series.</p>
            </div>
        `;
    }
    
    generateShareableLink(episode) {
        if (!episode) return window.location.origin + window.location.pathname;
        
        const urlParams = new URLSearchParams();
        urlParams.set('series', encodeURIComponent(episode.metadata.title));
        urlParams.set('season', episode.metadata.season || 1);
        urlParams.set('episode', episode.metadata.episode);
        
        return `${window.location.origin}${window.location.pathname}?${urlParams.toString()}`;
    }
    
    copyEpisodeUrl() {
        if (!this.selectedEpisode) return;
        
        const url = this.generateShareableLink(this.selectedEpisode);
        
        if (navigator.clipboard) {
            navigator.clipboard.writeText(url).then(() => {
                this.showToast('Episode link copied to clipboard!', 'success');
            }).catch(() => {
                this.fallbackCopyToClipboard(url);
            });
        } else {
            this.fallbackCopyToClipboard(url);
        }
    }
    
    fallbackCopyToClipboard(text) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        textArea.style.top = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        
        try {
            document.execCommand('copy');
            this.showToast('Episode link copied to clipboard!', 'success');
        } catch (err) {
            this.showToast('Failed to copy link. URL: ' + text, 'error');
        }
        
        document.body.removeChild(textArea);
    }
    
    showToast(message, type = 'info') {
        // Remove any existing toast
        const existingToast = document.getElementById('app-toast');
        if (existingToast) {
            existingToast.remove();
        }
        
        // Create toast element
        const toast = document.createElement('div');
        toast.id = 'app-toast';
        toast.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 6px;
            color: white;
            font-size: 14px;
            font-weight: 500;
            z-index: 2000;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
            transition: all 0.3s ease;
            transform: translateX(100%);
        `;
        
        // Set background color based on type
        const colors = {
            success: '#28a745',
            error: '#dc3545',
            info: '#17a2b8',
            warning: '#ffc107'
        };
        toast.style.backgroundColor = colors[type] || colors.info;
        
        toast.textContent = message;
        document.body.appendChild(toast);
        
        // Animate in
        setTimeout(() => {
            toast.style.transform = 'translateX(0)';
        }, 10);
        
        // Auto remove after 3 seconds
        setTimeout(() => {
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }, 3000);
    }

    // Utility functions
    formatFileSize(bytes) {
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = bytes;
        let unitIndex = 0;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return `${size.toFixed(1)} ${units[unitIndex]}`;
    }
    
    showTagsPopup(event, hiddenTags, hiddenCount) {
        // Remove any existing tooltip
        this.hideTagsPopup();
        
        // Create tooltip element
        const tooltip = document.createElement('div');
        tooltip.id = 'tags-tooltip-popup';
        tooltip.style.cssText = `
            position: fixed;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 0.8rem;
            z-index: 1000;
            max-width: 400px;
            line-height: 1.3;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
            pointer-events: none;
        `;
        
        tooltip.innerHTML = `<strong>Additional Tags (${hiddenCount}):</strong><br>${hiddenTags}`;
        
        // Position tooltip
        const rect = event.target.getBoundingClientRect();
        tooltip.style.left = Math.min(rect.left, window.innerWidth - 420) + 'px';
        tooltip.style.top = (rect.top - 10) + 'px';
        tooltip.style.transform = 'translateY(-100%)';
        
        document.body.appendChild(tooltip);
    }
    
    hideTagsPopup() {
        const existing = document.getElementById('tags-tooltip-popup');
        if (existing) {
            existing.remove();
        }
    }
    
    showEpisodePopup(event, index) {
        // Clear any pending hide timeout
        if (this.hideTimeout) {
            clearTimeout(this.hideTimeout);
            this.hideTimeout = null;
        }
        
        if (this.popupTimeout) {
            clearTimeout(this.popupTimeout);
        }
        
        const episode = this.filteredEpisodes[index];
        if (!episode) return;
        
        const popup = document.getElementById('episode-hover-popup');
        const isAlreadyVisible = popup.classList.contains('show');
        const isSameEpisode = this.currentPopupIndex === index;
        
        // If it's the same episode, don't do anything
        if (isSameEpisode && isAlreadyVisible) {
            return;
        }
        
        // Update current popup index
        this.currentPopupIndex = index;
        
        // If popup is already showing, use much faster transition
        if (isAlreadyVisible) {
            popup.style.transition = 'all 0.04s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
        } else {
            popup.style.transition = 'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
        }
        
        const thumbnailImg = document.getElementById('popup-thumbnail-img');
        const thumbnailPlaceholder = document.getElementById('popup-thumbnail-placeholder');
        const arrow = popup.querySelector('.popup-arrow');
        
        // Populate popup content
        document.getElementById('popup-title').textContent = episode.metadata.title;
        const episodeTitle = episode.metadata.episode_title || `Episode ${episode.metadata.episode}`;
        document.getElementById('popup-episode').textContent = `Season ${episode.metadata.season || 1}, Episode ${episode.metadata.episode}${episodeTitle !== `Episode ${episode.metadata.episode}` ? ` - ${episodeTitle}` : ''}`;
        document.getElementById('popup-download-date').textContent = new Date(episode.download_date).toLocaleDateString();
        document.getElementById('popup-file-size').textContent = this.formatFileSize(episode.file_size);
        
        // Set watch status
        const watchStatus = this.getEpisodeWatchStatus(episode);
        const statusIndicator = document.getElementById('popup-status-indicator');
        const statusText = document.getElementById('popup-status-text');
        
        statusIndicator.className = `popup-status-indicator ${watchStatus}`;
        const statusLabels = {
            watched: 'Watched',
            partially_watched: 'Partially Watched',
            not_watched: 'Not Watched',
            unknown: 'Unknown Status'
        };
        statusText.textContent = statusLabels[watchStatus] || 'Unknown';
        
        // Set thumbnail
        const thumbnailData = this.getThumbnailData(episode);
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;
        
        if (animUrl) {
            thumbnailImg.src = `file:///${animUrl}`;
            thumbnailImg.style.display = 'block';
            thumbnailPlaceholder.style.display = 'none';
            thumbnailImg.onerror = () => {
                thumbnailImg.style.display = 'none';
                thumbnailPlaceholder.style.display = 'flex';
                thumbnailPlaceholder.textContent = episode.metadata.episode || '?';
            };
        } else {
            thumbnailImg.style.display = 'none';
            thumbnailPlaceholder.style.display = 'flex';
            thumbnailPlaceholder.textContent = episode.metadata.episode || '?';
        }
        
        // Set ratings
        const ratingsContainer = document.getElementById('popup-ratings');
        const hasEpisodeRating = episode.episode_metadata && episode.episode_metadata.rating && episode.episode_metadata.rating > 0;
        const hasUserScore = episode.myanimelist_watch_status && episode.myanimelist_watch_status.my_score && episode.myanimelist_watch_status.my_score > 0;
        
        if (hasEpisodeRating || hasUserScore) {
            let ratingsHtml = '';
            if (hasEpisodeRating) {
                ratingsHtml += `<div class="popup-rating community">★ ${episode.episode_metadata.rating.toFixed(1)}</div>`;
            }
            if (hasUserScore) {
                ratingsHtml += `<div class="popup-rating user">♥ ${episode.myanimelist_watch_status.my_score}/10</div>`;
            }
            ratingsContainer.innerHTML = ratingsHtml;
            ratingsContainer.style.display = 'flex';
        } else {
            ratingsContainer.style.display = 'none';
        }
        
        // Set tags
        const tagsContainer = document.getElementById('popup-tags');
        const seriesTags = episode.series_metadata && episode.series_metadata.tags;
        
        if (seriesTags && seriesTags.length > 0) {
            const maxVisibleTags = 8;
            const visibleTags = seriesTags.slice(0, maxVisibleTags);
            const remainingCount = seriesTags.length - maxVisibleTags;
            
            let tagsHtml = '<div class="popup-tags-label">Tags:</div><div class="popup-tags-list">';
            visibleTags.forEach(tag => {
                tagsHtml += `<span class="popup-tag">${this.escapeHtml(tag)}</span>`;
            });
            if (remainingCount > 0) {
                tagsHtml += `<span class="popup-tag popup-tag-more">+${remainingCount}</span>`;
            }
            tagsHtml += '</div>';
            
            tagsContainer.innerHTML = tagsHtml;
            tagsContainer.style.display = 'flex';
        } else {
            tagsContainer.style.display = 'none';
        }
        
        // Position popup
        const episodeItem = event.currentTarget;
        const episodeRect = episodeItem.getBoundingClientRect();
        const sidebarRect = document.querySelector('.sidebar').getBoundingClientRect();
        const popupWidth = 525;
        const popupHeight = 500; // Increased estimated height
        
        // Calculate the center of the episode item
        const episodeCenterY = episodeRect.top + (episodeRect.height / 2);
        
        // Default: position to the right of the episode item, centered vertically
        let left = sidebarRect.right + 15;
        let top = episodeCenterY - (popupHeight / 2);
        let arrowClass = 'left';
        
        // Check if popup would go off the right edge of the screen
        if (left + popupWidth > window.innerWidth - 20) {
            // Position to the left of the sidebar instead
            left = sidebarRect.left - popupWidth - 15;
            arrowClass = 'right';
        }
        
        // Check if popup would go off the top or bottom of the screen
        if (top < 20) {
            // Too high - adjust down but keep same horizontal position and arrow
            top = 20;
        } else if (top + popupHeight > window.innerHeight - 20) {
            // Too low - adjust up but keep same horizontal position and arrow
            top = window.innerHeight - popupHeight - 20;
        }
        
        // Only use top/bottom arrow positioning if we're extremely constrained
        if (popupHeight > window.innerHeight - 40) {
            // Popup is taller than available screen space - use top/bottom positioning
            if (episodeCenterY < window.innerHeight / 2) {
                // Episode is in top half - show popup below with top arrow
                top = episodeRect.bottom + 15;
                arrowClass = 'top';
                left = episodeRect.left + (episodeRect.width / 2) - (popupWidth / 2);
            } else {
                // Episode is in bottom half - show popup above with bottom arrow
                top = episodeRect.top - popupHeight - 15;
                arrowClass = 'bottom';
                left = episodeRect.left + (episodeRect.width / 2) - (popupWidth / 2);
            }
        }
        
        // Ensure popup doesn't go off the left edge
        if (left < 20) {
            left = 20;
        }
        
        // Set initial popup position
        popup.style.left = `${left}px`;
        popup.style.top = `${top}px`;
        
        // Set arrow class
        arrow.className = `popup-arrow ${arrowClass}`;
        
        // Show popup with delay (or immediately if already visible)
        const showDelay = isAlreadyVisible ? 0 : 300;
        this.popupTimeout = setTimeout(() => {
            popup.classList.add('show');
            
            // Auto-adjust positioning after popup is rendered and has actual dimensions
            setTimeout(() => {
                const actualHeight = popup.offsetHeight;
                if (actualHeight !== popupHeight && (arrowClass === 'left' || arrowClass === 'right')) {
                    // Recalculate position with actual height for better centering
                    let adjustedTop = episodeCenterY - (actualHeight / 2);
                    
                    // Check bounds with actual height
                    if (adjustedTop < 20) {
                        adjustedTop = 20;
                    } else if (adjustedTop + actualHeight > window.innerHeight - 20) {
                        adjustedTop = window.innerHeight - actualHeight - 20;
                    }
                    
                    popup.style.top = `${adjustedTop}px`;
                }
                
                // Reset transition speed after adjustment
                if (isAlreadyVisible) {
                    setTimeout(() => {
                        popup.style.transition = 'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
                    }, 50);
                }
            }, 50); // Small delay to ensure popup is fully rendered
        }, showDelay);
    }
    
    hideEpisodePopup() {
        if (this.popupTimeout) {
            clearTimeout(this.popupTimeout);
        }
        
        // Use a small delay to allow for switching between episodes
        this.hideTimeout = setTimeout(() => {
            const popup = document.getElementById('episode-hover-popup');
            popup.classList.remove('show');
            this.currentPopupIndex = -1;
            
            // Reset transition speed
            setTimeout(() => {
                popup.style.transition = 'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
            }, 50);
        }, 100); // Small delay to allow moving between episodes
    }
    
    escapeHtml(text) {
        if (typeof text !== 'string') return '';
        return text.replace(/[&<>"']/g, match => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[match]));
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new LatestEpisodesApp();
});