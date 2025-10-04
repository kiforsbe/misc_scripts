// Latest Episodes Webapp JavaScript

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
        this.init();
    }    selectEpisodeByPath(filePath) {
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
    } init() {
        this.setupEventListeners();
        this.updateHeaderStats();
        this.populateSeriesFilter();
        this.filterAndDisplayEpisodes();
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
        if (!mal) return 'no_mal_data';
        
        return mal.status.toLowerCase().replace(' ', '_').replace('-', '_');
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
            html += `
                <div class="series-group">
                    <div class="series-group-header">
                        <span>${this.escapeHtml(seriesTitle)}</span>
                        <span class="series-group-count">${episodes.length} episode${episodes.length !== 1 ? 's' : ''}</span>
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
            <div class="episode-item" onclick="app.selectEpisode(${index})" data-index="${index}">
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
                        <span class="watch-indicator ${watchStatus}"></span>
                        ${this.groupBy === 'series' ? '' : this.escapeHtml(episode.metadata.title)}
                    </div>
                    <div class="episode-title">
                        ${this.escapeHtml(episodeTitle)}
                    </div>
                    <div class="episode-meta">
                        <span>Episode ${episode.metadata.episode}${episodeCount > 1 ? ` of ${episodeCount}` : ''}</span>
                        <span>${this.formatFileSize(episode.file_size)}</span>
                    </div>
                    <div class="episode-date">${downloadDate.toLocaleDateString()}</div>
                </div>
            </div>
        `;
    }
    
    selectEpisode(index) {
        // Update visual selection
        document.querySelectorAll('.episode-item').forEach(item => {
            item.classList.remove('selected');
        });
        
        const selectedItem = document.querySelector(`[data-index="${index}"]`);
        if (selectedItem) {
            selectedItem.classList.add('selected');
            this.selectedEpisode = this.filteredEpisodes[index];
            this.renderEpisodeDetails();
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
                detailsHtml += `<p><strong>Air Date:</strong> ${ep.air_date}</p>`;
            }
            if (ep.rating) {
                detailsHtml += `<p><strong>Rating:</strong> ${ep.rating}/10</p>`;
            }
            if (ep.plot) {
                detailsHtml += `<p><strong>Plot:</strong> ${this.escapeHtml(ep.plot)}</p>`;
            }
        }
        
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
                    ${series.rating ? `<p><strong>Rating:</strong> ${series.rating}/10</p>` : ''}
                    ${series.total_episodes ? `<p><strong>Total Episodes:</strong> ${series.total_episodes}</p>` : ''}
                    ${series.genres && series.genres.length > 0 ? `<p><strong>Genres:</strong> ${series.genres.join(', ')}</p>` : ''}
                    ${series.plot ? `<p><strong>Plot:</strong> ${this.escapeHtml(series.plot)}</p>` : ''}
            `;
            
            // MyAnimeList link
            if (episode.myanimelist_url) {
                detailsHtml += `
                    <div class="mal-info">
                        <a href="${episode.myanimelist_url}" target="_blank" class="mal-link">
                            <i class="bi bi-box-arrow-up-right"></i>
                            View on MyAnimeList
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
                    <span class="status-badge status-${mal.status.toLowerCase().replace(' ', '-').replace(' ', '_')}">${mal.status}</span>
                    <p><strong>Episodes Watched:</strong> ${mal.watched_episodes} / ${mal.total_episodes || '?'}</p>
                    ${mal.score > 0 ? `<p><strong>Your Score:</strong> ${mal.score}/10</p>` : ''}
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
        
        // Scroll to selected item
        const selectedItem = document.querySelector(`[data-index="${newIndex}"]`);
        if (selectedItem) {
            selectedItem.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
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