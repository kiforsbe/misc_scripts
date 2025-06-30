// Series Completeness Webapp JavaScript

class SeriesCompletenessApp {
    constructor() {
        this.data = SERIES_DATA;
        this.filteredSeries = [];
        this.selectedSeries = null;
        this.searchTerm = '';
        this.statusFilter = 'all';
        // New: Watch status filter states
        this.watchFilters = {
            notWatched: true,
            partiallyWatched: true,
            fullyWatched: true
        };
        // --- Thumbnails index ---
        this.thumbnails = SERIES_DATA.thumbnails || [];
        this.thumbnailMap = this.buildThumbnailMap(this.thumbnails);
        this.init();
    }

    buildThumbnailMap(thumbnails) {
        // Map by absolute path for fast lookup
        const map = {};
        for (const entry of thumbnails) {
            if (entry && entry.video) {
                map[entry.video] = entry;
            }
        }
        return map;
    }

    getFileThumbnail(file) {
        // Try to match by file.path or file.filename
        if (!file) return null;
        let key = file.path || file.filename;
        if (!key) return null;
        // Try exact match
        if (this.thumbnailMap[key]) return this.thumbnailMap[key];
        // Try basename match (fallback)
        const base = key.split(/[/\\]/).pop();
        for (const k in this.thumbnailMap) {
            if (k.split(/[/\\]/).pop() === base) return this.thumbnailMap[k];
        }
        return null;
    }

    init() {
        this.setupEventListeners();
        this.updateHeaderStats();
        this.filterAndDisplaySeries();
    }
    
    setupEventListeners() {
        // Search functionality
        const searchInput = document.getElementById('search-input');
        searchInput.addEventListener('input', (e) => {
            this.searchTerm = e.target.value.toLowerCase();
            this.filterAndDisplaySeries();
        });
        
        // Status filter
        const statusFilter = document.getElementById('status-filter');
        statusFilter.addEventListener('change', (e) => {
            this.statusFilter = e.target.value;
            this.filterAndDisplaySeries();
        });
        
        // Watch status filters
        document.getElementById('filter-not-watched').addEventListener('change', (e) => {
            this.watchFilters.notWatched = e.target.checked;
            this.filterAndDisplaySeries();
        });
        document.getElementById('filter-partially-watched').addEventListener('change', (e) => {
            this.watchFilters.partiallyWatched = e.target.checked;
            this.filterAndDisplaySeries();
        });
        document.getElementById('filter-fully-watched').addEventListener('change', (e) => {
            this.watchFilters.fullyWatched = e.target.checked;
            this.filterAndDisplaySeries();
        });
        
        // Keyboard navigation
        document.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                this.navigateList(e.key === 'ArrowDown' ? 1 : -1);
            } else if (e.key === 'Enter') {
                const selected = document.querySelector('.series-item.selected');
                if (selected) {
                    this.selectSeries(selected.dataset.seriesKey);
                }
            }
        });
    }
    
    updateHeaderStats() {
        const summary = this.data.completeness_summary;
        
        document.getElementById('total-series').textContent = summary.total_series;
        document.getElementById('complete-series').textContent = summary.complete_series;
        document.getElementById('incomplete-series').textContent = summary.incomplete_series;
        
        const completionRate = summary.total_episodes_expected > 0 
            ? ((summary.total_episodes_found / summary.total_episodes_expected) * 100).toFixed(1)
            : '0';
        document.getElementById('completion-rate').textContent = `${completionRate}%`;
    }
    
    filterAndDisplaySeries() {
        const groups = this.data.groups;
        this.filteredSeries = [];
        
        for (const [key, series] of Object.entries(groups)) {
            // Apply search filter
            const matchesSearch = !this.searchTerm || 
                series.title.toLowerCase().includes(this.searchTerm) ||
                (series.season && series.season.toString().includes(this.searchTerm));
            
            // Apply status filter
            const matchesStatus = this.statusFilter === 'all' || series.status === this.statusFilter;

            // --- New: Watch status filter ---
            // Determine watch status for the group
            const ws = series.watch_status || {};
            let watchCategory = 'notWatched';
            if ((ws.watched_episodes || 0) === (series.episodes_found || 0) && (series.episodes_found || 0) > 0) {
                watchCategory = 'fullyWatched';
            } else if ((ws.partially_watched_episodes || 0) > 0 || (ws.watched_episodes || 0) > 0) {
                watchCategory = 'partiallyWatched';
            }
            // Only include if the corresponding filter is checked
            const matchesWatch = this.watchFilters[watchCategory];
            // --- End new ---

            if (matchesSearch && matchesStatus && matchesWatch) {
                this.filteredSeries.push({ key, ...series });
            }
        }
        
        // Sort by title
        this.filteredSeries.sort((a, b) => {
            const titleA = a.title + (a.season ? ` S${a.season.toString().padStart(2, '0')}` : '');
            const titleB = b.title + (b.season ? ` S${b.season.toString().padStart(2, '0')}` : '');
            return titleA.localeCompare(titleB);
        });
        
        this.renderSeriesList();
    }
    
    renderSeriesList() {
        const container = document.getElementById('series-list');
        
        if (this.filteredSeries.length === 0) {
            container.innerHTML = `
                <div class="text-center p-4 text-muted">
                    <i class="bi bi-search display-4"></i>
                    <p class="mt-2">No series found matching your criteria</p>
                </div>
            `;
            return;
        }
        
        container.innerHTML = this.filteredSeries.map(series => {
            const titleWithSeason = series.title + (series.season ? ` S${series.season.toString().padStart(2, '0')}` : '');
            const statusClass = `status-${series.status}`;
            const statusIcon = this.getStatusIcon(series.status);
            
            // Calculate watch progress
            const watchStatus = series.watch_status || {};
            const watchedPercent = watchStatus.completion_percent || 0;
            
            return `
                <div class="series-item" data-series-key="${series.key}" onclick="app.selectSeries('${series.key}')">
                    <div class="series-title">${this.escapeHtml(titleWithSeason)}</div>
                    <div class="series-meta">
                        <span class="series-status ${statusClass}">
                            ${statusIcon} ${this.formatStatus(series.status)}
                        </span>
                        <div>
                          <span class="watch-count">${watchStatus.watched_episodes}</span>
                          <span class="episode-count">${series.episodes_found}/${series.episodes_expected || '?'}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }
    
    selectSeries(seriesKey) {
        // Update visual selection
        document.querySelectorAll('.series-item').forEach(item => {
            item.classList.remove('selected');
        });
        
        const selectedItem = document.querySelector(`[data-series-key="${seriesKey}"]`);
        if (selectedItem) {
            selectedItem.classList.add('selected');
        }
        
        // Store selection and render details
        this.selectedSeries = this.data.groups[seriesKey];
        this.renderSeriesDetails();
    }
    
    renderSeriesDetails() {
        if (!this.selectedSeries) return;
        
        const series = this.selectedSeries;
        const titleWithSeason = series.title + (series.season ? ` Season ${series.season}` : '');
        const statusClass = `status-${series.status}`;
        const statusIcon = this.getStatusIcon(series.status);

        const title_metadata_key = series.files[0].title_metadata_key || '';
        const title_metadata = this.data.title_metadata[title_metadata_key] || {};

        // Get MyAnimeList source if available. title_metadata.sources is an array of strings, that we then need to filder out the right source from
        const myanimeList_source_url = title_metadata.sources ?
            title_metadata.sources.find(source => source.toLowerCase().includes('myanimelist')) || null
            : null;

        const container = document.getElementById('series-details');
        container.innerHTML = `
            <div class="details-header">
                <a href="${myanimeList_source_url}"><h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2></a>
                <div class="details-subtitle">
                    ${series.episodes_found} episodes found${series.episodes_expected ? ` of ${series.episodes_expected} expected` : ''}
                </div>
                <div class="status-badge ${statusClass}">
                    ${statusIcon} ${this.formatStatus(series.status)}
                </div>
            </div>
            
            <div class="details-grid">
                <div class="details-card">
                    <h4 class="card-title">
                        <i class="bi bi-pie-chart"></i>
                        Completion Status
                    </h4>
                    ${this.renderCompletionInfo(series)}
                </div>
                
                <div class="details-card">
                    <h4 class="card-title">
                        <i class="bi bi-eye"></i>
                        Watch Progress
                    </h4>
                    ${this.renderWatchProgress(series)}
                </div>
            </div>
            
            ${this.renderEpisodeGrid(series)}
            
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-file-earmark-play"></i>
                    Files (${series.files.length})
                </h4>
                ${this.renderFilesList(series)}
            </div>
        `;
    }
    
    renderCompletionInfo(series) {
        const missing = series.missing_episodes || [];
        const extra = series.extra_episodes || [];
        
        let content = `
            <div class="mb-3">
                <strong>Episodes Found:</strong> ${series.episodes_found}<br>
                <strong>Episodes Expected:</strong> ${series.episodes_expected || 'Unknown'}
            </div>
        `;
        
        if (missing.length > 0) {
            content += `
                <div class="mb-2">
                    <strong class="text-danger">Missing Episodes:</strong><br>
                    <span class="text-danger">${this.formatEpisodeRanges(missing)}</span>
                </div>
            `;
        }
        
        if (extra.length > 0) {
            content += `
                <div class="mb-2">
                    <strong class="text-warning">Extra Episodes:</strong><br>
                    <span class="text-warning">${this.formatEpisodeRanges(extra)}</span>
                </div>
            `;
        }
        
        return content;
    }
    
    renderWatchProgress(series) {
        const watchStatus = series.watch_status || {};
        const watched = watchStatus.watched_episodes || 0;
        const partially = watchStatus.partially_watched_episodes || 0;
        const unwatched = watchStatus.unwatched_episodes || series.episodes_found;
        const percent = watchStatus.completion_percent || 0;
        
        return `
            <div class="progress-container">
                <div class="progress-label">
                    <span>Watched: ${watched}/${series.episodes_found}</span>
                    <span>${percent.toFixed(1)}%</span>
                </div>
                <div class="progress">
                    <div class="progress-bar bg-success" style="width: ${percent}%"></div>
                </div>
            </div>
            <div class="row text-center mt-3">
                <div class="col">
                    <small class="text-success">
                        <i class="bi bi-check-circle"></i> ${watched} Watched
                    </small>
                </div>
                ${partially > 0 ? `
                <div class="col">
                    <small class="text-warning">
                        <i class="bi bi-clock"></i> ${partially} Partial
                    </small>
                </div>
                ` : ''}
                <div class="col">
                    <small class="text-muted">
                        <i class="bi bi-circle"></i> ${unwatched} Unwatched
                    </small>
                </div>
            </div>
        `;
    }
    
    renderEpisodeGrid(series) {
        if (!series.episode_numbers || series.episode_numbers.length === 0) {
            return '';
        }
        // Determine the full range of episodes to show (from 1 to max expected or found)
        const maxEpisode = Math.max(
            ...series.episode_numbers,
            ...(series.episodes_expected ? [series.episodes_expected] : [])
        );
        const found = new Set(series.episode_numbers);
        const missing = new Set(series.missing_episodes || []);
        const extra = new Set(series.extra_episodes || []);
        const watched = new Set();
        // Get watched episodes
        series.files.forEach(file => {
            const plexStatus = file.plex_watch_status;
            if (plexStatus && plexStatus.watched) {
                const episode = file.episode;
                if (Array.isArray(episode)) {
                    episode.forEach(ep => watched.add(ep));
                } else if (episode !== null) {
                    watched.add(episode);
                }
            }
        });
        // Map episode number to file (for thumbnail lookup)
        const episodeToFile = {};
        series.files.forEach(file => {
            let eps = file.episode;
            if (Array.isArray(eps)) {
                eps.forEach(ep => { if (!episodeToFile[ep]) episodeToFile[ep] = file; });
            } else if (eps != null) {
                if (!episodeToFile[eps]) episodeToFile[eps] = file;
            }
        });
        // Status icons (SVG for high contrast)
        const statusIcons = {
            watched: '<svg width="24" height="24" viewBox="0 0 24 24" fill="#28a745" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10" fill="#28a745"/><polyline points="8 12.5 11 15.5 16 9.5" fill="none"/></svg>',
            found:   '<svg width="24" height="24" viewBox="0 0 24 24" fill="#007bff" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="6" width="18" height="12" rx="2" fill="#007bff"/><polygon points="10,9 16,12 10,15" fill="#fff"/></svg>',
            extra:   '<svg width="24" height="24" viewBox="0 0 24 24" fill="#ffc107" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10" fill="#ffc107"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/></svg>',
            missing: '<svg width="24" height="24" viewBox="0 0 24 24" fill="#dc3545" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10" fill="#dc3545"/><line x1="8" y1="8" x2="16" y2="16"/><line x1="16" y1="8" x2="8" y2="16"/></svg>'
        };
        let grid = '<div class="details-card"><h4 class="card-title"><i class="bi bi-grid-3x3"></i>Episode Grid</h4><div class="episode-grid">';
        for (let i = 1; i <= maxEpisode; i++) {
            let className = 'episode-number ';
            let title = `Episode ${i}`;
            let status = '';
            let icon = '';
            if (watched.has(i)) {
                className += 'episode-watched';
                title += ' (Watched)';
                status = 'watched';
                icon = statusIcons.watched;
            } else if (found.has(i)) {
                className += 'episode-found';
                title += ' (Available)';
                status = 'found';
                icon = statusIcons.found;
            } else if (extra.has(i)) {
                className += 'episode-extra';
                title += ' (Extra)';
                status = 'extra';
                icon = statusIcons.extra;
            } else {
                className += 'episode-missing';
                title += ' (Missing)';
                status = 'missing';
                icon = statusIcons.missing;
            }
            // Always render a box for every episode in the range
            let thumbBg = '';
            const file = episodeToFile[i];
            const thumb = this.getFileThumbnail(file);
            const thumbUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : null;
            if (thumbUrl) {
                thumbBg = `<img class=\"episode-thumb-bg\" src=\"${thumbUrl}\" alt=\"thumb\" loading=\"lazy\">`;
            }
            // Top-left: episode number, Top-right: status icon (SVG)
            grid += `<div class="${className}" title="${title}">${thumbBg}<span class=\"ep-corner ep-num-corner\">${i}</span><span class=\"ep-corner ep-status-corner\">${icon}</span></div>`;
        }
        grid += '</div></div>';
        return grid;
    }
    
    renderFilesList(series) {
        if (!series.files || series.files.length === 0) {
            return '<p class="text-muted">No files found</p>';
        }
        return `
            <div class="files-list">
                ${series.files.map(file => {
                    const plexStatus = file.plex_watch_status || {};
                    const watchIndicator = plexStatus.watched ? 'watched' : 
                        (plexStatus.view_offset > 0 ? 'partially-watched' : 'unwatched');
                    const thumb = this.getFileThumbnail(file);
                    const thumbUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : null;
                    return `
                        <div class="file-item file-item-with-thumb">
                            <div class="file-thumb">
                                ${thumbUrl ? `<img src="${thumbUrl}" alt="thumbnail" loading="lazy">` : '<div class="file-thumb-placeholder"></div>'}
                            </div>
                            <div class="file-info">
                                <div class="file-name" title="${this.escapeHtml(file.filename || file.path)}">
                                    ${this.escapeHtml(this.getFileName(file.filename || file.path))}
                                </div>
                                <div class="file-meta">
                                    <span>
                                        <span class="watch-indicator ${watchIndicator}"></span>
                                        ${plexStatus.watched ? 'Watched' : 
                                          (plexStatus.view_offset > 0 ? 'Partially Watched' : 'Unwatched')}
                                    </span>
                                    ${file.episode ? `<span><i class="bi bi-hash"></i>Episode ${Array.isArray(file.episode) ? file.episode.join(', ') : file.episode}</span>` : ''}
                                    ${file.size ? `<span><i class="bi bi-hdd"></i>${this.formatFileSize(file.size)}</span>` : ''}
                                    ${file.duration ? `<span><i class="bi bi-clock"></i>${this.formatDuration(file.duration)}</span>` : ''}
                                </div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }
    
    navigateList(direction) {
        const items = document.querySelectorAll('.series-item');
        const currentSelected = document.querySelector('.series-item.selected');
        
        if (items.length === 0) return;
        
        let newIndex = 0;
        if (currentSelected) {
            const currentIndex = Array.from(items).indexOf(currentSelected);
            newIndex = Math.max(0, Math.min(items.length - 1, currentIndex + direction));
        }
        
        const newSelected = items[newIndex];
        if (newSelected) {
            const seriesKey = newSelected.dataset.seriesKey;
            this.selectSeries(seriesKey);
            newSelected.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
    }
    
    // Utility functions
    getStatusIcon(status) {
        const icons = {
            'complete': '✅',
            'incomplete': '❌',
            'complete_with_extras': '⚠️',
            'no_episode_numbers': '❓',
            'unknown_total_episodes': '❓',
            'not_series': 'ℹ️',
            'no_metadata': '❓',
            'no_metadata_manager': '❓',
            'unknown': '❓'
        };
        return icons[status] || '❓';
    }
    
    formatStatus(status) {
        const statusMap = {
            'complete': 'Complete',
            'incomplete': 'Incomplete',
            'complete_with_extras': 'Complete (with extras)',
            'no_episode_numbers': 'No episode numbers',
            'unknown_total_episodes': 'Unknown total episodes',
            'not_series': 'Not a series',
            'no_metadata': 'No metadata',
            'no_metadata_manager': 'No metadata manager',
            'unknown': 'Unknown'
        };
        return statusMap[status] || 'Unknown';
    }
    
    formatEpisodeRanges(episodes) {
        if (!episodes || episodes.length === 0) return '';
        
        const sorted = [...episodes].sort((a, b) => a - b);
        const ranges = [];
        let start = sorted[0];
        let end = start;
        
        for (let i = 1; i < sorted.length; i++) {
            if (sorted[i] === end + 1) {
                end = sorted[i];
            } else {
                ranges.push(start === end ? start.toString() : `${start}-${end}`);
                start = end = sorted[i];
            }
        }
        ranges.push(start === end ? start.toString() : `${start}-${end}`);
        
        return ranges.join(', ');
    }
    
    getFileName(path) {
        return path.split(/[/\\]/).pop() || path;
    }
    
    formatFileSize(bytes) {
        if (!bytes) return '';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = bytes;
        let unitIndex = 0;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return `${size.toFixed(1)} ${units[unitIndex]}`;
    }
    
    formatDuration(seconds) {
        if (!seconds) return '';
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        
        if (hours > 0) {
            return `${hours}h ${minutes}m`;
        } else {
            return `${minutes}m`;
        }
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new SeriesCompletenessApp();
});
