// Series Completeness Webapp JavaScript

class SeriesCompletenessApp {
    constructor() {
        this.data = SERIES_DATA;
        this.filteredSeries = [];
        this.selectedSeries = null;
        this.searchTerm = '';
        this.statusFilter = 'all';
        this.malStatusFilter = 'all';
        // New: Watch status filter states
        this.watchFilters = {
            notWatched: true,
            partiallyWatched: true,
            fullyWatched: true
        };
        // --- Thumbnails index ---
        this.thumbnails = SERIES_DATA.thumbnails || [];
        this.thumbnailMap = this.buildThumbnailMap(this.thumbnails);
        this.popupTimeout = null;
        this.hideTimeout = null;
        this.currentPopupIndex = -1;
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

    getThumbnailData(file) {
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
        const filePath = file.path || file.filename;
        if (!filePath) return null;
        
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

    renderRatingInfo(file, style = 'compact') {
        let ratingHtml = '';
        const hasEpisodeRating = file.episode_metadata && file.episode_metadata.rating && file.episode_metadata.rating > 0;
        const hasUserScore = file.myanimelist_watch_status && file.myanimelist_watch_status.score && file.myanimelist_watch_status.score > 0;
        
        if (!hasEpisodeRating && !hasUserScore) return '';
        
        if (style === 'compact') {
            const ratings = [];
            if (hasEpisodeRating) {
                const communityScore = parseFloat(file.episode_metadata.rating).toFixed(1);
                ratings.push(`⭐ ${communityScore}/10`);
            }
            if (hasUserScore) {
                const userScore = parseFloat(file.myanimelist_watch_status.score).toFixed(1);
                ratings.push(`👤 ${userScore}/10`);
            }
            if (ratings.length > 0) {
                ratingHtml = `<div class="episode-ratings">${ratings.join(' • ')}</div>`;
            }
        } else if (style === 'detailed') {
            if (hasEpisodeRating) {
                const communityScore = parseFloat(file.episode_metadata.rating).toFixed(1);
                ratingHtml += `<p><strong>Rating:</strong> <span class="community-score">⭐ ${communityScore}/10</span></p>`;
            }
            if (hasUserScore) {
                const userScore = parseFloat(file.myanimelist_watch_status.score).toFixed(1);
                ratingHtml += `<p><strong>Your Score:</strong> <span class="user-score">👤 ${userScore}/10</span></p>`;
            }
        }
        
        return ratingHtml;
    }
    
    renderSeriesRatingInfo(series) {
        let ratingHtml = '';
        const metadata_id = series.title_id || (series.files && series.files[0] && series.files[0].metadata_id) || '';
        const title_metadata = this.data.title_metadata[metadata_id] || {};
        const hasCommunityRating = title_metadata.rating && title_metadata.rating > 0;
        const hasUserScore = series.myanimelist_watch_status && series.myanimelist_watch_status.score && series.myanimelist_watch_status.score > 0;
        
        if (hasCommunityRating) {
            const communityScore = parseFloat(title_metadata.rating).toFixed(1);
            ratingHtml += `<p><strong>Rating:</strong> <span class="community-score">⭐ ${communityScore}/10</span></p>`;
        }
        if (hasUserScore) {
            const userScore = parseFloat(series.myanimelist_watch_status.score).toFixed(1);
            ratingHtml += `<p><strong>Your Score:</strong> <span class="user-score">👤 ${userScore}/10</span></p>`;
        }
        
        return ratingHtml;
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
        
        // MAL status filter
        const malStatusFilter = document.getElementById('mal-status-filter');
        malStatusFilter.addEventListener('change', (e) => {
            this.malStatusFilter = e.target.value;
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
            
            // --- Movie status handling for filtering ---
            // Derive isMovie from title_metadata.type or files[0].type
            const files = series.files || [];
            const firstFile = files[0] || {};
            // Use series.title_id for season-specific metadata, fallback to first file's metadata_id
            const metadata_id = series.title_id || firstFile.metadata_id || '';
            const title_metadata = this.data.title_metadata && this.data.title_metadata[metadata_id] || {};
            let isMovie = false;
            if (title_metadata.type && typeof title_metadata.type === 'string' && title_metadata.type.toLowerCase().includes('movie')) {
                isMovie = true;
            }
            if (!isMovie && firstFile.type && typeof firstFile.type === 'string' && firstFile.type.toLowerCase().includes('movie')) {
                isMovie = true;
            }
            // --- End movie status handling ---

            // Apply status filter, treating movies as "complete"
            const matchesStatus = this.statusFilter === 'all' ||
                (isMovie && this.statusFilter === 'complete') ||
                (!isMovie && series.status === this.statusFilter);

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

            // Apply MAL status filter
            let matchesMalStatus = true;
            if (this.malStatusFilter !== 'all') {
                const malStatus = series.myanimelist_watch_status;
                if (this.malStatusFilter === 'no-mal-data') {
                    matchesMalStatus = !malStatus || !malStatus.my_status;
                } else {
                    const malStatusMap = {
                        'watching': 'Watching',
                        'completed': 'Completed',
                        'on-hold': 'On-Hold',
                        'dropped': 'Dropped',
                        'plan-to-watch': 'Plan to Watch'
                    };
                    const expectedStatus = malStatusMap[this.malStatusFilter];
                    matchesMalStatus = malStatus && malStatus.my_status === expectedStatus;
                }
            }

            if (matchesSearch && matchesStatus && matchesWatch && matchesMalStatus) {
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

            // --- Movie status handling (updated) ---
            // Derive isMovie from title_metadata.type or files[0].type
            const files = series.files || [];
            const firstFile = files[0] || {};
            // Use series.title_id for season-specific metadata, fallback to first file's metadata_id
            const metadata_id = series.title_id || firstFile.metadata_id || '';
            const title_metadata = this.data.title_metadata && this.data.title_metadata[metadata_id] || {};
            let isMovie = false;
            // Check title_metadata.type
            if (title_metadata.type && typeof title_metadata.type === 'string' && title_metadata.type.toLowerCase().includes('movie')) {
                isMovie = true;
            }
            // Or check files[0].type
            if (!isMovie && firstFile.type && typeof firstFile.type === 'string' && firstFile.type.toLowerCase().includes('movie')) {
                isMovie = true;
            }
            // ---

            let statusDisplay = '';
            let episodeDisplay = '';
            if (isMovie) {
                statusDisplay = `<span class="series-status status-movie">🎬 Movie</span>`;
                episodeDisplay = `<span class="episode-count">Movie</span>`;
            } else {
                statusDisplay = `<span class="series-status ${statusClass}">
                    ${statusIcon} ${this.formatStatus(series.status)}
                </span>`;
                episodeDisplay = `<span class="watch-count">${watchStatus.watched_episodes}</span>
                    <span class="episode-count">${series.episodes_found}/${series.episodes_expected || '?'}</span>`;
            }
            // --- 

            // MAL status display
            const malStatus = series.myanimelist_watch_status;
            let malStatusDisplay = '';
            if (malStatus && malStatus.my_status) {
                const statusIcons = {
                    'Watching': '👁️',
                    'Completed': '✅',
                    'On-Hold': '⏸️',
                    'Dropped': '❌',
                    'Plan to Watch': '📋'
                };
                const icon = statusIcons[malStatus.my_status] || '❓';
                malStatusDisplay = `<span class="mal-status" title="MyAnimeList: ${malStatus.my_status}">${icon} ${malStatus.my_status}</span>`;
            }

            // Get thumbnail for first episode
            const thumb = firstFile ? this.getThumbnailData(firstFile) : null;
            const staticUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : '';
            const animUrl = thumb && thumb.animated_thumbnail ? thumb.animated_thumbnail.replace(/\\/g, '/') : '';
            const thumbnailHtml = staticUrl 
                ? `<div class="series-thumbnail">
                    <img src="${staticUrl}" alt="${this.escapeHtml(titleWithSeason)}" loading="lazy" 
                         onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';"
                         onmouseenter="if(this.dataset.animUrl) this.src=this.dataset.animUrl" 
                         onmouseleave="if(this.dataset.animUrl) this.src='${staticUrl}'" 
                         data-anim-url="${animUrl}">
                    <div style="display: none; width: 100%; height: 100%; align-items: center; justify-content: center; font-size: 1.5rem; font-weight: 600;">📺</div>
                   </div>`
                : '<div class="series-thumbnail-placeholder">📺</div>';

            return `
                <div class="series-item" data-series-key="${series.key}" 
                     onclick="app.selectSeries('${series.key}')" 
                     onmouseenter="app.showSeriesPopup(event, '${series.key}')" 
                     onmouseleave="app.hideSeriesPopup()">
                    ${thumbnailHtml}
                    <div class="series-info">
                        <div class="series-title">${this.escapeHtml(titleWithSeason)}</div>
                        <div class="series-meta">
                            ${statusDisplay}
                            ${malStatusDisplay}
                            <div>
                              ${episodeDisplay}
                            </div>
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

        // Use series.title_id for season-specific metadata, fallback to first file's metadata_id
        const metadata_id = series.title_id || series.files[0].metadata_id || '';
        const title_metadata = this.data.title_metadata[metadata_id] || {};

        // Get thumbnail for first episode
        const firstFile = series.files && series.files[0];
        const thumbnailData = firstFile ? this.getThumbnailData(firstFile) : null;
        const staticUrl = thumbnailData && thumbnailData.static_thumbnail ? thumbnailData.static_thumbnail.replace(/\\/g, '/') : null;
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;

        // Get all available sources
        const sources = title_metadata.sources || [];
        const hasMultipleSources = sources.length > 1;
        const hasSources = sources.length > 0;
        
        // Get MyAnimeList source if available as default
        const myanimeList_source_url = sources.find(source => 
            source.toLowerCase().includes('myanimelist')
        ) || null;
        
        // Determine what to display for the title
        let titleHtml;
        if (!hasSources) {
            // No sources available - just show title without link
            titleHtml = `<h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2>`;
        } else if (!hasMultipleSources) {
            // Single source - direct link
            titleHtml = `<a href="${sources[0]}" target="_blank" rel="noopener noreferrer"><h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2></a>`;
        } else {
            // Multiple sources - show dropdown button with default link
            const defaultUrl = myanimeList_source_url || sources[0];
            titleHtml = `
                <div class="title-with-sources">
                    <a href="${defaultUrl}" target="_blank" rel="noopener noreferrer"><h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2></a>
                    <div class="sources-dropdown">
                        <button class="sources-dropdown-btn" onclick="app.toggleSourcesDropdown(event)" aria-label="Choose source">
                            <i class="bi bi-chevron-down"></i>
                        </button>
                        <div class="sources-dropdown-menu" style="display: none;">
                            ${sources.map(source => {
                                const sourceName = this.getSourceName(source);
                                return `<a href="${source}" target="_blank" rel="noopener noreferrer" class="source-link">${sourceName}</a>`;
                            }).join('')}
                        </div>
                    </div>
                </div>
            `;
        }

        const container = document.getElementById('series-details');
        container.innerHTML = `
            <div class="details-header" style="position: relative;">
                ${staticUrl ? `
                    <div class="series-detail-thumbnail">
                        <img src="${staticUrl}" alt="Series thumbnail" 
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" 
                             onmouseenter="if(this.dataset.animUrl) this.src=this.dataset.animUrl" 
                             onmouseleave="if(this.dataset.animUrl) this.src='${staticUrl}'" 
                             data-anim-url="${animUrl || ''}">
                        <div style="display: none; width: 100%; height: 100%; align-items: center; justify-content: center; font-size: 2rem; font-weight: 600;">
                            📺
                        </div>
                    </div>
                ` : ''}
                ${titleHtml}
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
                
                ${this.renderSeriesInfo(series)}
                
                ${this.renderMalInfo(series)}
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
        
        // Add rating information
        const ratingInfo = this.renderSeriesRatingInfo(series);
        if (ratingInfo) {
            content += `<div class="mt-3">${ratingInfo}</div>`;
        }
        
        return content;
    }
    
    renderSeriesInfo(series) {
        const metadata_id = series.title_id || (series.files && series.files[0] && series.files[0].metadata_id) || '';
        const title_metadata = this.data.title_metadata[metadata_id] || {};
        
        // Check if we have any metadata to show
        const hasYear = title_metadata.year;
        const hasGenres = title_metadata.genres && title_metadata.genres.length > 0;
        const hasTags = title_metadata.tags && title_metadata.tags.length > 0;
        const hasPlot = title_metadata.plot;
        const hasType = title_metadata.type;
        
        if (!hasYear && !hasGenres && !hasTags && !hasPlot && !hasType) {
            return ''; // Don't show the card if no metadata available
        }
        
        return `
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-info-circle"></i>
                    Series Information
                </h4>
                ${hasType ? `<p><strong>Type:</strong> ${this.escapeHtml(title_metadata.type)}</p>` : ''}
                ${hasYear ? `<p><strong>Year:</strong> ${title_metadata.year}</p>` : ''}
                ${hasGenres ? `<p><strong>Genres:</strong> ${title_metadata.genres.map(g => this.escapeHtml(g)).join(', ')}</p>` : ''}
                ${hasTags ? this.renderTagsWithPopup(title_metadata.tags) : ''}
                ${hasPlot ? `<p><strong>Plot:</strong> ${this.escapeHtml(title_metadata.plot)}</p>` : ''}
            </div>
        `;
    }
    
    renderMalInfo(series) {
        const malStatus = series.myanimelist_watch_status;
        
        if (!malStatus || !malStatus.my_status) {
            return ''; // Don't show the card if no MAL data
        }
        
        // Calculate season-specific MAL status based on actual watch progress
        const watchStatus = series.watch_status || {};
        const watchedEpisodes = watchStatus.watched_episodes || 0;
        const totalEpisodes = series.episodes_found || 0;
        
        // Determine season-specific status
        let seasonSpecificStatus = malStatus.my_status;
        let seasonSpecificIcon = malStatus.my_status;
        let seasonSpecificColor = malStatus.my_status;
        
        if (totalEpisodes > 0) {
            if (watchedEpisodes === 0) {
                // No episodes watched in this season - show as "Plan to Watch" regardless of series MAL status
                seasonSpecificStatus = 'Plan to Watch';
                seasonSpecificIcon = 'Plan to Watch';
                seasonSpecificColor = 'Plan to Watch';
            } else if (watchedEpisodes === totalEpisodes) {
                // All episodes watched in this season - show as "Completed"
                seasonSpecificStatus = 'Completed';
                seasonSpecificIcon = 'Completed';
                seasonSpecificColor = 'Completed';
            } else {
                // Partially watched - show as "Watching"
                seasonSpecificStatus = 'Watching';
                seasonSpecificIcon = 'Watching';
                seasonSpecificColor = 'Watching';
            }
        }
        
        const statusIcons = {
            'Watching': '👁️',
            'Completed': '✅',
            'On-Hold': '⏸️',
            'Dropped': '❌',
            'Plan to Watch': '📋'
        };
        
        const statusColors = {
            'Watching': 'text-primary',
            'Completed': 'text-success', 
            'On-Hold': 'text-warning',
            'Dropped': 'text-danger',
            'Plan to Watch': 'text-info'
        };
        
        const icon = statusIcons[seasonSpecificIcon] || '❓';
        const colorClass = statusColors[seasonSpecificColor] || 'text-muted';
        
        return `
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-star"></i>
                    MyAnimeList Status
                </h4>
                <div class="mal-info">
                    <div class="mal-status-large ${colorClass}">
                        <span class="mal-icon">${icon}</span>
                        <span class="mal-status-text">${seasonSpecificStatus}</span>
                    </div>
                    ${seasonSpecificStatus !== malStatus.my_status ? `
                    <div class="mal-season-note">
                        <small class="text-muted">Season-specific status (Series: ${malStatus.my_status})</small>
                    </div>
                    ` : ''}
                    ${malStatus.my_score > 0 ? `
                    <div class="mal-score">
                        <strong>Score:</strong> <span class="score-value">${malStatus.my_score}/10</span>
                    </div>
                    ` : ''}
                    ${malStatus.my_watched_episodes !== undefined && malStatus.my_watched_episodes >= 0 ? `
                    <div class="mal-episodes">
                        <strong>MAL Episodes Watched:</strong> ${malStatus.my_watched_episodes} (Series Total)
                    </div>
                    ` : ''}
                    ${malStatus.comments ? `
                    <div class="mal-comments mt-3">
                        <strong>Your Review:</strong>
                        <div class="mal-comments-text">${this.escapeHtml(malStatus.comments)}</div>
                    </div>
                    ` : ''}
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
            if (file.episode_watched) {
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
            const staticUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : null;
            const animUrl = thumb && thumb.animated_thumbnail ? thumb.animated_thumbnail.replace(/\\/g, '/') : null;
            if (staticUrl) {
                // Add mouse events for animated thumbnail
                thumbBg = `
                    <img class="episode-thumb-bg" src="${staticUrl}" alt="thumb" loading="lazy"
                        ${animUrl ? `
                            data-static="${staticUrl}" data-anim="${animUrl}"
                            onmouseenter="app._swapToAnimThumb(this)" 
                            onmouseleave="app._swapToStaticThumb(this)"
                        ` : ''}
                    >
                `;
            }
            // Top-left: episode number, Top-right: status icon (SVG)
            grid += `<div class="${className}" title="${title}">${thumbBg}<span class="ep-corner ep-num-corner">${i}</span><span class="ep-corner ep-status-corner">${icon}</span></div>`;
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
                ${series.files.map((file, idx) => {
                    //Use the basic watch-status, and if available, use the partial watch-status from Plex
                    const watchIndicator = file.episode_watched ? 'watched' : (file.plex_watch_status?.view_offset > 0 ? 'partially-watched' : 'unwatched');
                    const thumb = this.getFileThumbnail(file);
                    const staticUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : null;
                    const animUrl = thumb && thumb.animated_thumbnail ? thumb.animated_thumbnail.replace(/\\/g, '/') : null;
                    // Give the image a unique id for lookup
                    const imgId = `file-thumb-img-${idx}`;
                    // --- Add episode type label if type:extra or type:movie ---
                    let typeLabel = '';
                    let fileType = file.type;
                    const typeLabels = [];
                    const typeArr = Array.isArray(fileType) ? fileType : (fileType ? [fileType] : []);
                    typeArr.forEach(t => {
                        const typeLower = (t || '').toLowerCase();
                        if (typeLower === 'extra') {
                            typeLabels.push('<span class="file-type-label file-type-extra">Extra</span>');
                        } else if (typeLower === 'movie') {
                            typeLabels.push('<span class="file-type-label file-type-movie">Movie</span>');
                        }
                    });
                    typeLabel = typeLabels.join(' ');
                    // Attach mouse events to the file-item div
                    return `
                        <div class="file-item file-item-with-thumb"
                            ${animUrl ? `
                                onmouseenter="app._swapToAnimThumbById('${imgId}', '${animUrl}')"
                                onmouseleave="app._swapToStaticThumbById('${imgId}', '${staticUrl}')"
                            ` : ''}
                        >
                            <div class="file-thumb">
                                ${staticUrl ? `
                                    <img id="${imgId}" src="${staticUrl}" alt="thumbnail" loading="lazy">
                                ` : '<div class="file-thumb-placeholder"></div>'}
                            </div>
                            <div class="file-info">
                                <div class="file-name" title="${this.escapeHtml(file.filename || file.path)}">
                                    ${this.escapeHtml(this.getFileName(file.filename || file.path))}
                                </div>
                                <div class="file-meta">
                                    <span>
                                        <span class="watch-indicator ${watchIndicator}"></span>
                                        ${watchIndicator === 'watched' ? 'Watched' : (watchIndicator === 'partially-watched' ? 'Partially Watched' : 'Unwatched')}
                                    </span>
                                    ${typeLabel}
                                    ${file.episode ? `<span><i class="bi bi-hash"></i>Episode ${Array.isArray(file.episode) ? file.episode.join(', ') : file.episode}</span>` : ''}
                                    ${file.size ? `<span><i class="bi bi-hdd"></i>${this.formatFileSize(file.size)}</span>` : ''}
                                    ${file.duration ? `<span><i class="bi bi-clock"></i>${this.formatDuration(file.duration)}</span>` : ''}
                                    ${this.renderRatingInfo(file, 'compact')}
                                </div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }

    // --- Animated thumbnail swap handlers ---
    _swapToAnimThumb(img) {
        if (img.dataset.anim) {
            img.src = img.dataset.anim;
        }
    }
    _swapToStaticThumb(img) {
        if (img.dataset.static) {
            img.src = img.dataset.static;
        }
    }
    _swapToAnimThumbById(imgId, animUrl) {
        const img = document.getElementById(imgId);
        if (img && animUrl) img.src = animUrl;
    }
    _swapToStaticThumbById(imgId, staticUrl) {
        const img = document.getElementById(imgId);
        if (img && staticUrl) img.src = staticUrl;
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
    
    showSeriesPopup(event, seriesKey) {
        // Clear any existing hide timeout
        if (this.hideTimeout) {
            clearTimeout(this.hideTimeout);
        }
        
        if (this.popupTimeout) {
            clearTimeout(this.popupTimeout);
        }
        
        const series = this.data.groups[seriesKey];
        if (!series) return;
        
        const popup = document.getElementById('series-hover-popup');
        const isAlreadyVisible = popup.classList.contains('show');
        const isSameSeries = this.currentPopupIndex === seriesKey;
        
        // If it's the same series, don't do anything
        if (isSameSeries && isAlreadyVisible) {
            return;
        }
        
        // Update current popup index
        this.currentPopupIndex = seriesKey;
        
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
        const titleWithSeason = series.title + (series.season ? ` Season ${series.season}` : '');
        document.getElementById('popup-title').textContent = titleWithSeason;
        
        const statusIcon = this.getStatusIcon(series.status);
        const statusLabel = this.formatStatus(series.status);
        document.getElementById('popup-status-info').textContent = `${statusIcon} ${statusLabel} - ${series.episodes_found}/${series.episodes_expected || '?'} episodes`;
        
        // Set watch progress
        const watchStatus = series.watch_status || {};
        const watchedPercent = watchStatus.completion_percent || 0;
        document.getElementById('popup-watch-progress').textContent = `${watchStatus.watched_episodes || 0} watched (${watchedPercent.toFixed(0)}%)`;
        
        // Set MAL status if available
        const malContainer = document.getElementById('popup-mal-status');
        const malStatus = series.myanimelist_watch_status;
        if (malStatus && malStatus.my_status) {
            const statusIcons = {
                'Watching': '👁️',
                'Completed': '✅',
                'On-Hold': '⏸️',
                'Dropped': '❌',
                'Plan to Watch': '📋'
            };
            const icon = statusIcons[malStatus.my_status] || '❓';
            malContainer.textContent = `${icon} ${malStatus.my_status}`;
            malContainer.style.display = 'block';
        } else {
            malContainer.style.display = 'none';
        }
        
        // Set thumbnail
        const firstFile = series.files && series.files[0];
        const thumbnailData = firstFile ? this.getThumbnailData(firstFile) : null;
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;
        
        if (animUrl) {
            thumbnailImg.src = animUrl;
            thumbnailImg.style.display = 'block';
            thumbnailPlaceholder.style.display = 'none';
            thumbnailImg.onerror = () => {
                thumbnailImg.style.display = 'none';
                thumbnailPlaceholder.style.display = 'flex';
                thumbnailPlaceholder.textContent = '📺';
            };
        } else {
            thumbnailImg.style.display = 'none';
            thumbnailPlaceholder.style.display = 'flex';
            thumbnailPlaceholder.textContent = '📺';
        }
        
        // Set ratings
        const ratingsContainer = document.getElementById('popup-ratings');
        const metadata_id = series.title_id || (series.files && series.files[0] && series.files[0].metadata_id) || '';
        const title_metadata = this.data.title_metadata[metadata_id] || {};
        const hasCommunityRating = title_metadata.rating && title_metadata.rating > 0;
        const hasUserScore = series.myanimelist_watch_status && series.myanimelist_watch_status.score && series.myanimelist_watch_status.score > 0;
        
        let ratingsHtml = '';
        if (hasCommunityRating) {
            ratingsHtml += `<div class="popup-rating community">⭐ ${title_metadata.rating.toFixed(1)}</div>`;
        }
        if (hasUserScore) {
            ratingsHtml += `<div class="popup-rating user">♥ ${series.myanimelist_watch_status.score}/10</div>`;
        }
        if (ratingsHtml) {
            ratingsContainer.innerHTML = ratingsHtml;
            ratingsContainer.style.display = 'flex';
        } else {
            ratingsContainer.style.display = 'none';
        }
        
        // Position popup
        const seriesItem = event.currentTarget;
        const seriesRect = seriesItem.getBoundingClientRect();
        const sidebarRect = document.querySelector('.sidebar').getBoundingClientRect();
        const popupWidth = 525;
        const popupHeight = 450;
        
        // Calculate the center of the series item
        const seriesCenterY = seriesRect.top + (seriesRect.height / 2);
        
        // Default: position to the right of the series item, centered vertically
        let left = sidebarRect.right + 15;
        let top = seriesCenterY - (popupHeight / 2);
        let arrowClass = 'left';
        
        // Check if popup would go off the right edge of the screen
        if (left + popupWidth > window.innerWidth - 20) {
            // Position to the left of the sidebar instead
            left = sidebarRect.left - popupWidth - 15;
            arrowClass = 'right';
        }
        
        // Check if popup would go off the top or bottom of the screen
        if (top < 20) {
            top = 20;
        } else if (top + popupHeight > window.innerHeight - 20) {
            top = window.innerHeight - popupHeight - 20;
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
            
            // Auto-adjust positioning after popup is rendered
            setTimeout(() => {
                const actualHeight = popup.offsetHeight;
                if (actualHeight !== popupHeight && (arrowClass === 'left' || arrowClass === 'right')) {
                    let adjustedTop = seriesCenterY - (actualHeight / 2);
                    
                    if (adjustedTop < 20) {
                        adjustedTop = 20;
                    } else if (adjustedTop + actualHeight > window.innerHeight - 20) {
                        adjustedTop = window.innerHeight - actualHeight - 20;
                    }
                    
                    popup.style.top = `${adjustedTop}px`;
                }
                
                if (isAlreadyVisible) {
                    setTimeout(() => {
                        popup.style.transition = 'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
                    }, 50);
                }
            }, 50);
        }, showDelay);
    }
    
    hideSeriesPopup() {
        if (this.popupTimeout) {
            clearTimeout(this.popupTimeout);
        }
        
        // Use a small delay to allow for switching between series
        this.hideTimeout = setTimeout(() => {
            const popup = document.getElementById('series-hover-popup');
            popup.classList.remove('show');
            this.currentPopupIndex = -1;
            
            // Reset transition speed
            setTimeout(() => {
                popup.style.transition = 'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
            }, 50);
        }, 100);
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
                ${this.escapeHtml(visibleTagsText)}${hiddenCount > 0 ? `, <span class="tags-more" onmouseenter="app.showTagsPopup(event, '${this.escapeHtml(hiddenTagsText).replace(/'/g, "\\'")}'${hiddenCount})" onmouseleave="app.hideTagsPopup()" style="color: var(--primary-color); cursor: help; text-decoration: underline;">+${hiddenCount} more</span>` : ''}
            </p>
        `;
    }
    
    showTagsPopup(event, hiddenTagsText, hiddenCount) {
        const popup = document.getElementById('tags-popup');
        if (!popup) {
            // Create popup if it doesn't exist
            const popupDiv = document.createElement('div');
            popupDiv.id = 'tags-popup';
            popupDiv.className = 'tags-popup';
            document.body.appendChild(popupDiv);
        }
        
        const tagsPopup = document.getElementById('tags-popup');
        tagsPopup.textContent = hiddenTagsText;
        tagsPopup.style.display = 'block';
        
        const rect = event.target.getBoundingClientRect();
        tagsPopup.style.left = `${rect.left}px`;
        tagsPopup.style.top = `${rect.bottom + 5}px`;
    }
    
    hideTagsPopup() {
        const popup = document.getElementById('tags-popup');
        if (popup) {
            popup.style.display = 'none';
        }
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
    
    getSourceName(url) {
        if (url.includes('myanimelist.net')) return 'MyAnimeList';
        if (url.includes('anilist.co')) return 'AniList';
        if (url.includes('anisearch.com')) return 'AniSearch';
        if (url.includes('kitsu.app')) return 'Kitsu';
        if (url.includes('imdb.com')) return 'IMDb';
        if (url.includes('tmdb.org')) return 'TMDB';
        
        // Fallback to domain name
        try {
            const domain = new URL(url).hostname;
            return domain.replace('www.', '').split('.')[0];
        } catch {
            return 'External Link';
        }
    }
    
    toggleSourcesDropdown(event) {
        event.preventDefault();
        event.stopPropagation();
        
        const dropdown = event.target.closest('.sources-dropdown');
        const menu = dropdown.querySelector('.sources-dropdown-menu');
        const isVisible = menu.style.display !== 'none';
        
        // Close all other dropdowns first
        document.querySelectorAll('.sources-dropdown-menu').forEach(m => {
            if (m !== menu) m.style.display = 'none';
        });
        
        // Toggle this dropdown
        menu.style.display = isVisible ? 'none' : 'block';
        
        if (!isVisible) {
            // Close dropdown when clicking outside
            const closeDropdown = (e) => {
                if (!dropdown.contains(e.target)) {
                    menu.style.display = 'none';
                    document.removeEventListener('click', closeDropdown);
                }
            };
            setTimeout(() => document.addEventListener('click', closeDropdown), 0);
        }
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new SeriesCompletenessApp();
});
