// Series Completeness Webapp JavaScript

class SeriesCompletenessApp {
    constructor() {
        this.data = SERIES_DATA;
        this.filteredSeries = [];
        this.selectedSeries = null;
        this.selectedSeriesKey = null;
        this.searchTerm = '';
        this.combinedQuery = '';
        this.statusFilter = 'all';
        this.malStatusFilter = 'all';
        this.watchStatusFilter = 'all';
        this.groupBy = 'none';
        this.seriesTitles = [];
        this.seriesTags = [];
        this.seriesGenres = [];
        this.seriesTypes = [];
        this.activeSuggestionIndex = -1;
        this.lastSuggestionKey = '';
        this.suppressNextSearchFocusOpen = false;
        this.searchDebounceMs = 300;
        this.searchDebounceTimer = null;
        this.mobileBreakpoint = 768;
        this.supportsHoverInteractions = this.detectHoverSupport();
        this.thumbnailDir = SERIES_DATA.thumbnail_dir || 'thumbnails';
        this.hostSubnetIp = SERIES_DATA.host_subnet_ip || '127.0.0.1';
        this.popupTimeout = null;
        this.hideTimeout = null;
        this.currentPopupIndex = -1;
        this.normalizeSeriesData();
        this.init();
    }

    detectHoverSupport() {
        if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
            return true;
        }

        return window.matchMedia('(hover: hover)').matches;
    }

    isMobile() {
        return typeof window !== 'undefined' && window.innerWidth <= this.mobileBreakpoint;
    }

    sortSuggestionValues(values) {
        return Array.from(values).sort((left, right) => left.localeCompare(right, undefined, {
            numeric: true,
            sensitivity: 'base'
        }));
    }

    normalizeSeriesData() {
        if (!this.data || !this.data.groups || typeof this.data.groups !== 'object') {
            return;
        }

        const seriesSet = new Set();
        const tagSet = new Set();
        const genreSet = new Set();
        const typeSet = new Set();

        Object.entries(this.data.groups).forEach(([key, series]) => {
            series.key = key;
            this.cacheSeriesFilterFields(series);

            if (series._displayTitle) {
                seriesSet.add(series._displayTitle);
            }

            (series._filterTags || []).forEach((tag) => tagSet.add(tag));
            (series._filterGenres || []).forEach((genre) => genreSet.add(genre));

            if (series._filterTypeLabel) {
                typeSet.add(series._filterTypeLabel);
            }
        });

        this.seriesTitles = this.sortSuggestionValues(seriesSet);
        this.seriesTags = this.sortSuggestionValues(tagSet);
        this.seriesGenres = this.sortSuggestionValues(genreSet);
        this.seriesTypes = this.sortSuggestionValues(typeSet);
    }

    getTitleMetadata(series) {
        const files = series.files || [];
        const firstFile = files[0] || {};
        const metadataId = series.title_id || series.metadata_id || firstFile.metadata_id || '';
        const titleMetadata = this.data.title_metadata?.[metadataId] || {};
        const seriesMetadata = firstFile.series_metadata || {};
        const groupMetadata = series.group_metadata || {};

        return {
            ...seriesMetadata,
            ...groupMetadata,
            ...titleMetadata,
            tags: titleMetadata.tags || groupMetadata.tags || seriesMetadata.tags || [],
            genres: titleMetadata.genres || groupMetadata.genres || seriesMetadata.genres || [],
            sources: titleMetadata.sources || groupMetadata.sources || seriesMetadata.sources || []
        };
    }

    getSeriesType(series, titleMetadata = this.getTitleMetadata(series)) {
        const titleType = typeof titleMetadata.type === 'string' ? titleMetadata.type : '';
        const firstFile = (series.files || [])[0] || {};
        const fileType = Array.isArray(firstFile.type) ? firstFile.type.join(' ') : (firstFile.type || '');
        return titleType || fileType || 'Series';
    }

    getSeriesDisplayTitle(series) {
        return series.title + (series.season ? ` S${String(series.season).padStart(2, '0')}` : '');
    }

    normalizeStatusValue(value) {
        return String(value || '')
            .trim()
            .toLowerCase()
            .replace(/[_\s]+/g, '-')
            .replace(/[^a-z0-9-]/g, '');
    }

    toStatusClass(value) {
        return this.normalizeStatusValue(value).replace(/^-+|-+$/g, '') || 'unknown';
    }

    getNormalizedMalStatus(series) {
        const status = series.myanimelist_watch_status?.my_status;
        return status ? this.normalizeStatusValue(status) : 'no-mal-data';
    }

    getNormalizedWatchStatus(series) {
        const watchStatus = series.watch_status || {};
        const watchedEpisodes = watchStatus.watched_episodes || 0;
        const partiallyWatchedEpisodes = watchStatus.partially_watched_episodes || 0;
        const episodesFound = series.episodes_found || 0;

        if (episodesFound > 0 && watchedEpisodes >= episodesFound) {
            return 'fully-watched';
        }

        if (partiallyWatchedEpisodes > 0 || watchedEpisodes > 0) {
            return 'partially-watched';
        }

        return 'not-watched';
    }

    buildSeriesSearchIndex(series, titleMetadata) {
        const tags = Array.isArray(titleMetadata.tags) ? titleMetadata.tags.join(' ') : '';
        const genres = Array.isArray(titleMetadata.genres) ? titleMetadata.genres.join(' ') : '';
        const sources = Array.isArray(titleMetadata.sources) ? titleMetadata.sources.map((source) => this.getSourceName(source)).join(' ') : '';
        const filenames = Array.isArray(series.files)
            ? series.files.map((file) => file.filename || file.file_path || file.path || '').join(' ')
            : '';
        const completenessTerms = [
            this.formatStatus(series.status),
            this.getSeriesType(series, titleMetadata),
            series.title || '',
            series.season || '',
            series.myanimelist_watch_status?.my_status || '',
            this.getNormalizedWatchStatus(series),
            filenames,
            tags,
            genres,
            sources,
            titleMetadata.plot || ''
        ];

        return completenessTerms.join(' ').toLowerCase();
    }

    cacheSeriesFilterFields(series) {
        const titleMetadata = this.getTitleMetadata(series);
        const typeLabel = this.getSeriesType(series, titleMetadata);

        series._displayTitle = this.getSeriesDisplayTitle(series);
        series._filterStatus = series.status;
        series._filterStatusLabel = this.formatStatus(series.status);
        series._filterWatchStatus = this.getNormalizedWatchStatus(series);
        series._filterMalStatus = this.getNormalizedMalStatus(series);
        series._filterTypeLabel = typeLabel;
        series._filterType = this.normalizeStatusValue(typeLabel);
        series._filterTags = Array.isArray(titleMetadata.tags) ? titleMetadata.tags : [];
        series._filterGenres = Array.isArray(titleMetadata.genres) ? titleMetadata.genres : [];
        series._filterSearchIndex = this.buildSeriesSearchIndex(series, titleMetadata);
    }

    // Calculate SHA256 hash of a string (for thumbnail lookup)
    async sha256(text) {
        const encoder = new TextEncoder();
        const data = encoder.encode(text);
        const hashBuffer = await crypto.subtle.digest('SHA-256', data);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
        return hashHex;
    }

    // Get thumbnail paths for a video file based on filename hash
    async getThumbnailPaths(videoPath) {
        if (!videoPath) return { static: null, animated: null };
        
        // Extract filename from path
        const filename = videoPath.split(/[/\\]/).pop();
        
        // Calculate SHA256 hash of filename
        const hash = await this.sha256(filename);
        
        // Build thumbnail paths
        const staticPath = `${this.thumbnailDir}/${hash}_static.webp`;
        const animatedPath = `${this.thumbnailDir}/${hash}_video.webp`;
        
        return { static: staticPath, animated: animatedPath };
    }

    async getThumbnailData(file) {
        // Calculate thumbnail paths on-the-fly using filename hash
        const filePath = file.file_path || file.filepath || file.path || file.filename;
        if (!filePath) return null;
        
        const paths = await this.getThumbnailPaths(filePath);
        return {
            static_thumbnail: paths.static,
            animated_thumbnail: paths.animated
        };
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
        this.syncSearchContainerState('');
        this.filterAndDisplaySeries();
        this.showListOnMobile();
    }
    
    setupEventListeners() {
        const searchInput = document.getElementById('search-input');
        const suggestionsContainer = document.getElementById('series-suggestions');
        const dropdownIndicator = document.querySelector('.search-dropdown-indicator');
        const clearBtn = document.getElementById('search-clear-btn');

        searchInput.addEventListener('input', (e) => {
            this.handleSearchInput(e.target.value);
        });

        searchInput.addEventListener('focus', () => {
            if (this.suppressNextSearchFocusOpen) {
                this.suppressNextSearchFocusOpen = false;
                return;
            }

            this.renderSeriesSuggestions(searchInput.value);
        });

        searchInput.addEventListener('click', () => {
            this.renderSeriesSuggestions(searchInput.value);
        });

        searchInput.addEventListener('blur', () => {
            window.setTimeout(() => this.hideSeriesSuggestions(), 120);
        });

        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown') {
                if (this.areSeriesSuggestionsVisible()) {
                    e.preventDefault();
                    this.moveSuggestionSelection(1);
                }
            } else if (e.key === 'ArrowUp') {
                if (this.areSeriesSuggestionsVisible()) {
                    e.preventDefault();
                    this.moveSuggestionSelection(-1);
                }
            } else if (e.key === 'Enter') {
                if (this.areSeriesSuggestionsVisible() && this.activeSuggestionIndex >= 0) {
                    e.preventDefault();
                    this.selectActiveSuggestion();
                }
            } else if (e.key === 'Escape') {
                this.hideSeriesSuggestions();
            }
        });

        if (suggestionsContainer) {
            suggestionsContainer.addEventListener('mousedown', (event) => {
                event.preventDefault();
            });
        }

        if (dropdownIndicator) {
            dropdownIndicator.addEventListener('click', () => {
                this.toggleSeriesSuggestions();
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                searchInput.value = '';
                this.applySearchNow('');
                searchInput.focus();
            });
        }
        
        const statusFilter = document.getElementById('status-filter');
        statusFilter.addEventListener('change', (e) => {
            this.statusFilter = e.target.value;
            this.filterAndDisplaySeries();
        });
        
        const malStatusFilter = document.getElementById('mal-status-filter');
        malStatusFilter.addEventListener('change', (e) => {
            this.malStatusFilter = e.target.value;
            this.filterAndDisplaySeries();
        });
        
        const watchStatusFilter = document.getElementById('watch-status-filter');
        watchStatusFilter.addEventListener('change', (e) => {
            this.watchStatusFilter = e.target.value;
            this.filterAndDisplaySeries();
        });
        
        const groupBySelect = document.getElementById('group-by-select');
        groupBySelect.addEventListener('change', (e) => {
            this.groupBy = e.target.value;
            this.filterAndDisplaySeries();
        });

        const filterToggle = document.getElementById('filter-toggle');
        if (filterToggle) {
            filterToggle.addEventListener('click', () => {
                const filterControls = document.getElementById('filter-controls');
                const expanded = filterToggle.getAttribute('aria-expanded') !== 'false';
                filterToggle.setAttribute('aria-expanded', expanded ? 'false' : 'true');
                filterControls.classList.toggle('collapsed', expanded);
            });
        }

        const mobileBackButton = document.getElementById('mobile-back-btn');
        if (mobileBackButton) {
            mobileBackButton.addEventListener('click', () => {
                this.showListOnMobile();
            });
        }

        window.addEventListener('resize', () => {
            this.hideSeriesSuggestions();
            if (!this.isMobile()) {
                this.showDesktopLayout();
            } else if (this.selectedSeriesKey) {
                this.showMainOnMobile();
            }
        }, { passive: true });

        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-container')) {
                this.hideSeriesSuggestions();
                this.clearInputTypeMarker();
            }
        });
        
        document.addEventListener('keydown', (e) => {
            if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && document.activeElement?.id !== 'search-input') {
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

    clearPendingSearchDebounce() {
        if (this.searchDebounceTimer) {
            window.clearTimeout(this.searchDebounceTimer);
            this.searchDebounceTimer = null;
        }
    }

    syncSearchContainerState(rawValue) {
        const container = document.querySelector('.search-container');
        if (!container) {
            return;
        }

        container.classList.toggle('has-value', Boolean((rawValue || '').trim()));
    }

    setCombinedQuery(value) {
        this.searchTerm = value || '';
        this.combinedQuery = (value || '').trim().toLowerCase();
        this.syncSearchContainerState(value);
    }

    applySearchNow(value) {
        this.clearPendingSearchDebounce();
        this.setCombinedQuery(value);
        this.filterAndDisplaySeries();
        this.renderSeriesSuggestions(value);
        if (!value.trim()) {
            this.clearInputTypeMarker();
        }
    }

    handleSearchInput(value) {
        this.syncSearchContainerState(value);
        this.clearPendingSearchDebounce();
        this.searchDebounceTimer = window.setTimeout(() => {
            this.applySearchNow(value);
        }, this.searchDebounceMs);
    }

    getHighlightedText(value, inputValue) {
        if (!inputValue) {
            return this.escapeHtml(value);
        }

        const lowerValue = value.toLowerCase();
        const lowerInput = inputValue.toLowerCase();
        const matchIndex = lowerValue.indexOf(lowerInput);
        if (matchIndex === -1) {
            return this.escapeHtml(value);
        }

        const before = this.escapeHtml(value.slice(0, matchIndex));
        const match = this.escapeHtml(value.slice(matchIndex, matchIndex + inputValue.length));
        const after = this.escapeHtml(value.slice(matchIndex + inputValue.length));
        return `${before}<span class="match-highlight">${match}</span>${after}`;
    }

    renderSeriesSuggestions(rawValue) {
        const container = document.getElementById('series-suggestions');
        if (!container) {
            return;
        }

        const raw = (rawValue || '').trim();
        const suggestionKey = raw.toLowerCase();
        if (container.style.display === 'block' && suggestionKey === this.lastSuggestionKey) {
            return;
        }

        let forcedType = null;
        let filterText = suggestionKey;
        const prefixMatch = suggestionKey.match(/^(series|tag|genre|type):\s*(.*)$/);
        if (prefixMatch) {
            forcedType = prefixMatch[1];
            filterText = prefixMatch[2].trim();
            this.showInputTypeMarker(forcedType);
        } else {
            this.clearInputTypeMarker();
        }

        const suggestionGroups = [];
        const collectMatches = (items, typeLabel, prefixLabel) => {
            return items
                .filter((item) => !filterText || item.toLowerCase().includes(filterText))
                .map((item) => ({
                    value: item,
                    type: typeLabel,
                    prefix: prefixLabel
                }));
        };

        if (!forcedType || forcedType === 'series') {
            suggestionGroups.push(collectMatches(this.seriesTitles, 'Series', 'series'));
        }
        if (!forcedType || forcedType === 'tag') {
            suggestionGroups.push(collectMatches(this.seriesTags, 'Tag', 'tag'));
        }
        if (!forcedType || forcedType === 'genre') {
            suggestionGroups.push(collectMatches(this.seriesGenres, 'Genre', 'genre'));
        }
        if (!forcedType || forcedType === 'type') {
            suggestionGroups.push(collectMatches(this.seriesTypes, 'Type', 'type'));
        }

        const suggestions = [];
        suggestionGroups.forEach((group) => {
            suggestions.push(...group);
        });

        const deduped = Array.from(new Map(suggestions.map((item) => [`${item.prefix}:${item.value.toLowerCase()}`, item])).values());

        if (deduped.length === 0) {
            container.innerHTML = '';
            container.style.display = 'none';
            this.activeSuggestionIndex = -1;
            this.lastSuggestionKey = suggestionKey;
            return;
        }

        container.innerHTML = deduped.map((item, index) => `
            <div class="series-suggestion-item" data-index="${index}" data-value="${this.escapeHtml(item.value)}" data-prefix="${item.prefix}" data-type="${item.type}" role="option" aria-selected="false">
                <span>${this.getHighlightedText(item.value, filterText)}</span>
                <span class="match-hint">${item.type}</span>
            </div>
        `).join('');
        container.style.display = 'block';
        this.activeSuggestionIndex = -1;
        this.lastSuggestionKey = suggestionKey;

        container.querySelectorAll('.series-suggestion-item').forEach((item) => {
            item.addEventListener('click', () => {
                this.applySuggestion(item.getAttribute('data-value') || '', item.getAttribute('data-prefix') || 'series');
            });
        });
    }

    hideSeriesSuggestions() {
        const container = document.getElementById('series-suggestions');
        if (!container) {
            return;
        }

        container.style.display = 'none';
        this.activeSuggestionIndex = -1;
        this.lastSuggestionKey = '';
    }

    areSeriesSuggestionsVisible() {
        const container = document.getElementById('series-suggestions');
        return Boolean(container && container.style.display === 'block');
    }

    toggleSeriesSuggestions() {
        if (this.areSeriesSuggestionsVisible()) {
            this.hideSeriesSuggestions();
            return;
        }

        const searchInput = document.getElementById('search-input');
        this.renderSeriesSuggestions(searchInput ? searchInput.value : '');
    }

    showInputTypeMarker(type) {
        const container = document.querySelector('.search-container');
        if (!container) {
            return;
        }

        let marker = container.querySelector('.input-type-hint');
        if (!marker) {
            marker = document.createElement('span');
            marker.className = 'input-type-hint';
            container.appendChild(marker);
        }

        marker.className = `input-type-hint input-type-${type}`;
        marker.textContent = `${type.charAt(0).toUpperCase()}${type.slice(1)} search`;
    }

    clearInputTypeMarker() {
        const marker = document.querySelector('.search-container .input-type-hint');
        if (marker) {
            marker.remove();
        }
    }

    moveSuggestionSelection(delta) {
        const items = Array.from(document.querySelectorAll('.series-suggestion-item'));
        if (items.length === 0) {
            return;
        }

        if (this.activeSuggestionIndex === -1) {
            this.activeSuggestionIndex = delta > 0 ? 0 : items.length - 1;
        } else {
            this.activeSuggestionIndex = (this.activeSuggestionIndex + delta + items.length) % items.length;
        }

        items.forEach((item, index) => {
            item.classList.toggle('active', index === this.activeSuggestionIndex);
            item.setAttribute('aria-selected', index === this.activeSuggestionIndex ? 'true' : 'false');
        });
    }

    selectActiveSuggestion() {
        const activeItem = document.querySelector('.series-suggestion-item.active');
        if (!activeItem) {
            return;
        }

        this.applySuggestion(activeItem.getAttribute('data-value') || '', activeItem.getAttribute('data-prefix') || 'series');
    }

    applySuggestion(value, prefix) {
        const query = `${prefix}:${value}`;
        const searchInput = document.getElementById('search-input');
        if (searchInput) {
            searchInput.value = query;
        }

        this.suppressNextSearchFocusOpen = true;
        this.applySearchNow(query);
        this.hideSeriesSuggestions();
    }

    applyTagFilter(tag) {
        const normalizedTag = String(tag || '').trim();
        if (!normalizedTag) {
            return;
        }

        const searchInput = document.getElementById('search-input');
        if (searchInput) {
            searchInput.value = `tag:${normalizedTag}`;
            searchInput.focus();
        }

        this.applySearchNow(`tag:${normalizedTag}`);
        this.showListOnMobile();
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

    matchesCombinedQuery(series) {
        if (!this.combinedQuery) {
            return true;
        }

        const prefixMatch = this.combinedQuery.match(/^(series|tag|genre|type):\s*(.*)$/);
        if (prefixMatch) {
            const prefix = prefixMatch[1];
            const value = prefixMatch[2].trim();
            if (!value) {
                return true;
            }

            if (prefix === 'series') {
                return series._displayTitle.toLowerCase().includes(value);
            }
            if (prefix === 'tag') {
                return series._filterTags.some((tag) => tag.toLowerCase().includes(value));
            }
            if (prefix === 'genre') {
                return series._filterGenres.some((genre) => genre.toLowerCase().includes(value));
            }
            if (prefix === 'type') {
                return series._filterTypeLabel.toLowerCase().includes(value);
            }
        }

        const terms = this.combinedQuery.split(/\s+/).filter(Boolean);
        return terms.every((term) => series._filterSearchIndex.includes(term));
    }
    
    async filterAndDisplaySeries() {
        const groups = this.data.groups;
        this.filteredSeries = [];
        
        for (const [key, series] of Object.entries(groups)) {
            const matchesSearch = this.matchesCombinedQuery(series);
            const matchesStatus = this.statusFilter === 'all' || series._filterStatus === this.statusFilter;
            const matchesWatch = this.watchStatusFilter === 'all' || series._filterWatchStatus === this.watchStatusFilter;
            const matchesMalStatus = this.malStatusFilter === 'all' || series._filterMalStatus === this.malStatusFilter;

            if (matchesSearch && matchesStatus && matchesWatch && matchesMalStatus) {
                this.filteredSeries.push({ key, ...series });
            }
        }
        
        this.filteredSeries.sort((a, b) => {
            return a._displayTitle.localeCompare(b._displayTitle);
        });
        
        await this.renderSeriesList();

        if (this.selectedSeriesKey) {
            const stillVisible = this.filteredSeries.some((series) => series.key === this.selectedSeriesKey);
            if (!stillVisible) {
                this.selectedSeriesKey = null;
                this.selectedSeries = null;
                document.getElementById('series-details').innerHTML = `
                    <div class="welcome-message">
                        <i class="bi bi-collection-play display-1 text-muted"></i>
                        <h3 class="text-muted">Select a series to inspect coverage</h3>
                        <p class="text-muted">Choose a series from the list to review completeness, watch progress, Plex activity, and MyAnimeList metadata in one place.</p>
                    </div>
                `;
            }
        }
    }

    showMainOnMobile() {
        const contentContainer = document.querySelector('.content-container');
        const header = document.querySelector('.header');
        const mobileBackButton = document.getElementById('mobile-back-btn');
        if (contentContainer) {
            contentContainer.classList.add('mobile-show-main');
        }
        if (header) {
            header.classList.add('mobile-showing-main');
        }
        if (mobileBackButton) {
            mobileBackButton.style.display = 'inline-flex';
        }
    }

    showListOnMobile() {
        const contentContainer = document.querySelector('.content-container');
        const header = document.querySelector('.header');
        const mobileBackButton = document.getElementById('mobile-back-btn');
        if (contentContainer) {
            contentContainer.classList.remove('mobile-show-main');
        }
        if (header) {
            header.classList.remove('mobile-showing-main');
        }
        if (mobileBackButton) {
            mobileBackButton.style.display = 'none';
        }
    }

    showDesktopLayout() {
        const contentContainer = document.querySelector('.content-container');
        const header = document.querySelector('.header');
        const mobileBackButton = document.getElementById('mobile-back-btn');
        if (contentContainer) {
            contentContainer.classList.remove('mobile-show-main');
        }
        if (header) {
            header.classList.remove('mobile-showing-main');
        }
        if (mobileBackButton) {
            mobileBackButton.style.display = 'none';
        }
    }
    
    async renderSeriesList() {
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
        
        let html = '';
        
        if (this.groupBy === 'none') {
            html = await this.renderSeriesItems(this.filteredSeries);
        } else {
            // Group series
            const groups = this.groupSeries(this.filteredSeries);
            
            // Render all groups in parallel
            const groupHtmlPairs = await Promise.all(
                Object.entries(groups).map(async ([groupName, seriesList]) => {
                    const itemsHtml = await this.renderSeriesItems(seriesList);
                    return `
                        <div class="series-group">
                            <div class="series-group-header">
                                <span class="series-group-title">${this.escapeHtml(groupName)}</span>
                                <span class="series-group-count">${seriesList.length}</span>
                            </div>
                            ${itemsHtml}
                        </div>
                    `;
                })
            );
            html = groupHtmlPairs.join('');
        }
        
        container.innerHTML = html;

        if (this.selectedSeriesKey) {
            const selectedItem = container.querySelector(`[data-series-key="${this.selectedSeriesKey}"]`);
            if (selectedItem) {
                selectedItem.classList.add('selected');
            }
        }
    }
    
    groupSeries(series) {
        const groups = {};
        
        for (const s of series) {
            let groupName = 'Other';
            
            if (this.groupBy === 'status') {
                groupName = s._filterStatusLabel;
            } else if (this.groupBy === 'mal-status') {
                groupName = s.myanimelist_watch_status?.my_status || 'No MAL Data';
            }
            
            if (!groups[groupName]) {
                groups[groupName] = [];
            }
            groups[groupName].push(s);
        }
        
        return groups;
    }
    
    async renderSeriesItems(seriesList) {
        const items = await Promise.all(seriesList.map(async (series) => {
            const titleWithSeason = series._displayTitle;
            const statusClass = `status-${this.toStatusClass(series._filterStatus)}`;
            const statusIcon = this.getStatusIcon(series.status);
            const watchStatus = series.watch_status || {};
            const watchedPercent = watchStatus.completion_percent || 0;
            const files = series.files || [];
            const firstFile = files[0] || {};
            const titleMetadata = this.getTitleMetadata(series);
            const isMovie = series._filterType === 'movie';
            const statusDisplay = isMovie
                ? `<span class="series-status status-movie">🎬 Movie</span>`
                : `<span class="series-status ${statusClass}">${statusIcon} ${series._filterStatusLabel}</span>`;
            const episodeDisplay = isMovie
                ? `<span class="episode-count">Movie</span>`
                : `<span class="episode-count">${series.episodes_found}/${series.episodes_expected || '?'}</span>`;
            const subtitleBits = [
                series._filterTypeLabel,
                titleMetadata.year ? String(titleMetadata.year) : '',
                titleMetadata.genres?.slice(0, 2).join(' • ') || ''
            ].filter(Boolean);
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

            const quickMeta = [`<span class="watch-count">${watchStatus.watched_episodes || 0} watched</span>`, episodeDisplay, `<span>${watchedPercent.toFixed(0)}%</span>`].join('');
            const tags = (series._filterTags || []).slice(0, 3);
            const tagMarkup = tags.length > 0
                ? `<div class="series-tags-inline">${tags.map((tag) => `<span class="series-tag-pill">${this.escapeHtml(tag)}</span>`).join('')}</div>`
                : '';

            const thumb = firstFile ? await this.getThumbnailData(firstFile) : null;
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
                <div class="series-item watch-${series._filterWatchStatus} mal-${series._filterMalStatus}" data-series-key="${series.key}" 
                     onclick="app.selectSeries('${series.key}')" 
                     onmouseenter="${this.supportsHoverInteractions ? `app.showSeriesPopup(event, '${series.key}')` : ''}" 
                     onmouseleave="${this.supportsHoverInteractions ? 'app.hideSeriesPopup()' : ''}">
                    ${thumbnailHtml}
                    <div class="series-info">
                        <div class="series-title">${this.escapeHtml(titleWithSeason)}</div>
                        ${subtitleBits.length > 0 ? `<div class="episode-ratings">${subtitleBits.map((bit) => this.escapeHtml(bit)).join(' • ')}</div>` : ''}
                        <div class="series-meta series-meta-primary">
                            <div class="series-meta-statuses">
                                ${statusDisplay}
                                ${malStatusDisplay}
                            </div>
                            <div class="series-progress-indicator">${watchedPercent.toFixed(0)}%</div>
                        </div>
                        <div class="series-meta">
                            ${quickMeta}
                        </div>
                        ${tagMarkup}
                    </div>
                </div>
            `;
        }));
        return items.join('');
    }
    
    async selectSeries(seriesKey) {
        document.querySelectorAll('.series-item').forEach(item => {
            item.classList.remove('selected');
        });
        
        const selectedItem = document.querySelector(`[data-series-key="${seriesKey}"]`);
        if (selectedItem) {
            selectedItem.classList.add('selected');
        }
        
        this.selectedSeriesKey = seriesKey;
        this.selectedSeries = this.data.groups[seriesKey];
        await this.renderSeriesDetails();

        if (this.isMobile()) {
            this.showMainOnMobile();
        }
    }

    getPreferredPlexFile(series) {
        const files = Array.isArray(series.files) ? series.files : [];
        return files.find((file) => file?.plex_watch_status?.server_hash && file?.plex_watch_status?.metadata_item_id) || files[0] || null;
    }

    buildPlexUrl(plexWatchStatus) {
        if (!plexWatchStatus?.server_hash || !plexWatchStatus?.metadata_item_id) {
            return null;
        }

        return `http://${this.hostSubnetIp}:32400/web/index.html#!/server/${plexWatchStatus.server_hash}/details?key=%2Flibrary%2Fmetadata%2F${plexWatchStatus.metadata_item_id}`;
    }

    getPreferredInfoSource(series) {
        const titleMetadata = this.getTitleMetadata(series);
        const sources = Array.isArray(titleMetadata.sources) ? [...titleMetadata.sources] : [];
        const firstFileSource = this.getPreferredPlexFile(series)?.source_url;
        if (firstFileSource && !sources.includes(firstFileSource)) {
            sources.push(firstFileSource);
        }

        const preferredSource = sources.find((source) => source.includes('myanimelist.net'))
            || sources.find((source) => source.includes('imdb.com'))
            || sources[0]
            || null;

        return preferredSource ? {
            url: preferredSource,
            name: this.getSourceName(preferredSource)
        } : null;
    }

    renderSourceLinks(sources, plexUrl) {
        const sourceLinks = Array.isArray(sources) ? sources : [];
        if (sourceLinks.length === 0 && !plexUrl) {
            return '';
        }

        const sourceHtml = sourceLinks.map((source) => {
            const sourceName = this.getSourceName(source);
            const linkClass = sourceName === 'MyAnimeList' ? 'mal-link' : (sourceName === 'IMDb' ? 'imdb-link' : 'external-link');
            return `<a href="${source}" target="_blank" rel="noopener noreferrer" class="external-link ${linkClass}">${this.escapeHtml(sourceName)}</a>`;
        }).join('');
        const plexHtml = plexUrl
            ? `<a href="${plexUrl}" target="_blank" rel="noopener noreferrer" class="plex-link"><span class="plex-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 4l8 8-8 8z"></path></svg></span><span>Plex</span></a>`
            : '';

        return `<div class="external-links">${sourceHtml}${plexHtml}</div>`;
    }
    
    async renderSeriesDetails() {
        if (!this.selectedSeries) return;
        
        const series = this.selectedSeries;
        const titleWithSeason = series.title + (series.season ? ` Season ${series.season}` : '');
        const statusClass = `status-${this.toStatusClass(series.status)}`;
        const statusIcon = this.getStatusIcon(series.status);
        const title_metadata = this.getTitleMetadata(series);
        const firstFile = this.getPreferredPlexFile(series);
        const thumbnailData = firstFile ? await this.getThumbnailData(firstFile) : null;
        const staticUrl = thumbnailData && thumbnailData.static_thumbnail ? thumbnailData.static_thumbnail.replace(/\\/g, '/') : null;
        const animUrl = thumbnailData && thumbnailData.animated_thumbnail ? thumbnailData.animated_thumbnail.replace(/\\/g, '/') : null;
        const sources = title_metadata.sources || [];
        const hasMultipleSources = sources.length > 1;
        const hasSources = sources.length > 0;
        const myanimeList_source_url = sources.find(source => 
            source.toLowerCase().includes('myanimelist')
        ) || null;
        const plexUrl = this.buildPlexUrl(firstFile?.plex_watch_status);
        const metaSummary = [series._filterTypeLabel, title_metadata.year ? String(title_metadata.year) : ''].filter(Boolean).join(' • ');
        
        let titleHtml;
        if (!hasSources) {
            titleHtml = `<h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2>`;
        } else if (!hasMultipleSources) {
            titleHtml = `<a href="${sources[0]}" target="_blank" rel="noopener noreferrer"><h2 class="details-title">${this.escapeHtml(titleWithSeason)}</h2></a>`;
        } else {
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

        const subtitle = `${series.episodes_found} episodes found${series.episodes_expected ? ` of ${series.episodes_expected} expected` : ''}`;
        const sourceLinks = this.renderSourceLinks(sources, plexUrl);

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
                <div class="details-heading-block">
                    ${titleHtml}
                    ${metaSummary ? `<div class="episode-ratings">${this.escapeHtml(metaSummary)}</div>` : ''}
                    <div class="details-subtitle">${subtitle}</div>
                    <div class="details-badges-row">
                        <div class="status-badge ${statusClass}">${statusIcon} ${this.formatStatus(series.status)}</div>
                        <div class="status-badge status-${series._filterWatchStatus}">${this.getWatchStatusLabel(series._filterWatchStatus)}</div>
                        ${series.myanimelist_watch_status?.my_status ? `<div class="status-badge status-${series._filterMalStatus}">${this.escapeHtml(series.myanimelist_watch_status.my_status)}</div>` : ''}
                    </div>
                    ${sourceLinks}
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
            
            ${await this.renderEpisodeGrid(series)}
            
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-file-earmark-play"></i>
                    Files (${series.files.length})
                </h4>
                ${await this.renderFilesList(series)}
            </div>
        `;
    }

    getWatchStatusLabel(status) {
        const labels = {
            'fully-watched': 'Fully Watched',
            'partially-watched': 'In Progress',
            'not-watched': 'Not Watched'
        };
        return labels[status] || 'Unknown';
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
        const plexFile = this.getPreferredPlexFile(series);
        const plexWatchStatus = plexFile?.plex_watch_status || null;
        const plexUrl = this.buildPlexUrl(plexWatchStatus);
        let content = `
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

        if (plexWatchStatus) {
            content += `
                <div class="mt-3 file-info">
                    <div><strong>Plex Plays:</strong> ${plexWatchStatus.watch_count || 0}</div>
                    ${plexWatchStatus.last_watched ? `<div><strong>Last Watched:</strong> ${this.escapeHtml(String(plexWatchStatus.last_watched))}</div>` : ''}
                    ${plexWatchStatus.progress_percent > 0 && !plexWatchStatus.watched ? `<div><strong>Plex Progress:</strong> ${plexWatchStatus.progress_percent.toFixed(1)}%</div>` : ''}
                </div>
            `;
            if (plexUrl) {
                content += `<div class="external-links"><a href="${plexUrl}" target="_blank" rel="noopener noreferrer" class="plex-link"><span class="plex-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 4l8 8-8 8z"></path></svg></span><span>Open in Plex</span></a></div>`;
            }
        }

        const ratingInfo = this.renderSeriesRatingInfo(series);
        if (ratingInfo) {
            content += `<div class="mt-3">${ratingInfo}</div>`;
        }
        
        return content;
    }
    
    renderSeriesInfo(series) {
        const title_metadata = this.getTitleMetadata(series);
        const group_metadata = series.group_metadata || {};
        
        // Check if we have any metadata to show
        const hasYear = title_metadata.year;
        const hasGenres = title_metadata.genres && title_metadata.genres.length > 0;
        const hasTags = title_metadata.tags && title_metadata.tags.length > 0;
        const hasPlot = title_metadata.plot;
        const hasType = title_metadata.type;
        
        // Check for timestamps
        const hasCreated = group_metadata.avg_created_time;
        const hasModified = group_metadata.avg_modified_time;
        const hasAccess = group_metadata.avg_access_time;
        
        if (!hasYear && !hasGenres && !hasTags && !hasPlot && !hasType && !hasCreated && !hasModified && !hasAccess) {
            return ''; // Don't show the card if no metadata available
        }
        
        // Format timestamps
        let timestampHtml = '';
        if (hasCreated || hasModified || hasAccess) {
            timestampHtml = '<div class="mt-3"><strong>Average File Dates:</strong><br>';
            if (hasCreated) {
                timestampHtml += `<span class="text-muted">Created:</span> ${this.formatTimestamp(group_metadata.avg_created_time)}<br>`;
            }
            if (hasModified) {
                timestampHtml += `<span class="text-muted">Modified:</span> ${this.formatTimestamp(group_metadata.avg_modified_time)}<br>`;
            }
            if (hasAccess) {
                timestampHtml += `<span style="color: #999;">Accessed:</span> <span style="color: #999;">${this.formatTimestamp(group_metadata.avg_access_time)}</span><br>`;
            }
            timestampHtml += '</div>';
        }
        
        const genresHtml = hasGenres
            ? `<div><strong>Genres:</strong> <span>${title_metadata.genres.map(g => this.escapeHtml(g)).join(', ')}</span></div>`
            : '';

        return `
            <div class="details-card">
                <h4 class="card-title">
                    <i class="bi bi-info-circle"></i>
                    Series Information
                </h4>
                ${hasType ? `<p><strong>Type:</strong> ${this.escapeHtml(title_metadata.type)}</p>` : ''}
                ${hasYear ? `<p><strong>Year:</strong> ${title_metadata.year}</p>` : ''}
                ${genresHtml}
                ${hasTags ? this.renderTagsWithPopup(title_metadata.tags) : ''}
                ${hasPlot ? `<p><strong>Plot:</strong> ${this.escapeHtml(title_metadata.plot)}</p>` : ''}
                ${timestampHtml}
            </div>
        `;
    }
    
    renderMalInfo(series) {
        const malStatus = series.myanimelist_watch_status;
        
        if (!malStatus || !malStatus.my_status) {
            return ''; // Don't show the card if no MAL data
        }
        
        const seasonSpecificStatus = malStatus.my_status;
        const seasonSpecificIcon = malStatus.my_status;
        const seasonSpecificColor = malStatus.my_status;
        const preferredInfoSource = this.getPreferredInfoSource(series);
        
        const statusIcons = {
            'Watching': '👁️',
            'Completed': '✅',
            'Completed (Season)': '✅',
            'On-Hold': '⏸️',
            'Dropped': '❌',
            'Plan to Watch': '📋'
        };
        
        const statusColors = {
            'Watching': 'text-primary',
            'Completed': 'text-success', 
            'Completed (Season)': 'text-success',
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
                    ${preferredInfoSource ? `<div class="external-links"><a href="${preferredInfoSource.url}" target="_blank" rel="noopener noreferrer" class="external-link ${preferredInfoSource.name === 'MyAnimeList' ? 'mal-link' : (preferredInfoSource.name === 'IMDb' ? 'imdb-link' : '')}">Open on ${this.escapeHtml(preferredInfoSource.name)}</a></div>` : ''}
                    ${malStatus.my_score > 0 ? `
                    <div class="mal-score">
                        <strong>Score:</strong> <span class="score-value">${malStatus.my_score}/10</span>
                    </div>
                    ` : ''}
                    ${malStatus.my_watched_episodes !== undefined && malStatus.my_watched_episodes >= 0 ? `
                    <div class="mal-episodes">
                        <strong>MAL Episodes Watched:</strong> ${malStatus.my_watched_episodes}${malStatus.series_episodes ? ` / ${malStatus.series_episodes}` : ''}
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
    
    async renderEpisodeGrid(series) {
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
        
        // Process episodes in parallel
        const episodes = [];
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
            
            const file = episodeToFile[i];
            episodes.push({ i, className, title, file });
        }
        
        // Get all thumbnails in parallel
        const episodeHtmlArray = await Promise.all(episodes.map(async ({i, className, title, file}) => {
            let thumbBg = '';
            let enhancedTitle = title;
            
            if (file) {
                // Add timestamp info to tooltip
                const timestamps = [];
                if (file.created_time) {
                    timestamps.push(`Created: ${this.formatTimestamp(file.created_time)}`);
                }
                if (file.modified_time) {
                    timestamps.push(`Modified: ${this.formatTimestamp(file.modified_time)}`);
                }
                if (file.access_time) {
                    timestamps.push(`Accessed: ${this.formatTimestamp(file.access_time)}`);
                }
                if (timestamps.length > 0) {
                    enhancedTitle += '&#10;' + timestamps.join('&#10;');
                }
                
                const thumb = await this.getThumbnailData(file);
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
            }
            
            // Determine status icon
            let icon = '';
            if (watched.has(i)) {
                icon = statusIcons.watched;
            } else if (found.has(i)) {
                icon = statusIcons.found;
            } else if (extra.has(i)) {
                icon = statusIcons.extra;
            } else {
                icon = statusIcons.missing;
            }
            
            return `<div class="${className}" title="${enhancedTitle}">${thumbBg}<span class="ep-corner ep-num-corner">${i}</span><span class="ep-corner ep-status-corner">${icon}</span></div>`;
        }));
        
        grid += episodeHtmlArray.join('');
        grid += '</div></div>';
        return grid;
    }
    
    async renderFilesList(series) {
        if (!series.files || series.files.length === 0) {
            return '<p class="text-muted">No files found</p>';
        }
        
        const fileItemsHtml = await Promise.all(series.files.map(async (file, idx) => {
            const watchIndicator = file.episode_watched ? 'watched' : (file.plex_watch_status?.view_offset > 0 ? 'partially-watched' : 'unwatched');
            const thumb = await this.getThumbnailData(file);
            const staticUrl = thumb && thumb.static_thumbnail ? thumb.static_thumbnail.replace(/\\/g, '/') : null;
            const animUrl = thumb && thumb.animated_thumbnail ? thumb.animated_thumbnail.replace(/\\/g, '/') : null;
            const imgId = `file-thumb-img-${idx}`;
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
            const plexUrl = this.buildPlexUrl(file.plex_watch_status);
            const plexSummary = file.plex_watch_status
                ? [`${file.plex_watch_status.watch_count || 0} plays`, file.plex_watch_status.progress_percent > 0 && !file.plex_watch_status.watched ? `${file.plex_watch_status.progress_percent.toFixed(0)}% progress` : '', file.plex_watch_status.last_watched ? `Last ${this.escapeHtml(String(file.plex_watch_status.last_watched))}` : ''].filter(Boolean).join(' • ')
                : '';
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
                        ${plexSummary ? `<div class="file-meta file-meta-plex"><span><i class="bi bi-badge-hd"></i>${plexSummary}</span>${plexUrl ? `<a href="${plexUrl}" target="_blank" rel="noopener noreferrer" class="plex-link"><span class="plex-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 4l8 8-8 8z"></path></svg></span><span>Plex</span></a>` : ''}</div>` : ''}
                        <div class="file-meta" style="margin-top: 4px; font-size: 0.85em;">
                            ${file.created_time ? `<span title="Created"><i class="bi bi-calendar-plus"></i>${this.formatTimestamp(file.created_time)}</span>` : ''}
                            ${file.modified_time ? `<span title="Modified"><i class="bi bi-pencil"></i>${this.formatTimestamp(file.modified_time)}</span>` : ''}
                            ${file.access_time ? `<span title="Accessed" style="color: #999;"><i class="bi bi-clock-history"></i>${this.formatTimestamp(file.access_time)}</span>` : ''}
                        </div>
                    </div>
                </div>
            `;
        }));
        
        return `<div class="files-list">${fileItemsHtml.join('')}</div>`;
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
    
    async showSeriesPopup(event, seriesKey) {
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
        
        // Set thumbnail (await the async call)
        const firstFile = series.files && series.files[0];
        const thumbnailData = firstFile ? await this.getThumbnailData(firstFile) : null;
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
        const hiddenTagsText = hiddenTags.join(', ');

        if (tags.length <= maxVisible) {
            return `<div><strong>Tags:</strong> <span class="tags-inline-list">${visibleTags.map((tag) => `<button type="button" class="tags-more tag-filter-chip" onclick="app.applyTagFilter(decodeURIComponent('${encodeURIComponent(tag)}'))">${this.escapeHtml(tag)}</button>`).join(' ')}</span></div>`;
        }

        const encodedHiddenTags = encodeURIComponent(hiddenTagsText);
        
        return `
            <div><strong>Tags:</strong> 
                <span class="tags-inline-list">${visibleTags.map((tag) => `<button type="button" class="tags-more tag-filter-chip" onclick="app.applyTagFilter(decodeURIComponent('${encodeURIComponent(tag)}'))">${this.escapeHtml(tag)}</button>`).join(' ')}</span>${hiddenCount > 0 ? ` <span class="tags-more" onmouseenter="app.showTagsPopup(event, decodeURIComponent('${encodedHiddenTags}'), ${hiddenCount})" onmouseleave="app.hideTagsPopup()">+${hiddenCount} more</span>` : ''}
            </div>
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
    
    formatTimestamp(timestamp) {
        if (!timestamp) return '';
        const date = new Date(timestamp * 1000); // Convert from Unix timestamp
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}`;
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
