(function () {
    const VIRTUAL_ROW_OVERSCAN = 10;
    const VIRTUAL_ROW_ESTIMATE = 44;

    const report = JSON.parse(document.getElementById("netflixWatchStatusReport").textContent);
    const rows = Array.isArray(report.rows) ? report.rows : [];
    const columns = Array.isArray(report.columns) ? report.columns : [];
    const nameColumn = columns.find((column) => column.key === "title") || { key: "title", header: "Title", align: "left" };
    const optionalColumns = columns.filter((column) => column.key !== nameColumn.key);
    const defaultColumnOrder = optionalColumns.map((column) => column.key);
    const defaultVisibleColumns = new Set(["year", "runtime_minutes", "average_rating", "genres"]);
    const rowMap = new Map(rows.map((row) => [row.id, row]));
    const childrenMap = new Map();

    rows.forEach((row) => {
        const key = row.parent_id || "ROOT";
        if (!childrenMap.has(key)) {
            childrenMap.set(key, []);
        }
        childrenMap.get(key).push(row);
    });

    const preferredDefaultColumnOrder = ["year", "runtime_minutes", "average_rating", "genres"];
    const state = {
        query: "",
        selectedId: rows[0] ? rows[0].id : null,
        expanded: new Set(),
        shownColumns: new Set(defaultColumnOrder.filter((key) => defaultVisibleColumns.has(key))),
        columnOrder: [
            ...preferredDefaultColumnOrder.filter((key) => defaultColumnOrder.includes(key)),
            ...defaultColumnOrder.filter((key) => !preferredDefaultColumnOrder.includes(key)),
        ],
        draggingColumnKey: null,
        dragInsertPosition: "before",
        showListThumbnails: false,
        virtualRows: [],
        virtualRowOffsets: [],
        virtualRowHeights: [],
        virtualHeightCache: new Map(),
        virtualTotalHeight: 0,
        virtualRenderScheduled: false,
        virtualForceRenderScheduled: false,
        lastVirtualRangeKey: "",
    };

    const elements = {
        appWindow: document.getElementById("appWindow"),
        sourceCsv: document.getElementById("sourceCsv"),
        searchInput: document.getElementById("searchInput"),
        listThumbnailToggle: document.getElementById("listThumbnailToggle"),
        columnPickerMenu: document.getElementById("columnPickerMenu"),
        columnOptions: document.getElementById("columnOptions"),
        treegridHeaderRow: document.getElementById("treegridHeaderRow"),
        resultBody: document.getElementById("resultBody"),
        treegridWrap: document.querySelector(".treegrid-wrap"),
        selectionTitle: document.getElementById("selectionTitle"),
        selectionSubtitle: document.getElementById("selectionSubtitle"),
        selectionDetails: document.getElementById("selectionDetails"),
        mediaPreview: document.getElementById("mediaPreview"),
    };

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/\"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatItemType(value) {
        const labels = {
            movie: "Movie",
            series: "Series",
            season: "Season",
            episode: "Episode",
        };
        return labels[value] || "Item";
    }

    function displayTitle(row) {
        return row.display_title || row.title || "";
    }

    function displayEpisodeTitle(row) {
        return row.display_episode_title || row.episode_title || "";
    }

    function hashString(value) {
        let hash = 0;
        for (let index = 0; index < value.length; index += 1) {
            hash = ((hash << 5) - hash) + value.charCodeAt(index);
            hash |= 0;
        }
        return Math.abs(hash);
    }

    function genrePalette(genre) {
        const normalized = String(genre || "").trim().toLowerCase();
        const paletteMap = {
            action: { bg: "rgba(178, 7, 16, 0.12)", border: "rgba(178, 7, 16, 0.22)", fg: "#8e1117" },
            adventure: { bg: "rgba(184, 115, 0, 0.12)", border: "rgba(184, 115, 0, 0.24)", fg: "#8a5a00" },
            animation: { bg: "rgba(0, 97, 168, 0.12)", border: "rgba(0, 97, 168, 0.22)", fg: "#00548f" },
            anime: { bg: "rgba(0, 97, 168, 0.12)", border: "rgba(0, 97, 168, 0.22)", fg: "#00548f" },
            comedy: { bg: "rgba(220, 120, 0, 0.12)", border: "rgba(220, 120, 0, 0.22)", fg: "#9a5600" },
            crime: { bg: "rgba(79, 70, 229, 0.12)", border: "rgba(79, 70, 229, 0.22)", fg: "#4338ca" },
            documentary: { bg: "rgba(44, 110, 73, 0.12)", border: "rgba(44, 110, 73, 0.22)", fg: "#235837" },
            drama: { bg: "rgba(114, 88, 56, 0.12)", border: "rgba(114, 88, 56, 0.22)", fg: "#6a5136" },
            family: { bg: "rgba(44, 110, 73, 0.12)", border: "rgba(44, 110, 73, 0.22)", fg: "#235837" },
            fantasy: { bg: "rgba(126, 34, 206, 0.12)", border: "rgba(126, 34, 206, 0.22)", fg: "#7e22ce" },
            history: { bg: "rgba(120, 53, 15, 0.12)", border: "rgba(120, 53, 15, 0.22)", fg: "#92400e" },
            horror: { bg: "rgba(17, 24, 39, 0.12)", border: "rgba(17, 24, 39, 0.22)", fg: "#111827" },
            music: { bg: "rgba(190, 24, 93, 0.12)", border: "rgba(190, 24, 93, 0.22)", fg: "#be185d" },
            musical: { bg: "rgba(190, 24, 93, 0.12)", border: "rgba(190, 24, 93, 0.22)", fg: "#be185d" },
            mystery: { bg: "rgba(67, 56, 202, 0.12)", border: "rgba(67, 56, 202, 0.22)", fg: "#4338ca" },
            romance: { bg: "rgba(225, 29, 72, 0.12)", border: "rgba(225, 29, 72, 0.22)", fg: "#be123c" },
            'sci-fi': { bg: "rgba(8, 145, 178, 0.12)", border: "rgba(8, 145, 178, 0.22)", fg: "#0f766e" },
            sciencefiction: { bg: "rgba(8, 145, 178, 0.12)", border: "rgba(8, 145, 178, 0.22)", fg: "#0f766e" },
            sport: { bg: "rgba(22, 101, 52, 0.12)", border: "rgba(22, 101, 52, 0.22)", fg: "#166534" },
            thriller: { bg: "rgba(124, 58, 237, 0.12)", border: "rgba(124, 58, 237, 0.22)", fg: "#6d28d9" },
            war: { bg: "rgba(127, 29, 29, 0.12)", border: "rgba(127, 29, 29, 0.22)", fg: "#991b1b" },
            western: { bg: "rgba(146, 64, 14, 0.12)", border: "rgba(146, 64, 14, 0.22)", fg: "#92400e" },
        };

        if (paletteMap[normalized]) {
            return paletteMap[normalized];
        }

        const hue = hashString(normalized) % 360;
        return {
            bg: `hsla(${hue}, 70%, 92%, 0.96)`,
            border: `hsla(${hue}, 45%, 58%, 0.34)`,
            fg: `hsl(${hue}, 60%, 30%)`,
        };
    }

    function genreBadgesMarkup(value) {
        const genres = String(value || "")
            .split(",")
            .map((genre) => genre.trim())
            .filter(Boolean);

        if (!genres.length) {
            return "";
        }

        return `<div class="genre-badges">${genres.map((genre) => {
            const palette = genrePalette(genre);
            return `<span class="genre-badge" style="--genre-bg:${escapeHtml(palette.bg)}; --genre-border:${escapeHtml(palette.border)}; --genre-fg:${escapeHtml(palette.fg)};">${escapeHtml(genre)}</span>`;
        }).join("")}</div>`;
    }

    function formatEpisodeTreeSubtitle(row) {
        const parts = [];

        if (row.season) {
            parts.push(`Season ${row.season}`);
        }
        if (row.episode) {
            parts.push(`Episode ${row.episode}`);
        }

        let subtitle = parts.join(" • ");
        if (row.watch_dates) {
            subtitle = subtitle ? `${subtitle} • Watched ${row.watch_dates}` : `Watched ${row.watch_dates}`;
        }
        return subtitle;
    }

    function sourceLinkInfo(row) {
        const sourceId = String(row.source_id || "").trim();
        if (!sourceId) {
            return null;
        }
        if (/^tt\d+$/i.test(sourceId)) {
            return {
                label: "IMDB",
                url: `https://www.imdb.com/title/${encodeURIComponent(sourceId)}/`,
            };
        }
        if (/^\d+$/.test(sourceId)) {
            return {
                label: "MAL",
                url: `https://myanimelist.net/anime/${encodeURIComponent(sourceId)}`,
            };
        }
        return null;
    }

    function sourceBadgeThemeClass(sourceLink) {
        if (!sourceLink || !sourceLink.label) {
            return "";
        }
        return `source-pill-${String(sourceLink.label).trim().toLowerCase()}`;
    }

    function hasAvailableThumbnails() {
        return rows.some((row) => row.thumbnail && row.thumbnail.url);
    }

    function syncListThumbnailMode() {
        const available = hasAvailableThumbnails();
        elements.listThumbnailToggle.disabled = !available;
        elements.listThumbnailToggle.hidden = !available;
        elements.listThumbnailToggle.classList.toggle("is-active", available && state.showListThumbnails);
        elements.listThumbnailToggle.setAttribute("aria-pressed", available && state.showListThumbnails ? "true" : "false");
        elements.appWindow.classList.toggle("list-thumbnails-mode", available && state.showListThumbnails);
    }

    function visibleOptionalColumns() {
        return state.columnOrder
            .map((key) => optionalColumns.find((column) => column.key === key))
            .filter(Boolean)
            .filter((column) => state.shownColumns.has(column.key));
    }

    function orderedOptionalColumns() {
        return state.columnOrder
            .map((key) => optionalColumns.find((column) => column.key === key))
            .filter(Boolean);
    }

    function moveColumnBefore(sourceKey, targetKey) {
        if (!sourceKey || !targetKey || sourceKey === targetKey) {
            return;
        }
        const sourceIndex = state.columnOrder.indexOf(sourceKey);
        const targetIndex = state.columnOrder.indexOf(targetKey);
        if (sourceIndex < 0 || targetIndex < 0) {
            return;
        }
        const nextOrder = [...state.columnOrder];
        nextOrder.splice(sourceIndex, 1);
        const insertIndex = nextOrder.indexOf(targetKey);
        nextOrder.splice(insertIndex, 0, sourceKey);
        state.columnOrder = nextOrder;
    }

    function moveColumnRelative(sourceKey, targetKey, position) {
        if (!sourceKey || !targetKey || sourceKey === targetKey) {
            return;
        }
        if (position === "before") {
            moveColumnBefore(sourceKey, targetKey);
            return;
        }

        const sourceIndex = state.columnOrder.indexOf(sourceKey);
        const targetIndex = state.columnOrder.indexOf(targetKey);
        if (sourceIndex < 0 || targetIndex < 0) {
            return;
        }

        const nextOrder = [...state.columnOrder];
        nextOrder.splice(sourceIndex, 1);
        const adjustedTargetIndex = nextOrder.indexOf(targetKey);
        nextOrder.splice(adjustedTargetIndex + 1, 0, sourceKey);
        state.columnOrder = nextOrder;
    }

    function clearDragState() {
        state.draggingColumnKey = null;
        state.dragInsertPosition = "before";
        elements.treegridHeaderRow.querySelectorAll(".optional-column-header").forEach((header) => {
            header.classList.remove("drag-over");
            header.classList.remove("insert-before");
            header.classList.remove("insert-after");
        });
    }

    function closeColumnMenu() {
        elements.columnPickerMenu.hidden = true;
    }

    function openColumnMenu(clientX, clientY) {
        elements.columnPickerMenu.hidden = false;
        const menu = elements.columnPickerMenu;
        menu.style.left = "0px";
        menu.style.top = "0px";
        const rect = menu.getBoundingClientRect();
        const maxLeft = Math.max(8, window.innerWidth - rect.width - 8);
        const maxTop = Math.max(8, window.innerHeight - rect.height - 8);
        menu.style.left = `${Math.min(clientX, maxLeft)}px`;
        menu.style.top = `${Math.min(clientY, maxTop)}px`;
    }

    function buildAncestors(row) {
        const ancestors = [];
        let current = row;
        while (current && current.parent_id) {
            current = rowMap.get(current.parent_id) || null;
            if (current) {
                ancestors.unshift(current);
            }
        }
        return ancestors;
    }

    function rowMatches(row, query) {
        if (!query) {
            return true;
        }
        return String(row.search_text || "").includes(query);
    }

    function branchMatches(row, query) {
        if (rowMatches(row, query)) {
            return true;
        }
        const children = childrenMap.get(row.id) || [];
        return children.some((child) => branchMatches(child, query));
    }

    function flattenVisibleRows(parentId) {
        const visible = [];
        const siblings = childrenMap.get(parentId || "ROOT") || [];
        const queryActive = Boolean(state.query);

        siblings.forEach((row) => {
            if (!branchMatches(row, state.query)) {
                return;
            }
            visible.push(row);
            if (row.has_children && (queryActive || state.expanded.has(row.id))) {
                visible.push(...flattenVisibleRows(row.id));
            }
        });

        return visible;
    }

    function subtitleForRow(row, ancestors) {
        if (row.item_type === "episode") {
            const chain = ancestors.map((item) => item.title).filter(Boolean);
            return chain.join(" / ");
        }
        if (row.item_type === "season") {
            return ancestors[0] ? ancestors[0].title : "";
        }
        if (row.item_type === "movie") {
            return row.year ? `Released ${row.year}` : "Movie";
        }
        if (row.item_type === "series") {
            return row.episode ? `Progress ${row.episode}` : "Series";
        }
        return "";
    }

    function selectionItems(items) {
        const filtered = items.filter((item) => item.value !== "" && item.value !== null && item.value !== undefined);
        if (!filtered.length) {
            return '<div class="empty-state">No details available.</div>';
        }
        return filtered.map((item) => `
            <div class="selection-item">
                <span class="selection-item-label">${escapeHtml(item.label)}</span>
                <span class="selection-item-value">${escapeHtml(item.value)}</span>
            </div>
        `).join("");
    }

    function renderColumnOptions() {
        elements.columnOptions.innerHTML = orderedOptionalColumns().map((column) => `
            <label class="column-option">
                <input class="column-toggle-input" type="checkbox" value="${escapeHtml(column.key)}" ${state.shownColumns.has(column.key) ? "checked" : ""}>
                <span class="column-toggle" aria-hidden="true"></span>
                <span class="column-option-label">${escapeHtml(column.header || column.key)}</span>
            </label>
        `).join("");
    }

    function renderPreview(row) {
        const thumbnail = row.thumbnail || {};
        const status = thumbnail.status || "not_requested";
        elements.mediaPreview.dataset.thumbnailStatus = status;
        if (thumbnail.url) {
            elements.mediaPreview.innerHTML = `<img src="${escapeHtml(thumbnail.url)}" alt="${escapeHtml(thumbnail.alt || row.title)}">`;
            return;
        }
        elements.mediaPreview.innerHTML = `
            <div class="media-preview-placeholder">
                <span class="eyebrow">Artwork</span>
                <strong>No thumbnail available</strong>
                <p>Status: ${escapeHtml(status.replace(/_/g, " "))}</p>
            </div>
        `;
    }

    function renderSelection() {
        const row = state.selectedId ? rowMap.get(state.selectedId) : null;
        if (!row) {
            elements.selectionTitle.textContent = "No selection";
            elements.selectionSubtitle.textContent = "";
            elements.selectionDetails.innerHTML = '<div class="empty-state">Select a row to inspect its details.</div>';
            renderPreview({ thumbnail: { status: "not_requested" } });
            return;
        }

        const ancestors = buildAncestors(row);
        elements.selectionTitle.textContent = displayTitle(row) || formatItemType(row.item_type);
        elements.selectionSubtitle.textContent = subtitleForRow(row, ancestors);

        renderPreview(row);

        const detailItems = [
            { label: "Type", value: formatItemType(row.item_type) },
            { label: "Title", value: displayTitle(row) },
            { label: "Release Year", value: row.year },
            { label: "Season", value: row.season },
            { label: "Season Title", value: row.season_title },
            { label: "Episode", value: row.episode },
            { label: "Episode Title", value: displayEpisodeTitle(row) },
            { label: "Watch Status", value: row.watch_state === "aggregate" ? "Aggregate" : row.watch_state },
            { label: "Views", value: row.views },
            { label: "Watch Dates", value: row.watch_dates },
        ];
        elements.selectionDetails.innerHTML = selectionItems(detailItems);
    }

    function renderHeader() {
        const gutterHeader = '<th class="gutter-column" aria-hidden="true"></th>';
        const trailingGutterHeader = '<th class="gutter-column right-gutter-column" aria-hidden="true"></th>';
        const fixedHeader = `
            <th class="column-${escapeHtml(nameColumn.key)} ${nameColumn.align === "right" ? "align-right" : ""} ${nameColumn.align === "center" ? "align-center" : ""}">
                <span class="column-header-button is-fixed">${escapeHtml(nameColumn.header || nameColumn.key)}</span>
            </th>
        `;
        const dataHeaders = visibleOptionalColumns().map((column, index) => `
            <th class="optional-column-header column-${escapeHtml(column.key)} ${column.align === "right" ? "align-right" : ""} ${column.align === "center" ? "align-center" : ""}" data-column-key="${escapeHtml(column.key)}" draggable="true">
                <span class="column-header-button">${escapeHtml(column.header || column.key || `Column ${index + 1}`)}</span>
            </th>
        `).join("");
        elements.treegridHeaderRow.innerHTML = gutterHeader + fixedHeader + dataHeaders + trailingGutterHeader;
    }

    function toggleRow(rowId) {
        if (state.expanded.has(rowId)) {
            state.expanded.delete(rowId);
        } else {
            state.expanded.add(rowId);
        }
        renderRows();
    }

    function rowSubtitle(row) {
        if (row.item_type === "series") {
            return row.year ? `${row.year}` : "Series";
        }
        if (row.item_type === "season") {
            return row.episode ? `Progress ${row.episode}` : "Season";
        }
        if (row.item_type === "episode") {
            return formatEpisodeTreeSubtitle(row);
        }
        if (row.watch_dates) {
            return `Watched ${row.watch_dates}`;
        }
        return "";
    }

    function titleCellMarkup(row) {
        const expanded = state.expanded.has(row.id);
        const toggleLabel = expanded ? "Collapse" : "Expand";
        const thumbnail = row.thumbnail || {};
        const subtitle = rowSubtitle(row);
        const sourceLink = sourceLinkInfo(row);
        const sourceBadgeMarkup = sourceLink
            ? `<a class="source-badge-link source-pill ${escapeHtml(sourceBadgeThemeClass(sourceLink))}" href="${escapeHtml(sourceLink.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(sourceLink.label)}</a>`
            : "";
        const thumbnailMarkup = state.showListThumbnails
            ? (thumbnail.url
                ? `<span class="row-thumbnail-shell"><img class="row-thumbnail" src="${escapeHtml(thumbnail.url)}" alt="${escapeHtml(thumbnail.alt || displayTitle(row))}" loading="lazy"></span>`
                : '<span class="row-thumbnail-shell is-empty" aria-hidden="true"></span>')
            : "";
        return `
            <div class="treecell ${row.watch_state === "unwatched" ? "is-unwatched" : ""}" style="--level:${row.level}">
                <span class="tree-indent"></span>
                <button class="tree-toggle ${row.has_children ? "" : "is-hidden"}" data-action="toggle" data-row-id="${escapeHtml(row.id)}" aria-label="${escapeHtml(toggleLabel)}">
                    ${row.has_children ? (expanded ? "−" : "+") : ""}
                </button>
                <div class="tree-name-button">
                    ${thumbnailMarkup}
                    <div class="tree-text-badge-group">
                        <div class="tree-title-block">
                            <div class="tree-title-line">
                                <button class="tree-title-select tree-title" data-action="select" data-row-id="${escapeHtml(row.id)}">${escapeHtml(displayTitle(row))}</button>
                            </div>
                            <div class="tree-subtitle-line">
                                <span class="tree-subtitle">${escapeHtml(subtitle)}</span>
                            </div>
                        </div>
                        ${sourceBadgeMarkup}
                    </div>
                </div>
            </div>
        `;
    }

    function cellMarkup(row, column) {
        if (column.key === "title") {
            return titleCellMarkup(row);
        }
        if (column.key === "episode_title" && row.item_type !== "episode") {
            return displayEpisodeTitle(row) ? escapeHtml(displayEpisodeTitle(row)) : `<span class="item-pill type-${escapeHtml(row.item_type)}">${escapeHtml(formatItemType(row.item_type))}</span>`;
        }
        if (column.key === "episode_title") {
            return escapeHtml(displayEpisodeTitle(row));
        }
        if (column.key === "source_id") {
            const sourceLink = sourceLinkInfo(row);
            if (sourceLink) {
                return `<a class="source-id-link" href="${escapeHtml(sourceLink.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(row[column.key] || "")}</a>`;
            }
            return escapeHtml(row[column.key] || "");
        }
        if (column.key === "genres") {
            return genreBadgesMarkup(row[column.key]);
        }
        if (column.key === "title") {
            return escapeHtml(displayTitle(row));
        }
        return escapeHtml(row[column.key] || "");
    }

    function gutterCellMarkup(row) {
        const statusLabel = row.watch_state === "aggregate" ? "" : row.watch_state;
        const statusClass = row.watch_state === "watched"
            ? "is-watched"
            : row.watch_state === "unwatched"
                ? "is-unwatched"
                : "is-empty";
        return `<span class="status-gutter ${statusClass}" aria-label="${escapeHtml(statusLabel)}"></span>`;
    }

    function visibleColumnKeys() {
        return [nameColumn.key, ...visibleOptionalColumns().map((column) => column.key), state.showListThumbnails ? "thumbs" : "plain"].join("|");
    }

    function virtualRowCacheKey(row) {
        return `${row.id}|${row.level}|${row.watch_state}|${visibleColumnKeys()}`;
    }

    function virtualRowHeight(row) {
        return state.virtualHeightCache.get(virtualRowCacheKey(row)) || VIRTUAL_ROW_ESTIMATE;
    }

    function recalculateVirtualMetrics() {
        const offsets = new Array(state.virtualRows.length);
        const heights = new Array(state.virtualRows.length);
        let runningOffset = 0;

        for (let index = 0; index < state.virtualRows.length; index += 1) {
            const row = state.virtualRows[index];
            const height = virtualRowHeight(row);
            offsets[index] = runningOffset;
            heights[index] = height;
            runningOffset += height;
        }

        state.virtualRowOffsets = offsets;
        state.virtualRowHeights = heights;
        state.virtualTotalHeight = runningOffset;
    }

    function findVirtualStartIndex(scrollTop) {
        let low = 0;
        let high = state.virtualRowOffsets.length - 1;
        let result = 0;

        while (low <= high) {
            const mid = Math.floor((low + high) / 2);
            const itemTop = state.virtualRowOffsets[mid];
            const itemBottom = itemTop + state.virtualRowHeights[mid];

            if (itemBottom >= scrollTop) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return result;
    }

    function currentVirtualRange() {
        if (!state.virtualRows.length) {
            return { start: 0, end: -1, topOffset: 0, bottomOffset: 0 };
        }

        const scrollTop = elements.treegridWrap ? elements.treegridWrap.scrollTop || 0 : 0;
        const viewportHeight = elements.treegridWrap ? elements.treegridWrap.clientHeight || 0 : 0;
        const viewportBottom = scrollTop + viewportHeight;
        const firstVisibleIndex = findVirtualStartIndex(scrollTop);
        let end = firstVisibleIndex;

        while (end < state.virtualRows.length && state.virtualRowOffsets[end] < viewportBottom) {
            end += 1;
        }

        const start = Math.max(0, firstVisibleIndex - VIRTUAL_ROW_OVERSCAN);
        const finalEnd = Math.min(state.virtualRows.length - 1, end + VIRTUAL_ROW_OVERSCAN);
        const topOffset = state.virtualRowOffsets[start] || 0;
        const renderedHeight = finalEnd >= start
            ? (state.virtualRowOffsets[finalEnd] + state.virtualRowHeights[finalEnd]) - topOffset
            : 0;
        const bottomOffset = Math.max(0, state.virtualTotalHeight - topOffset - renderedHeight);

        return { start, end: finalEnd, topOffset, bottomOffset };
    }

    function scheduleVirtualRender(force) {
        if (force) {
            state.virtualForceRenderScheduled = true;
        }

        if (state.virtualRenderScheduled) {
            return;
        }

        state.virtualRenderScheduled = true;
        requestAnimationFrame(() => {
            const shouldForce = state.virtualForceRenderScheduled;
            state.virtualRenderScheduled = false;
            state.virtualForceRenderScheduled = false;
            renderVirtualBody(shouldForce);
        });
    }

    function measureVisibleRows() {
        const renderedRows = elements.resultBody.querySelectorAll("tr[data-virtual-index]");
        let hasChanged = false;

        renderedRows.forEach((rowElement) => {
            const index = Number(rowElement.dataset.virtualIndex);
            if (!Number.isInteger(index) || index < 0 || index >= state.virtualRows.length) {
                return;
            }

            const row = state.virtualRows[index];
            const key = virtualRowCacheKey(row);
            const measuredHeight = Math.ceil(rowElement.getBoundingClientRect().height);
            if (!measuredHeight) {
                return;
            }

            if (state.virtualHeightCache.get(key) !== measuredHeight) {
                state.virtualHeightCache.set(key, measuredHeight);
                hasChanged = true;
            }
        });

        if (hasChanged) {
            scheduleVirtualRender(true);
        }
    }

    function renderVirtualRow(row, index, visibleColumns) {
        return `
            <tr class="tree-row ${row.id === state.selectedId ? "is-selected" : ""} ${row.watch_state === "unwatched" ? "is-unwatched" : ""}" data-row-id="${escapeHtml(row.id)}" data-virtual-index="${index}">
                <td class="gutter-column">${gutterCellMarkup(row)}</td>
                <td class="column-${escapeHtml(nameColumn.key)} ${nameColumn.align === "right" ? "align-right" : ""} ${nameColumn.align === "center" ? "align-center" : ""}">${cellMarkup(row, nameColumn)}</td>
                ${visibleColumns.map((column) => `<td class="column-${escapeHtml(column.key)} ${column.align === "right" ? "align-right" : ""} ${column.align === "center" ? "align-center" : ""}">${cellMarkup(row, column)}</td>`).join("")}
                <td class="gutter-column right-gutter-column"></td>
            </tr>
        `;
    }

    function renderVirtualBody(forceMeasurement) {
        recalculateVirtualMetrics();
        const visibleColumns = visibleOptionalColumns();
        const colSpan = 2 + visibleColumns.length;

        if (!state.virtualRows.length) {
            elements.resultBody.innerHTML = `<tr><td colspan="${colSpan}"><div class="empty-state">No entries match the current filters.</div></td></tr>`;
            return;
        }

        const range = currentVirtualRange();
        const rangeKey = `${range.start}:${range.end}:${range.topOffset}:${range.bottomOffset}:${state.selectedId || ""}`;
        if (!forceMeasurement && rangeKey === state.lastVirtualRangeKey) {
            return;
        }

        state.lastVirtualRangeKey = rangeKey;

        let html = "";
        if (range.topOffset > 0) {
            html += `<tr class="virtual-spacer-row" aria-hidden="true"><td class="virtual-spacer-cell" colspan="${colSpan}" style="height:${range.topOffset}px"></td></tr>`;
        }

        for (let index = range.start; index <= range.end; index += 1) {
            html += renderVirtualRow(state.virtualRows[index], index, visibleColumns);
        }

        if (range.bottomOffset > 0) {
            html += `<tr class="virtual-spacer-row" aria-hidden="true"><td class="virtual-spacer-cell" colspan="${colSpan}" style="height:${range.bottomOffset}px"></td></tr>`;
        }

        elements.resultBody.innerHTML = html;

        if (forceMeasurement) {
            measureVisibleRows();
        } else {
            requestAnimationFrame(() => {
                measureVisibleRows();
            });
        }
    }

    function renderRows() {
        state.virtualRows = flattenVisibleRows(null);
        state.lastVirtualRangeKey = "";
        renderVirtualBody(true);
    }

    function render() {
        syncListThumbnailMode();
        renderHeader();
        renderRows();
        renderSelection();
    }

    elements.resultBody.addEventListener("click", (event) => {
        const target = event.target.closest("[data-row-id]");
        if (!target) {
            return;
        }
        const rowId = target.getAttribute("data-row-id");
        const action = target.getAttribute("data-action") || "select";
        if (!rowId) {
            return;
        }
        if (action === "toggle") {
            toggleRow(rowId);
            return;
        }
        state.selectedId = rowId;
        render();
    });

    elements.searchInput.addEventListener("input", (event) => {
        state.query = String(event.target.value || "").trim().toLowerCase();
        render();
    });

    elements.listThumbnailToggle.addEventListener("click", () => {
        if (elements.listThumbnailToggle.disabled) {
            return;
        }
        state.showListThumbnails = !state.showListThumbnails;
        render();
    });

    elements.treegridHeaderRow.addEventListener("dragstart", (event) => {
        const header = event.target.closest("th.optional-column-header[data-column-key]");
        if (!header) {
            return;
        }
        state.draggingColumnKey = header.dataset.columnKey || null;
        if (event.dataTransfer) {
            event.dataTransfer.effectAllowed = "move";
            event.dataTransfer.setData("text/plain", state.draggingColumnKey || "");
        }
    });

    elements.treegridHeaderRow.addEventListener("dragover", (event) => {
        const header = event.target.closest("th.optional-column-header");
        if (!header || !state.draggingColumnKey) {
            return;
        }
        event.preventDefault();
        const rect = header.getBoundingClientRect();
        state.dragInsertPosition = event.clientX < rect.left + rect.width / 2 ? "before" : "after";
        elements.treegridHeaderRow.querySelectorAll(".optional-column-header").forEach((candidate) => {
            const isActive = candidate === header;
            candidate.classList.toggle("drag-over", isActive);
            candidate.classList.toggle("insert-before", isActive && state.dragInsertPosition === "before");
            candidate.classList.toggle("insert-after", isActive && state.dragInsertPosition === "after");
        });
    });

    elements.treegridHeaderRow.addEventListener("drop", (event) => {
        const header = event.target.closest("th.optional-column-header");
        if (!header || !state.draggingColumnKey) {
            return;
        }
        event.preventDefault();
        moveColumnRelative(state.draggingColumnKey, header.dataset.columnKey || "", state.dragInsertPosition);
        clearDragState();
        renderColumnOptions();
        render();
    });

    elements.treegridHeaderRow.addEventListener("dragend", () => {
        clearDragState();
    });

    elements.treegridHeaderRow.addEventListener("contextmenu", (event) => {
        event.preventDefault();
        openColumnMenu(event.clientX, event.clientY);
    });

    elements.columnOptions.addEventListener("change", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) {
            return;
        }
        if (target.checked) {
            state.shownColumns.add(target.value);
        } else {
            state.shownColumns.delete(target.value);
        }
        renderColumnOptions();
        render();
    });

    document.addEventListener("click", (event) => {
        if (!elements.columnPickerMenu.hidden && !event.target.closest("#columnPickerMenu")) {
            closeColumnMenu();
        }
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeColumnMenu();
        }
    });

    if (elements.treegridWrap) {
        elements.treegridWrap.addEventListener("scroll", () => {
            scheduleVirtualRender(false);
        }, { passive: true });
    }

    window.addEventListener("resize", () => {
        state.virtualHeightCache.clear();
        state.lastVirtualRangeKey = "";
        scheduleVirtualRender(true);
    });

    elements.sourceCsv.textContent = report.meta && report.meta.source_csv ? report.meta.source_csv : "";
    renderColumnOptions();
    render();
}());
