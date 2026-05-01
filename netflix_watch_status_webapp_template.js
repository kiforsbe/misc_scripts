(function () {
    const FILTER_INPUT_DEBOUNCE_MS = 250;
    const VIRTUAL_ROW_OVERSCAN = 10;
    const VIRTUAL_ROW_ESTIMATE = 44;

    const report = JSON.parse(document.getElementById("netflixWatchStatusReport").textContent);
    const rows = Array.isArray(report.rows) ? report.rows : [];
    const columns = Array.isArray(report.columns) ? report.columns : [];
    const nameColumn = columns.find((column) => column.key === "title") || { key: "title", header: "Title", align: "left" };
    const optionalColumns = columns.filter((column) => column.key !== nameColumn.key);
    const defaultColumnOrder = optionalColumns.map((column) => column.key);
    const defaultVisibleColumns = new Set(["year", "runtime_minutes", "average_rating"]);
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
    const SMART_FILTER_FIELD_INFO = [
        { name: "title", field: "title", hint: "Top-level title text" },
        { name: "type", field: "item_type", hint: "movie, series, season, episode" },
        { name: "state", field: "watch_state", hint: "watched, partial, unwatched" },
        { name: "releasedate", field: "release_date", hint: "Date-aware release filter" },
        { name: "season", field: "season", hint: "Season number" },
        { name: "season_title", field: "season_title", hint: "Season title text" },
        { name: "episode", field: "episode", hint: "Episode number" },
        { name: "episode_title", field: "episode_title", hint: "Episode title text" },
        { name: "views", field: "views", hint: "Watch count" },
        { name: "watchdate", field: "watch_date", hint: "Date-aware watch filter" },
        { name: "runtime", field: "runtime_minutes", hint: "Accepts 30m, 1h20m" },
        { name: "rating", field: "average_rating", hint: "Numeric rating" },
        { name: "votes", field: "num_votes", hint: "Vote count" },
        { name: "genres", field: "genres", hint: "Genre text" },
        { name: "source", field: "source_id", hint: "Source identifier" },
        { name: "title_type", field: "title_type", hint: "Provider title type" },
        { name: "id", field: "id", hint: "Row id" },
        { name: "parent", field: "parent_id", hint: "Parent row id" },
        { name: "level", field: "level", hint: "Tree level" },
        { name: "progress", field: "progress", hint: "Count or percent watched" },
        { name: "total", field: "progress_total", hint: "Total episodes in aggregate" },
        { name: "children", field: "has_children", hint: "Items with child rows" },
    ];

    const smartFilterFieldNameByCanonical = new Map(SMART_FILTER_FIELD_INFO.map((entry) => [entry.field, entry.name]));
    const smartFilterStaticValueTemplates = {
        item_type: [
            { value: "movie", hint: "Type" },
            { value: "series", hint: "Type" },
            { value: "season", hint: "Type" },
            { value: "episode", hint: "Type" },
        ],
        watch_state: [
            { value: "watched", hint: "State" },
            { value: "partial", hint: "State" },
            { value: "unwatched", hint: "State" },
            { value: "aggregate", hint: "State" },
        ],
        release_date: [
            { value: "<2018", hint: "Before Jan 1, 2018" },
            { value: "2018", hint: "Any date in 2018" },
            { value: "2018-05", hint: "Any date in May 2018" },
            { value: "2018..2020", hint: "Date range" },
        ],
        watch_date: [
            { value: "2024-01..2024-03", hint: "Month range" },
            { value: ">=2024-02-15", hint: "Since specific date" },
            { value: "2024", hint: "Any date in year" },
        ],
        runtime_minutes: [
            { value: "<30m", hint: "Less than 30 minutes" },
            { value: "1h20m", hint: "Duration value" },
            { value: "45m..90m", hint: "Duration range" },
            { value: ">=2h", hint: "Comparison" },
        ],
        average_rating: [
            { value: ">=8", hint: "Comparison" },
            { value: "7..9", hint: "Range" },
        ],
        num_votes: [
            { value: ">=1000", hint: "Comparison" },
            { value: "100..10000", hint: "Range" },
        ],
        progress: [
            { value: ">0", hint: "Started watching" },
            { value: "1..4", hint: "Watched count range" },
            { value: "<20%", hint: "Percent watched" },
            { value: ">=50%", hint: "Percent watched" },
        ],
        progress_total: [
            { value: ">=10", hint: "Comparison" },
            { value: "1..12", hint: "Episode total range" },
        ],
        has_children: [
            { value: "", hint: "Has children" },
            { value: "true", hint: "Boolean" },
            { value: "false", hint: "Boolean" },
        ],
    };

    const smartFilterDataValues = {
        genres: uniqueSortedValues(rows.flatMap((row) => String(row.genres || "").split(",").map((genre) => genre.trim()).filter(Boolean)), 100),
    };

    const state = {
        query: "",
        compiledFilter: () => true,
        compiledHighlightFilter: () => false,
        revealMatchingDescendants: false,
        revealTopLevelTitleDescendants: false,
        hasEpisodeTitleClause: false,
        filterError: "",
        filterInputTimer: null,
        selectedId: rows[0] ? rows[0].id : null,
        expanded: new Set(),
        filterCollapsed: new Set(),
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
        smartFilterSuggestions: [],
        activeSmartFilterSuggestionIndex: -1,
        lastSmartFilterSuggestionKey: "",
        smartFilterSuggestionRefreshScheduled: false,
        contextMenuRowId: null,
    };

    const elements = {
        appWindow: document.getElementById("appWindow"),
        searchInput: document.getElementById("searchInput"),
        smartFilterSuggestions: document.getElementById("smartFilterSuggestions"),
        smartFilterHelpButton: document.getElementById("smartFilterHelpButton"),
        smartFilterHelp: document.getElementById("smartFilterHelp"),
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
        statusTotalItems: document.getElementById("statusTotalItems"),
        statusFilteredItems: document.getElementById("statusFilteredItems"),
        statusShownRows: document.getElementById("statusShownRows"),
        statusWatchedItems: document.getElementById("statusWatchedItems"),
        statusPartialItems: document.getElementById("statusPartialItems"),
        statusUnwatchedItems: document.getElementById("statusUnwatchedItems"),
        statusWatchtime: document.getElementById("statusWatchtime"),
        rowContextMenu: document.getElementById("rowContextMenu"),
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

    function combinedDisplayText(preferredValue, fallbackValue) {
        const preferred = String(preferredValue || "").trim();
        const fallback = String(fallbackValue || "").trim();
        const comparablePreferred = preferred.replace(/\s+\*$/, "");
        const comparableFallback = fallback.replace(/\s+\*$/, "");
        if (!preferred) {
            return fallback;
        }
        if (!fallback) {
            return preferred;
        }
        return comparablePreferred.localeCompare(comparableFallback, undefined, { sensitivity: "base" }) === 0
            ? preferred
            : `${preferred} ${fallback}`;
    }

    function topLevelRow(row) {
        let current = row || null;
        while (current && current.parent_id) {
            current = rowMap.get(current.parent_id) || null;
        }
        return current;
    }

    function topLevelTitleText(row) {
        const topLevel = topLevelRow(row);
        return topLevel ? combinedDisplayText(displayTitle(topLevel), topLevel.title) : "";
    }

    function uniqueSortedValues(values, limit) {
        return [...new Set(values.map((value) => String(value || "").trim()).filter(Boolean))]
            .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base", numeric: true }))
            .slice(0, limit || Number.POSITIVE_INFINITY);
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

    function closeRowContextMenu() {
        if (!elements.rowContextMenu) {
            return;
        }
        elements.rowContextMenu.hidden = true;
        elements.rowContextMenu.innerHTML = "";
        state.contextMenuRowId = null;
    }

    function positionFloatingMenu(menu, clientX, clientY) {
        menu.style.left = "0px";
        menu.style.top = "0px";
        const rect = menu.getBoundingClientRect();
        const maxLeft = Math.max(8, window.innerWidth - rect.width - 8);
        const maxTop = Math.max(8, window.innerHeight - rect.height - 8);
        menu.style.left = `${Math.min(clientX, maxLeft)}px`;
        menu.style.top = `${Math.min(clientY, maxTop)}px`;
    }

    function openColumnMenu(clientX, clientY) {
        closeRowContextMenu();
        elements.columnPickerMenu.hidden = false;
        positionFloatingMenu(elements.columnPickerMenu, clientX, clientY);
    }

    function parseNumericValue(token) {
        return /^-?\d+$/.test(token) ? Number.parseInt(token, 10) : Number.parseFloat(token);
    }

    function parseNumericExpr(expr) {
        const trimmed = String(expr || "").trim();
        const approxMatch = trimmed.match(/^~\s*(-?\d+(?:\.\d+)?)\s*(?:±|\+\/-)\s*(\d+(?:\.\d+)?)$/);
        if (approxMatch) {
            const center = Number.parseFloat(approxMatch[1]);
            const delta = Number.parseFloat(approxMatch[2]);
            return (value) => center - delta <= value && value <= center + delta;
        }

        const moduloMatch = trimmed.match(/^%\s*(\d+)$/);
        if (moduloMatch) {
            const divisor = Number.parseInt(moduloMatch[1], 10);
            if (divisor === 0) {
                throw new Error("Modulo divisor cannot be zero");
            }
            return (value) => value % divisor === 0;
        }

        const rangeMatch = trimmed.match(/^\s*(-?\d+(?:\.\d+)?)\s*\.\.\s*(-?\d+(?:\.\d+)?)\s*$/);
        if (rangeMatch) {
            const lower = parseNumericValue(rangeMatch[1]);
            const upper = parseNumericValue(rangeMatch[2]);
            if (lower > upper) {
                throw new Error("Range lower bound cannot exceed upper bound");
            }
            return (value) => lower <= value && value <= upper;
        }

        if (trimmed.includes(",")) {
            const values = new Set(trimmed.split(",").map((part) => part.trim()).filter(Boolean).map(parseNumericValue));
            if (!values.size) {
                throw new Error("Empty enumeration expression");
            }
            return (value) => values.has(value);
        }

        const comparisonMatch = trimmed.match(/^(<=|>=|!=|=|<|>)\s*(-?\d+(?:\.\d+)?)$/);
        if (comparisonMatch) {
            const operator = comparisonMatch[1];
            const threshold = parseNumericValue(comparisonMatch[2]);
            if (operator === "=") {
                return (value) => value === threshold;
            }
            if (operator === "!=") {
                return (value) => value !== threshold;
            }
            if (operator === ">") {
                return (value) => value > threshold;
            }
            if (operator === ">=") {
                return (value) => value >= threshold;
            }
            if (operator === "<") {
                return (value) => value < threshold;
            }
            return (value) => value <= threshold;
        }

        if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
            const exact = parseNumericValue(trimmed);
            return (value) => value === exact;
        }

        throw new Error(`Unsupported numeric expression: ${trimmed}`);
    }

    function transformNumericExpr(expr, valueTransform) {
        const trimmed = String(expr || "").trim();
        const approxMatch = trimmed.match(/^~\s*([^±]+?)\s*(?:±|\+\/-)\s*(.+)$/);
        if (approxMatch) {
            return `~${valueTransform(approxMatch[1].trim())}±${valueTransform(approxMatch[2].trim())}`;
        }

        const moduloMatch = trimmed.match(/^%\s*(.+)$/);
        if (moduloMatch) {
            const value = valueTransform(moduloMatch[1].trim());
            if (!Number.isInteger(value)) {
                throw new Error("Modulo expressions require integer values");
            }
            return `%${value}`;
        }

        const rangeMatch = trimmed.match(/^(.+?)\.\.(.+)$/);
        if (rangeMatch) {
            return `${valueTransform(rangeMatch[1].trim())}..${valueTransform(rangeMatch[2].trim())}`;
        }

        const comparisonMatch = trimmed.match(/^(<=|>=|!=|=|<|>)(.+)$/);
        if (comparisonMatch) {
            return `${comparisonMatch[1]}${valueTransform(comparisonMatch[2].trim())}`;
        }

        if (trimmed.includes(",")) {
            return trimmed.split(",").map((part) => valueTransform(part.trim())).join(",");
        }

        return String(valueTransform(trimmed));
    }

    function normalizeRuntimeToken(token) {
        const trimmed = String(token || "").trim().toLowerCase();
        if (!trimmed) {
            throw new Error("Runtime filter cannot be empty");
        }
        if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
            return Number.parseFloat(trimmed);
        }

        let totalMinutes = 0;
        let matched = false;
        trimmed.replace(/(-?\d+(?:\.\d+)?)\s*([hm])/g, (_, valueText, unit) => {
            matched = true;
            const value = Number.parseFloat(valueText);
            totalMinutes += unit === "h" ? value * 60 : value;
            return "";
        });

        if (!matched) {
            throw new Error(`Invalid runtime token: ${token}`);
        }

        return totalMinutes;
    }

    function parseRuntimeExpr(expr) {
        return parseNumericExpr(transformNumericExpr(expr, normalizeRuntimeToken));
    }

    function utcTimestamp(year, month, day) {
        return Date.UTC(year, month - 1, day);
    }

    function endOfMonthDay(year, month) {
        return new Date(Date.UTC(year, month, 0)).getUTCDate();
    }

    function parseDatePeriod(token) {
        const trimmed = String(token || "").trim();
        if (!trimmed) {
            throw new Error("Date filter cannot be empty");
        }

        const fullDateMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (fullDateMatch) {
            const year = Number.parseInt(fullDateMatch[1], 10);
            const month = Number.parseInt(fullDateMatch[2], 10);
            const day = Number.parseInt(fullDateMatch[3], 10);
            if (month < 1 || month > 12) {
                throw new Error(`Invalid month in date token: ${token}`);
            }
            const maxDay = endOfMonthDay(year, month);
            if (day < 1 || day > maxDay) {
                throw new Error(`Invalid day in date token: ${token}`);
            }
            const timestamp = utcTimestamp(year, month, day);
            return { start: timestamp, end: timestamp };
        }

        const monthMatch = trimmed.match(/^(\d{4})-(\d{2})$/);
        if (monthMatch) {
            const year = Number.parseInt(monthMatch[1], 10);
            const month = Number.parseInt(monthMatch[2], 10);
            if (month < 1 || month > 12) {
                throw new Error(`Invalid month in date token: ${token}`);
            }
            return {
                start: utcTimestamp(year, month, 1),
                end: utcTimestamp(year, month, endOfMonthDay(year, month)),
            };
        }

        const yearMatch = trimmed.match(/^(\d{4})$/);
        if (yearMatch) {
            const year = Number.parseInt(yearMatch[1], 10);
            return {
                start: utcTimestamp(year, 1, 1),
                end: utcTimestamp(year, 12, 31),
            };
        }

        throw new Error(`Invalid date token: ${token}`);
    }

    function parseDateValues(value) {
        return [...new Set((String(value || "").match(/\d{4}-\d{2}-\d{2}/g) || []).map((token) => parseDatePeriod(token).start))];
    }

    function parseReleaseDateValues(value) {
        return parseYearValues(value).map((year) => utcTimestamp(year, 1, 1));
    }

    function parseDateExpr(expr) {
        const trimmed = String(expr || "").trim();
        if (!trimmed) {
            throw new Error("Date filter cannot be empty");
        }

        const rangeMatch = trimmed.match(/^(.+?)\.\.(.+)$/);
        if (rangeMatch) {
            const lower = parseDatePeriod(rangeMatch[1].trim());
            const upper = parseDatePeriod(rangeMatch[2].trim());
            if (lower.start > upper.end) {
                throw new Error("Date range lower bound cannot exceed upper bound");
            }
            return (value) => lower.start <= value && value <= upper.end;
        }

        if (trimmed.includes(",")) {
            const periods = trimmed.split(",").map((part) => parseDatePeriod(part.trim()));
            return (value) => periods.some((period) => period.start <= value && value <= period.end);
        }

        const comparisonMatch = trimmed.match(/^(<=|>=|!=|=|<|>)(.+)$/);
        if (comparisonMatch) {
            const operator = comparisonMatch[1];
            const period = parseDatePeriod(comparisonMatch[2].trim());
            if (operator === "=") {
                return (value) => period.start <= value && value <= period.end;
            }
            if (operator === "!=") {
                return (value) => value < period.start || value > period.end;
            }
            if (operator === ">") {
                return (value) => value > period.end;
            }
            if (operator === ">=") {
                return (value) => value >= period.start;
            }
            if (operator === "<") {
                return (value) => value < period.start;
            }
            return (value) => value <= period.end;
        }

        const period = parseDatePeriod(trimmed);
        return (value) => period.start <= value && value <= period.end;
    }

    function normalizePercentToken(token) {
        const trimmed = String(token || "").trim();
        if (!trimmed) {
            throw new Error("Percentage filter cannot be empty");
        }
        const normalized = trimmed.replace(/%/g, "").trim();
        if (!normalized || !/^-?\d+(?:\.\d+)?$/.test(normalized)) {
            throw new Error(`Invalid percentage token: ${token}`);
        }
        return Number.parseFloat(normalized);
    }

    function parsePercentExpr(expr) {
        return parseNumericExpr(transformNumericExpr(expr, normalizePercentToken));
    }

    function tokenizeQuery(text) {
        const tokens = [];
        let current = "";
        let quote = null;

        for (const character of String(text || "")) {
            if (quote) {
                if (character === quote) {
                    quote = null;
                } else {
                    current += character;
                }
                continue;
            }

            if (character === '"' || character === "'") {
                quote = character;
                continue;
            }

            if (/\s/.test(character)) {
                if (current) {
                    tokens.push(current);
                    current = "";
                }
                continue;
            }

            current += character;
        }

        if (current) {
            tokens.push(current);
        }

        return tokens;
    }

    function globToRegExp(pattern) {
        const source = String(pattern || "")
            .replace(/[|\\{}()[\]^$+?.]/g, "\\$&")
            .replace(/\*/g, ".*")
            .replace(/\?/g, ".");
        return new RegExp(`^${source}$`, "i");
    }

    function normalizeStringFilterValue(value) {
        return String(value || "")
            .trim()
            .replace(/\s+/g, " ")
            .toLowerCase();
    }

    function createStringMatcher(rawValue) {
        const value = String(rawValue || "").trim();
        if (!value) {
            throw new Error("Filter value cannot be empty");
        }
        if (value.startsWith("=")) {
            const expected = normalizeStringFilterValue(value.slice(1));
            if (!expected) {
                throw new Error("Exact filter value cannot be empty");
            }
            return (candidate) => normalizeStringFilterValue(candidate) === expected;
        }
        if (value.includes("*") || value.includes("?")) {
            const regex = globToRegExp(value);
            return (candidate) => regex.test(String(candidate || ""));
        }
        const lowered = normalizeStringFilterValue(value);
        return (candidate) => normalizeStringFilterValue(candidate).includes(lowered);
    }

    function highlightSuggestionText(value, inputValue) {
        const text = String(value || "");
        const query = String(inputValue || "").trim().toLowerCase();
        if (!query) {
            return escapeHtml(text);
        }
        const matchIndex = text.toLowerCase().indexOf(query);
        if (matchIndex < 0) {
            return escapeHtml(text);
        }
        const before = escapeHtml(text.slice(0, matchIndex));
        const match = escapeHtml(text.slice(matchIndex, matchIndex + query.length));
        const after = escapeHtml(text.slice(matchIndex + query.length));
        return `${before}<span class="smart-filter-suggestion-highlight">${match}</span>${after}`;
    }

    function currentSmartFilterToken(rawValue, caretIndex) {
        const value = String(rawValue || "");
        const caret = Number.isInteger(caretIndex) ? caretIndex : value.length;
        let start = caret;
        let end = caret;

        while (start > 0 && !/\s/.test(value[start - 1])) {
            start -= 1;
        }
        while (end < value.length && !/\s/.test(value[end])) {
            end += 1;
        }

        const token = value.slice(start, end);
        const separatorIndex = token.indexOf(":");
        return {
            start,
            end,
            token,
            separatorIndex,
            fieldName: separatorIndex > 0 ? token.slice(0, separatorIndex) : "",
            rawValue: separatorIndex > 0 ? token.slice(separatorIndex + 1) : "",
        };
    }

    function smartFilterFieldSuggestions(query) {
        const lowered = String(query || "").trim().toLowerCase();
        return SMART_FILTER_FIELD_INFO
            .filter((entry) => !lowered || entry.name.includes(lowered))
            .map((entry) => ({
                key: `field:${entry.name}`,
                insertText: `${entry.name}:`,
                displayText: `${entry.name}:`,
                matchText: entry.name,
                hint: entry.hint,
            }));
    }

    function smartFilterValueSuggestions(field, query) {
        const lowered = String(query || "").trim().toLowerCase();
        const fieldName = smartFilterFieldNameByCanonical.get(field) || field;
        const staticSuggestions = (smartFilterStaticValueTemplates[field] || []).map((entry) => ({
            key: `value:${field}:${entry.value || "(empty)"}`,
            value: entry.value,
            hint: entry.hint,
        }));
        const dynamicSuggestions = (field === "genres" ? (smartFilterDataValues.genres || []) : []).map((value) => ({
            key: `value:${field}:${value}`,
            value,
            hint: "Value",
        }));

        const combined = [...staticSuggestions, ...dynamicSuggestions];
        const deduped = [...new Map(combined.map((entry) => [String(entry.value), entry])).values()];
        const matching = deduped.filter((entry) => !lowered || String(entry.value || "").toLowerCase().includes(lowered));
        const ranked = matching.sort((left, right) => {
            const leftValue = String(left.value || "").toLowerCase();
            const rightValue = String(right.value || "").toLowerCase();
            const leftStarts = lowered && leftValue.startsWith(lowered) ? 0 : 1;
            const rightStarts = lowered && rightValue.startsWith(lowered) ? 0 : 1;
            if (leftStarts !== rightStarts) {
                return leftStarts - rightStarts;
            }
            return leftValue.localeCompare(rightValue, undefined, { sensitivity: "base", numeric: true });
        });

        return ranked.slice(0, 10).map((entry) => ({
            key: entry.key,
            insertText: `${fieldName}:${entry.value}`,
            displayText: `${fieldName}:${entry.value}`,
            matchText: String(entry.value || ""),
            hint: entry.hint,
        }));
    }

    function renderSmartFilterSuggestions(rawValue) {
        if (!elements.smartFilterSuggestions || !elements.searchInput) {
            return;
        }

        const caretIndex = elements.searchInput.selectionStart ?? String(rawValue || "").length;
        const tokenInfo = currentSmartFilterToken(rawValue, caretIndex);
        const suggestionKey = `${rawValue}|${caretIndex}`;
        if (state.lastSmartFilterSuggestionKey === suggestionKey) {
            return;
        }

        let suggestions = [];
        if (tokenInfo.separatorIndex > 0) {
            const canonicalField = canonicalSmartFilterField(tokenInfo.fieldName);
            if (canonicalField) {
                suggestions = smartFilterValueSuggestions(canonicalField, tokenInfo.rawValue);
            } else {
                suggestions = smartFilterFieldSuggestions(tokenInfo.fieldName);
            }
        } else {
            suggestions = smartFilterFieldSuggestions(tokenInfo.token);
            if (!String(tokenInfo.token || "").trim()) {
                suggestions = suggestions.slice(0, 12);
            }
        }

        state.smartFilterSuggestions = suggestions;
        state.activeSmartFilterSuggestionIndex = suggestions.length ? 0 : -1;
        state.lastSmartFilterSuggestionKey = suggestionKey;

        if (!suggestions.length) {
            hideSmartFilterSuggestions();
            return;
        }

        elements.smartFilterSuggestions.innerHTML = suggestions.map((suggestion, index) => `
            <div id="smartFilterSuggestion-${index}" class="smart-filter-suggestion-item ${index === state.activeSmartFilterSuggestionIndex ? "active" : ""}" role="option" aria-selected="${index === state.activeSmartFilterSuggestionIndex ? "true" : "false"}" data-index="${index}">
                <span class="smart-filter-suggestion-main">${highlightSuggestionText(suggestion.displayText, tokenInfo.separatorIndex > 0 ? tokenInfo.rawValue : tokenInfo.token)}</span>
                <span class="smart-filter-suggestion-hint">${escapeHtml(suggestion.hint || "Field")}</span>
            </div>
        `).join("");
        elements.smartFilterSuggestions.hidden = false;
        elements.searchInput.setAttribute("aria-expanded", "true");
        elements.searchInput.setAttribute("aria-activedescendant", `smartFilterSuggestion-${state.activeSmartFilterSuggestionIndex}`);
    }

    function refreshSmartFilterSuggestionsFromInput() {
        if (!elements.searchInput) {
            return;
        }
        renderSmartFilterSuggestions(String(elements.searchInput.value || ""));
    }

    function scheduleSmartFilterSuggestionRefresh() {
        if (state.smartFilterSuggestionRefreshScheduled) {
            return;
        }
        state.smartFilterSuggestionRefreshScheduled = true;
        requestAnimationFrame(() => {
            state.smartFilterSuggestionRefreshScheduled = false;
            refreshSmartFilterSuggestionsFromInput();
        });
    }

    function hideSmartFilterSuggestions() {
        if (!elements.smartFilterSuggestions || !elements.searchInput) {
            return;
        }
        elements.smartFilterSuggestions.hidden = true;
        elements.smartFilterSuggestions.innerHTML = "";
        elements.searchInput.setAttribute("aria-expanded", "false");
        elements.searchInput.removeAttribute("aria-activedescendant");
        state.smartFilterSuggestions = [];
        state.activeSmartFilterSuggestionIndex = -1;
        state.lastSmartFilterSuggestionKey = "";
    }

    function areSmartFilterSuggestionsVisible() {
        return Boolean(elements.smartFilterSuggestions && !elements.smartFilterSuggestions.hidden && state.smartFilterSuggestions.length);
    }

    function updateActiveSmartFilterSuggestion() {
        if (!elements.smartFilterSuggestions || !elements.searchInput) {
            return;
        }
        const items = Array.from(elements.smartFilterSuggestions.querySelectorAll(".smart-filter-suggestion-item"));
        items.forEach((item, index) => {
            const active = index === state.activeSmartFilterSuggestionIndex;
            item.classList.toggle("active", active);
            item.setAttribute("aria-selected", active ? "true" : "false");
            if (active) {
                elements.searchInput.setAttribute("aria-activedescendant", item.id);
                item.scrollIntoView({ block: "nearest" });
            }
        });
    }

    function moveSmartFilterSuggestionSelection(delta) {
        if (!areSmartFilterSuggestionsVisible()) {
            return;
        }
        const nextIndex = Math.max(0, Math.min(state.smartFilterSuggestions.length - 1, state.activeSmartFilterSuggestionIndex + delta));
        state.activeSmartFilterSuggestionIndex = nextIndex;
        updateActiveSmartFilterSuggestion();
    }

    function applySmartFilterSuggestion(index) {
        if (!elements.searchInput || index < 0 || index >= state.smartFilterSuggestions.length) {
            return false;
        }
        const suggestion = state.smartFilterSuggestions[index];
        const inputValue = String(elements.searchInput.value || "");
        const caretIndex = elements.searchInput.selectionStart ?? inputValue.length;
        const tokenInfo = currentSmartFilterToken(inputValue, caretIndex);
        const before = inputValue.slice(0, tokenInfo.start);
        const after = inputValue.slice(tokenInfo.end);
        const needsTrailingSpace = after && !/^\s/.test(after);
        const nextValue = `${before}${suggestion.insertText}${needsTrailingSpace ? " " : ""}${after}`;
        const nextCaret = before.length + suggestion.insertText.length + (needsTrailingSpace ? 1 : 0);

        elements.searchInput.value = nextValue;
        elements.searchInput.focus({ preventScroll: true });
        elements.searchInput.setSelectionRange(nextCaret, nextCaret);
        hideSmartFilterSuggestions();
        elements.searchInput.dispatchEvent(new Event("input", { bubbles: true }));
        return true;
    }

    function canonicalSmartFilterField(fieldName) {
        return ({
            title: "title",
            releasedate: "release_date",
            source: "source_id",
            type: "item_type",
            state: "watch_state",
            season: "season",
            season_title: "season_title",
            episode: "episode",
            episode_title: "episode_title",
            views: "views",
            watchdate: "watch_date",
            runtime: "runtime_minutes",
            rating: "average_rating",
            votes: "num_votes",
            genres: "genres",
            title_type: "title_type",
            id: "id",
            parent: "parent_id",
            level: "level",
            children: "has_children",
            progress: "progress",
            total: "progress_total",
        })[String(fieldName || "").trim().toLowerCase()] || "";
    }

    function normalizeItemTypeToken(value) {
        const lowered = String(value || "").trim().toLowerCase();
        const normalized = ({
            movie: "movie",
            movies: "movie",
            film: "movie",
            series: "series",
            show: "series",
            shows: "series",
            season: "season",
            seasons: "season",
            episode: "episode",
            episodes: "episode",
        })[lowered];
        if (!normalized) {
            throw new Error(`Unknown type filter: ${value}`);
        }
        return normalized;
    }

    function normalizeWatchStateToken(value) {
        const lowered = String(value || "").trim().toLowerCase();
        const normalized = ({
            watched: "watched",
            complete: "watched",
            completed: "watched",
            partial: "partial",
            partially: "partial",
            unwatched: "unwatched",
            unseen: "unwatched",
            aggregate: "aggregate",
            grouped: "aggregate",
        })[lowered];
        if (!normalized) {
            throw new Error(`Unknown watch state filter: ${value}`);
        }
        return normalized;
    }

    function parseBooleanToken(value) {
        const lowered = String(value || "").trim().toLowerCase();
        if (["1", "true", "yes", "y", "on"].includes(lowered)) {
            return true;
        }
        if (["0", "false", "no", "n", "off"].includes(lowered)) {
            return false;
        }
        throw new Error(`Invalid boolean value: ${value}`);
    }

    function parseYearValues(value) {
        return [...new Set((String(value || "").match(/\d{4}/g) || []).map((token) => Number.parseInt(token, 10)))];
    }

    function parseLeadingInteger(value) {
        const match = String(value || "").match(/-?\d+/);
        return match ? Number.parseInt(match[0], 10) : null;
    }

    function parseVotesValue(value) {
        const normalized = String(value || "").replace(/,/g, "").trim();
        if (!normalized || !/^-?\d+$/.test(normalized)) {
            return null;
        }
        return Number.parseInt(normalized, 10);
    }

    function parseRatingValue(value) {
        const normalized = String(value || "").trim();
        if (!normalized || !/^-?\d+(?:\.\d+)?$/.test(normalized)) {
            return null;
        }
        return Number.parseFloat(normalized);
    }

    function parseRuntimeMinutesValue(value) {
        const normalized = String(value || "").trim();
        if (!normalized) {
            return null;
        }
        try {
            return normalizeRuntimeToken(normalized);
        } catch {
            return null;
        }
    }

    function parseProgressCounts(value) {
        const match = String(value || "").match(/^\s*(\d+)\s*\/\s*(\d+)\s*$/);
        if (!match) {
            return { watched: null, total: null };
        }
        return {
            watched: Number.parseInt(match[1], 10),
            total: Number.parseInt(match[2], 10),
        };
    }

    function parseProgressPercentValue(value) {
        const counts = parseProgressCounts(value);
        if (!Number.isFinite(counts.watched) || !Number.isFinite(counts.total) || counts.total <= 0) {
            return null;
        }
        return (counts.watched / counts.total) * 100;
    }

    function numericValuesFrom(row, value) {
        if (Array.isArray(value)) {
            return value.filter((item) => Number.isFinite(item));
        }
        if (value === null || value === undefined || value === "") {
            return [];
        }
        return Number.isFinite(value) ? [value] : [];
    }

    function buildNumericFieldPredicate(rawValue, valuesForRow, parser = parseNumericExpr) {
        const matcher = parser(rawValue);
        return (row) => numericValuesFrom(row, valuesForRow(row)).some((value) => matcher(value));
    }

    function buildStringFieldPredicate(rawValue, valueForRow) {
        const matcher = createStringMatcher(rawValue);
        return (row) => matcher(valueForRow(row));
    }

    function buildFreeTextPredicate(token) {
        const lowered = String(token || "").trim().toLowerCase();
        if (!lowered) {
            return () => true;
        }
        return (row) => String(row.search_text || "").toLowerCase().includes(lowered);
    }

    function buildFieldPredicate(fieldName, rawValue, options = {}) {
        const field = canonicalSmartFilterField(fieldName);
        if (!field) {
            throw new Error(`Unknown filter field: ${fieldName}`);
        }

        const titleScope = options.titleScope || "top-level";

        const usesPercentExpr = String(rawValue || "").includes("%");

        if (field === "title") {
            if (titleScope === "local") {
                return buildStringFieldPredicate(rawValue, (row) => (row.level === 0 ? combinedDisplayText(displayTitle(row), row.title) : ""));
            }
            return buildStringFieldPredicate(rawValue, (row) => topLevelTitleText(row));
        }
        if (field === "source_id") {
            return buildStringFieldPredicate(rawValue, (row) => row.source_id || "");
        }
        if (field === "item_type") {
            const expected = normalizeItemTypeToken(rawValue);
            return (row) => row.item_type === expected;
        }
        if (field === "watch_state") {
            const expected = normalizeWatchStateToken(rawValue);
            return (row) => row.watch_state === expected;
        }
        if (field === "release_date") {
            return buildNumericFieldPredicate(rawValue, (row) => parseReleaseDateValues(row.year), parseDateExpr);
        }
        if (field === "release_year") {
            return buildNumericFieldPredicate(rawValue, (row) => parseYearValues(row.year));
        }
        if (field === "season") {
            return buildNumericFieldPredicate(rawValue, (row) => parseLeadingInteger(row.season));
        }
        if (field === "season_title") {
            return buildStringFieldPredicate(rawValue, (row) => row.season_title || "");
        }
        if (field === "episode") {
            return buildNumericFieldPredicate(rawValue, (row) => parseLeadingInteger(row.episode));
        }
        if (field === "episode_title") {
            return buildStringFieldPredicate(rawValue, (row) => combinedDisplayText(displayEpisodeTitle(row), row.episode_title));
        }
        if (field === "views") {
            return buildNumericFieldPredicate(rawValue, (row) => parseLeadingInteger(row.views));
        }
        if (field === "watch_date") {
            return buildNumericFieldPredicate(rawValue, (row) => parseDateValues(row.watch_dates), parseDateExpr);
        }
        if (field === "watch_year") {
            return buildNumericFieldPredicate(rawValue, (row) => parseYearValues(row.watch_dates));
        }
        if (field === "runtime_minutes") {
            return buildNumericFieldPredicate(rawValue, (row) => parseRuntimeMinutesValue(row.runtime_minutes), parseRuntimeExpr);
        }
        if (field === "average_rating") {
            return buildNumericFieldPredicate(rawValue, (row) => parseRatingValue(row.average_rating));
        }
        if (field === "num_votes") {
            return buildNumericFieldPredicate(rawValue, (row) => parseVotesValue(row.num_votes));
        }
        if (field === "genres") {
            return buildStringFieldPredicate(rawValue, (row) => row.genres || "");
        }
        if (field === "title_type") {
            return buildStringFieldPredicate(rawValue, (row) => row.title_type || "");
        }
        if (field === "id") {
            return buildStringFieldPredicate(rawValue, (row) => row.id || "");
        }
        if (field === "parent_id") {
            return buildStringFieldPredicate(rawValue, (row) => row.parent_id || "");
        }
        if (field === "level") {
            return buildNumericFieldPredicate(rawValue, (row) => Number(row.level ?? 0));
        }
        if (field === "has_children") {
            if (!String(rawValue || "").trim()) {
                return (row) => Boolean(row.has_children);
            }
            const expected = parseBooleanToken(rawValue);
            return (row) => Boolean(row.has_children) === expected;
        }
        if (field === "progress") {
            if (usesPercentExpr) {
                return buildNumericFieldPredicate(rawValue, (row) => parseProgressPercentValue(row.episode), parsePercentExpr);
            }
            return buildNumericFieldPredicate(rawValue, (row) => parseProgressCounts(row.episode).watched);
        }
        if (field === "progress_total") {
            return buildNumericFieldPredicate(rawValue, (row) => parseProgressCounts(row.episode).total);
        }

        throw new Error(`Unsupported filter field: ${fieldName}`);
    }

    function compileSmartFilter(text, options = {}) {
        const trimmed = String(text || "").trim();
        if (!trimmed) {
            return {
                predicate: () => true,
                hasEpisodeTitleClause: false,
                summary: "Smart filter ready",
            };
        }

        const tokens = tokenizeQuery(trimmed);
        const groups = [[]];
        let negateNext = false;
        let clauseCount = 0;
        let revealMatchingDescendants = false;
        let revealTopLevelTitleDescendants = false;
        let hasEpisodeTitleClause = false;

        for (let index = 0; index < tokens.length; index += 1) {
            const token = tokens[index];
            const lowered = token.toLowerCase();

            if (["or", "||", "--or"].includes(lowered)) {
                if (!groups[groups.length - 1].length) {
                    throw new Error("or must follow a filter term");
                }
                groups.push([]);
                continue;
            }

            if (["not", "!", "--not"].includes(lowered)) {
                negateNext = !negateNext;
                continue;
            }

            let predicate;
            if (token.startsWith("--")) {
                const fieldName = token.slice(2);
                const canonicalField = canonicalSmartFilterField(fieldName);
                const isValueLess = canonicalField === "has_children";
                let rawValue = "";
                if (!canonicalField) {
                    throw new Error(`Unknown filter field: ${fieldName}`);
                }
                if (!isValueLess) {
                    if (index + 1 >= tokens.length) {
                        throw new Error(`Missing value for --${fieldName}`);
                    }
                    rawValue = tokens[index + 1];
                    index += 1;
                }
                if (["progress", "progress_total"].includes(canonicalField)) {
                    revealMatchingDescendants = true;
                }
                if (!negateNext && canonicalField === "title") {
                    revealTopLevelTitleDescendants = true;
                }
                if (!negateNext && canonicalField === "episode_title") {
                    hasEpisodeTitleClause = true;
                }
                predicate = buildFieldPredicate(fieldName, rawValue, options);
            } else {
                const separatorIndex = token.indexOf(":");
                if (separatorIndex > 0) {
                    const fieldName = token.slice(0, separatorIndex);
                    const rawValue = token.slice(separatorIndex + 1);
                    const canonicalField = canonicalSmartFilterField(fieldName);
                    const knownField = Boolean(canonicalField);
                    if (knownField) {
                        if (["progress", "progress_total"].includes(canonicalField)) {
                            revealMatchingDescendants = true;
                        }
                        if (!negateNext && canonicalField === "title") {
                            revealTopLevelTitleDescendants = true;
                        }
                        if (!negateNext && canonicalField === "episode_title") {
                            hasEpisodeTitleClause = true;
                        }
                        predicate = buildFieldPredicate(fieldName, rawValue, options);
                    } else {
                        predicate = buildFreeTextPredicate(token);
                    }
                } else {
                    predicate = buildFreeTextPredicate(token);
                }
            }

            if (negateNext) {
                const inner = predicate;
                predicate = (row) => !inner(row);
                negateNext = false;
            }

            groups[groups.length - 1].push(predicate);
            clauseCount += 1;
        }

        if (negateNext) {
            throw new Error("not must be followed by a filter");
        }

        if (!groups.every((group) => group.length)) {
            throw new Error("or cannot terminate a filter");
        }

        return {
            predicate: (row) => groups.some((group) => group.every((filter) => filter(row))),
            revealMatchingDescendants,
            revealTopLevelTitleDescendants,
            hasEpisodeTitleClause,
            summary: `${clauseCount} clause${clauseCount === 1 ? "" : "s"} active${groups.length > 1 ? ` across ${groups.length} groups` : ""}`,
        };
    }

    function syncFilterInputState() {
        if (!elements.searchInput) {
            return;
        }
        elements.searchInput.classList.toggle("is-invalid", Boolean(state.filterError));
        elements.searchInput.setAttribute("aria-invalid", state.filterError ? "true" : "false");
        elements.searchInput.title = state.filterError || "";
    }

    function applySmartFilterQuery(nextQuery) {
        const nextNormalizedQuery = String(nextQuery || "").trim();
        if (state.query !== nextNormalizedQuery) {
            state.filterCollapsed.clear();
        }
        closeRowContextMenu();
        state.query = nextNormalizedQuery;
        try {
            const compiled = compileSmartFilter(state.query);
            const compiledHighlight = compileSmartFilter(state.query, { titleScope: "local" });
            state.compiledFilter = compiled.predicate;
            state.compiledHighlightFilter = compiledHighlight.predicate;
            state.revealMatchingDescendants = compiled.revealMatchingDescendants;
            state.revealTopLevelTitleDescendants = compiled.revealTopLevelTitleDescendants;
            state.hasEpisodeTitleClause = compiled.hasEpisodeTitleClause;
            state.filterError = "";
        } catch (error) {
            state.filterError = error instanceof Error ? error.message : String(error);
            state.compiledHighlightFilter = () => false;
            state.revealMatchingDescendants = false;
            state.revealTopLevelTitleDescendants = false;
            state.hasEpisodeTitleClause = false;
        }
        syncFilterInputState();
        render();
    }

    function closeSmartFilterHelp() {
        if (!elements.smartFilterHelp || !elements.smartFilterHelpButton) {
            return;
        }
        elements.smartFilterHelp.hidden = true;
        elements.smartFilterHelpButton.setAttribute("aria-expanded", "false");
    }

    function toggleSmartFilterHelp() {
        if (!elements.smartFilterHelp || !elements.smartFilterHelpButton) {
            return;
        }
        const nextHidden = !elements.smartFilterHelp.hidden;
        elements.smartFilterHelp.hidden = nextHidden;
        elements.smartFilterHelpButton.setAttribute("aria-expanded", nextHidden ? "false" : "true");
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
        return state.compiledFilter(row);
    }

    function rowHasDirectFilterMatch(row) {
        if (!state.query) {
            return false;
        }
        if (state.compiledHighlightFilter(row)) {
            return true;
        }
        return Boolean(
            state.hasEpisodeTitleClause
            && row.item_type === "episode"
            && rowMatches(row, state.query)
        );
    }

    function isRowExpanded(row) {
        if (!row || !row.has_children) {
            return false;
        }

        if (state.filterCollapsed.has(row.id)) {
            return false;
        }

        if (state.query) {
            return true;
        }

        return state.expanded.has(row.id);
    }

    function branchMatches(row, query) {
        if (rowMatches(row, query)) {
            return true;
        }
        const children = childrenMap.get(row.id) || [];
        return children.some((child) => branchMatches(child, query));
    }

    function flattenVisibleRows(parentId, includeAllDescendants) {
        const visible = [];
        const siblings = childrenMap.get(parentId || "ROOT") || [];
        const queryActive = Boolean(state.query);

        siblings.forEach((row) => {
            const directMatch = rowMatches(row, state.query);
            const visibleByQuery = includeAllDescendants || branchMatches(row, state.query);

            if (!visibleByQuery) {
                return;
            }

            visible.push(row);

            const revealDescendants = includeAllDescendants || (
                queryActive
                && directMatch
                && row.has_children
                && (
                    state.revealMatchingDescendants
                    || (state.revealTopLevelTitleDescendants && row.level === 0)
                )
            );

            if (row.has_children && isRowExpanded(row)) {
                visible.push(...flattenVisibleRows(row.id, revealDescendants));
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

    function collectBranchRows(row) {
        if (!row) {
            return [];
        }

        const branchRows = [];
        const stack = [row];
        while (stack.length) {
            const current = stack.pop();
            branchRows.push(current);
            const children = childrenMap.get(current.id) || [];
            for (let index = children.length - 1; index >= 0; index -= 1) {
                stack.push(children[index]);
            }
        }
        return branchRows;
    }

    function collectExpandableBranchRows(row) {
        return collectBranchRows(row).filter((branchRow) => branchRow.has_children);
    }

    function branchCanExpand(row) {
        return collectExpandableBranchRows(row).some((branchRow) => !isRowExpanded(branchRow));
    }

    function branchCanCollapse(row) {
        return collectExpandableBranchRows(row).some((branchRow) => isRowExpanded(branchRow));
    }

    function expandablesInView() {
        return rows.filter((row) => row.has_children && (!state.query || branchMatches(row, state.query)));
    }

    function canExpandAllInView() {
        return expandablesInView().some((row) => !isRowExpanded(row));
    }

    function canCollapseAllInView() {
        return expandablesInView().some((row) => isRowExpanded(row));
    }

    function setBranchExpanded(row, expanded) {
        collectExpandableBranchRows(row).forEach((branchRow) => {
            if (expanded) {
                state.expanded.add(branchRow.id);
                if (state.query) {
                    state.filterCollapsed.delete(branchRow.id);
                }
                return;
            }
            state.expanded.delete(branchRow.id);
            if (state.query) {
                state.filterCollapsed.add(branchRow.id);
            }
        });
        renderRows();
    }

    function setAllExpandedInView(expanded) {
        expandablesInView().forEach((row) => {
            if (expanded) {
                state.expanded.add(row.id);
                if (state.query) {
                    state.filterCollapsed.delete(row.id);
                }
                return;
            }
            state.expanded.delete(row.id);
            if (state.query) {
                state.filterCollapsed.add(row.id);
            }
        });
        renderRows();
    }

    function formatRowSummary(row) {
        const title = displayTitle(row) || formatItemType(row.item_type);
        const details = [];
        const subtitle = rowSubtitle(row);
        if (subtitle) {
            details.push(subtitle);
        }
        if (!subtitle && row.year) {
            details.push(`Released ${row.year}`);
        }
        const episodeTitle = displayEpisodeTitle(row);
        if (episodeTitle && episodeTitle !== title) {
            details.push(`Episode title ${episodeTitle}`);
        }
        details.push(watchStateLabel(row.watch_state));
        if (row.runtime_minutes) {
            details.push(`Runtime ${row.runtime_minutes}`);
        }
        if (row.average_rating) {
            details.push(`Rating ${row.average_rating}`);
        }
        if (row.views) {
            details.push(`Views ${row.views}`);
        }
        if (row.genres) {
            details.push(`Genres ${row.genres}`);
        }
        if (row.source_id) {
            details.push(`Source ${row.source_id}`);
        }
        return details.length ? `${formatItemType(row.item_type)}: ${title} - ${details.join(" | ")}` : `${formatItemType(row.item_type)}: ${title}`;
    }

    function formatRowSummaryWithChildren(row) {
        const branchRows = collectBranchRows(row);
        const lines = [formatRowSummary(row)];
        if (branchRows.length <= 1) {
            return lines.join("\n");
        }

        branchRows.slice(1).forEach((branchRow) => {
            const depth = Math.max(1, Number(branchRow.level || 0) - Number(row.level || 0));
            lines.push(`${"  ".repeat(depth)}- ${formatRowSummary(branchRow)}`);
        });
        return lines.join("\n");
    }

    function legacyCopyText(text) {
        const textarea = document.createElement("textarea");
        textarea.value = text;
        textarea.setAttribute("readonly", "true");
        textarea.style.position = "fixed";
        textarea.style.top = "0";
        textarea.style.left = "0";
        textarea.style.opacity = "0";
        document.body.appendChild(textarea);
        textarea.focus({ preventScroll: true });
        textarea.select();
        textarea.setSelectionRange(0, textarea.value.length);
        let succeeded = false;
        try {
            succeeded = Boolean(document.execCommand("copy"));
        } catch {
            succeeded = false;
        }
        document.body.removeChild(textarea);
        return succeeded;
    }

    async function copyTextToClipboard(text) {
        if (navigator.clipboard && window.isSecureContext) {
            try {
                await navigator.clipboard.writeText(text);
                return true;
            } catch {
                return legacyCopyText(text);
            }
        }
        return legacyCopyText(text);
    }

    function rowContextMenuItems(row) {
        const items = [];
        const canExpandBranch = branchCanExpand(row);
        const canCollapseBranch = branchCanCollapse(row);
        const canExpandAll = canExpandAllInView();
        const canCollapseAll = canCollapseAllInView();

        if (row.has_children) {
            items.push({
                action: "toggle-row",
                label: isRowExpanded(row) ? "Collapse" : "Expand",
                enabled: isRowExpanded(row) ? canCollapseBranch : canExpandBranch,
            });
        }

        items.push(
            {
                action: "expand-all",
                label: "Expand all",
                enabled: canExpandAll,
            },
            {
                action: "collapse-all",
                label: "Collapse all",
                enabled: canCollapseAll,
            },
            { separator: true },
            {
                action: "copy-summary",
                label: "Copy row summary",
                enabled: true,
            }
        );

        if (row.has_children) {
            items.push({
                action: "copy-summary-children",
                label: "Copy node summary",
                enabled: true,
            });
        }

        return items;
    }

    function renderRowContextMenu(row) {
        const items = rowContextMenuItems(row);
        elements.rowContextMenu.innerHTML = items.map((item) => {
            if (item.separator) {
                return '<div class="row-context-menu-separator" role="separator"></div>';
            }
            return `
                <button class="row-context-menu-item" type="button" role="menuitem" data-action="${escapeHtml(item.action)}" ${item.enabled ? "" : "disabled"}>
                    <span class="row-context-menu-label">${escapeHtml(item.label)}</span>
                </button>
            `;
        }).join("");
    }

    function openRowContextMenu(clientX, clientY, rowId) {
        const row = rowMap.get(rowId);
        if (!row || !elements.rowContextMenu) {
            closeRowContextMenu();
            return;
        }

        closeColumnMenu();
        state.contextMenuRowId = rowId;
        renderRowContextMenu(row);
        elements.rowContextMenu.hidden = false;
        positionFloatingMenu(elements.rowContextMenu, clientX, clientY);
    }

    async function handleRowContextMenuAction(action, rowId) {
        const row = rowMap.get(rowId);
        if (!row) {
            closeRowContextMenu();
            return;
        }

        if (action === "toggle-row") {
            closeRowContextMenu();
            setBranchExpanded(row, !isRowExpanded(row));
            return;
        }
        if (action === "expand-all") {
            closeRowContextMenu();
            setAllExpandedInView(true);
            return;
        }
        if (action === "collapse-all") {
            closeRowContextMenu();
            setAllExpandedInView(false);
            return;
        }
        if (action === "copy-summary") {
            await copyTextToClipboard(formatRowSummary(row));
            closeRowContextMenu();
            return;
        }
        if (action === "copy-summary-children") {
            await copyTextToClipboard(formatRowSummaryWithChildren(row));
            closeRowContextMenu();
        }
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

    function watchStateLabel(watchState) {
        if (watchState === "partial") {
            return "Partially watched";
        }
        if (watchState === "watched") {
            return "Watched";
        }
        if (watchState === "unwatched") {
            return "Unwatched";
        }
        return "Aggregate";
    }

    function watchStateRowClass(watchState) {
        if (watchState === "watched") {
            return "is-watched";
        }
        if (watchState === "partial") {
            return "is-partial";
        }
        if (watchState === "unwatched") {
            return "is-unwatched";
        }
        return "";
    }

    function formatCount(value) {
        return Number(value || 0).toLocaleString();
    }

    function formatDurationMinutes(totalMinutes) {
        const minutes = Math.max(0, Number(totalMinutes) || 0);
        if (minutes < 60) {
            return `${minutes}m`;
        }

        const hours = Math.floor(minutes / 60);
        const remainingMinutes = minutes % 60;
        if (hours < 24) {
            return remainingMinutes ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
        }

        const days = Math.floor(hours / 24);
        const remainingHours = hours % 24;
        if (!remainingHours && !remainingMinutes) {
            return `${days}d`;
        }
        if (!remainingMinutes) {
            return `${days}d ${remainingHours}h`;
        }
        return `${days}d ${remainingHours}h ${remainingMinutes}m`;
    }

    function isRuntimeLeafRow(row) {
        return !row.has_children && (row.item_type === "movie" || row.item_type === "episode");
    }

    function collectStatusMetrics() {
        const metrics = {
            totalItems: rows.length,
            filteredItems: 0,
            shownRows: state.virtualRows.length,
            watchedItems: 0,
            partialItems: 0,
            unwatchedItems: 0,
            watchedRuntimeMinutes: 0,
            totalRuntimeMinutes: 0,
        };

        rows.forEach((row) => {
            if (!rowMatches(row, state.query)) {
                return;
            }

            metrics.filteredItems += 1;
            if (row.watch_state === "watched") {
                metrics.watchedItems += 1;
            } else if (row.watch_state === "partial") {
                metrics.partialItems += 1;
            } else if (row.watch_state === "unwatched") {
                metrics.unwatchedItems += 1;
            }

            if (!isRuntimeLeafRow(row)) {
                return;
            }

            const runtimeMinutes = parseRuntimeMinutesValue(row.runtime_minutes);
            if (!Number.isFinite(runtimeMinutes) || runtimeMinutes <= 0) {
                return;
            }

            metrics.totalRuntimeMinutes += runtimeMinutes;
            if (row.watch_state === "watched") {
                metrics.watchedRuntimeMinutes += runtimeMinutes;
            }
        });

        return metrics;
    }

    function renderStatusBar() {
        const metrics = collectStatusMetrics();
        elements.statusTotalItems.textContent = formatCount(metrics.totalItems);
        elements.statusFilteredItems.textContent = formatCount(metrics.filteredItems);
        elements.statusShownRows.textContent = formatCount(metrics.shownRows);
        elements.statusWatchedItems.textContent = formatCount(metrics.watchedItems);
        elements.statusPartialItems.textContent = formatCount(metrics.partialItems);
        elements.statusUnwatchedItems.textContent = formatCount(metrics.unwatchedItems);
        elements.statusWatchtime.textContent = `${formatDurationMinutes(metrics.watchedRuntimeMinutes)} / ${formatDurationMinutes(metrics.totalRuntimeMinutes)}`;
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
            { label: "Watch Status", value: watchStateLabel(row.watch_state) },
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
        const row = rowMap.get(rowId);
        if (!row || !row.has_children) {
            return;
        }

        const expanded = isRowExpanded(row);
        if (state.query) {
            if (expanded) {
                state.filterCollapsed.add(rowId);
                state.expanded.delete(rowId);
            } else {
                state.filterCollapsed.delete(rowId);
                state.expanded.add(rowId);
            }
        } else if (expanded) {
            state.expanded.delete(rowId);
        } else {
            state.expanded.add(rowId);
        }
        renderRows();
    }

    function rowSubtitle(row) {
        if (row.item_type === "series") {
            if (row.year && row.episode) {
                return `${row.year} - Progress ${row.episode}`;
            }
            if (row.episode) {
                return `Progress ${row.episode}`;
            }
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
        const expanded = isRowExpanded(row);
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
        const watchStateClass = watchStateRowClass(row.watch_state);
        return `
            <div class="treecell ${watchStateClass}" style="--level:${row.level}">
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
        const statusLabel = row.watch_state === "aggregate" ? "" : watchStateLabel(row.watch_state);
        const statusClass = row.watch_state === "watched"
            ? "is-watched"
            : row.watch_state === "partial"
                ? "is-partial"
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
        const watchStateClass = watchStateRowClass(row.watch_state);
        const directFilterMatchClass = rowHasDirectFilterMatch(row) ? "is-filter-match" : "";
        return `
            <tr class="tree-row ${row.id === state.selectedId ? "is-selected" : ""} ${watchStateClass} ${directFilterMatchClass}" data-row-id="${escapeHtml(row.id)}" data-virtual-index="${index}">
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
        syncFilterInputState();
        renderHeader();
        renderRows();
        renderStatusBar();
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

    elements.resultBody.addEventListener("contextmenu", (event) => {
        const rowElement = event.target.closest("tr.tree-row[data-row-id]");
        if (!rowElement) {
            return;
        }

        const rowId = rowElement.getAttribute("data-row-id");
        if (!rowId) {
            return;
        }

        event.preventDefault();
        if (state.selectedId !== rowId) {
            state.selectedId = rowId;
            renderRows();
            renderSelection();
        }
        openRowContextMenu(event.clientX, event.clientY, rowId);
    });

    elements.searchInput.addEventListener("input", (event) => {
        const nextQuery = String(event.target.value || "");
        if (state.filterInputTimer) {
            clearTimeout(state.filterInputTimer);
        }
        state.filterError = "";
        syncFilterInputState();
        renderSmartFilterSuggestions(nextQuery);
        state.filterInputTimer = window.setTimeout(() => {
            state.filterInputTimer = null;
            applySmartFilterQuery(nextQuery);
        }, FILTER_INPUT_DEBOUNCE_MS);
    });

    elements.searchInput.addEventListener("focus", () => {
        scheduleSmartFilterSuggestionRefresh();
    });

    elements.searchInput.addEventListener("click", () => {
        scheduleSmartFilterSuggestionRefresh();
    });

    elements.searchInput.addEventListener("mouseup", () => {
        scheduleSmartFilterSuggestionRefresh();
    });

    elements.searchInput.addEventListener("select", () => {
        scheduleSmartFilterSuggestionRefresh();
    });

    elements.searchInput.addEventListener("blur", () => {
        window.setTimeout(() => {
            hideSmartFilterSuggestions();
        }, 120);
    });

    elements.searchInput.addEventListener("keydown", (event) => {
        if (event.key === "ArrowDown" && areSmartFilterSuggestionsVisible()) {
            event.preventDefault();
            moveSmartFilterSuggestionSelection(1);
            return;
        }
        if (event.key === "ArrowUp" && areSmartFilterSuggestionsVisible()) {
            event.preventDefault();
            moveSmartFilterSuggestionSelection(-1);
            return;
        }
        if ((event.key === "Enter" || event.key === "Tab") && areSmartFilterSuggestionsVisible() && state.activeSmartFilterSuggestionIndex >= 0) {
            event.preventDefault();
            applySmartFilterSuggestion(state.activeSmartFilterSuggestionIndex);
            return;
        }
        if (event.key === "Escape") {
            hideSmartFilterSuggestions();
        }
    });

    elements.searchInput.addEventListener("keyup", (event) => {
        if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
            scheduleSmartFilterSuggestionRefresh();
        }
    });

    if (elements.smartFilterSuggestions) {
        elements.smartFilterSuggestions.addEventListener("mousedown", (event) => {
            event.preventDefault();
            const item = event.target.closest(".smart-filter-suggestion-item[data-index]");
            if (!item) {
                return;
            }
            const index = Number(item.dataset.index);
            if (Number.isInteger(index)) {
                applySmartFilterSuggestion(index);
            }
        });
    }

    if (elements.smartFilterHelpButton) {
        elements.smartFilterHelpButton.addEventListener("click", () => {
            toggleSmartFilterHelp();
        });
    }

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

    if (elements.rowContextMenu) {
        elements.rowContextMenu.addEventListener("click", async (event) => {
            const item = event.target.closest(".row-context-menu-item[data-action]");
            if (!item || item.disabled || !state.contextMenuRowId) {
                return;
            }
            await handleRowContextMenuAction(item.dataset.action || "", state.contextMenuRowId);
        });
    }

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
        if (elements.rowContextMenu && !elements.rowContextMenu.hidden && !event.target.closest("#rowContextMenu")) {
            closeRowContextMenu();
        }
        if (!elements.smartFilterHelp.hidden && !event.target.closest("#smartFilterHelp") && !event.target.closest("#smartFilterHelpButton")) {
            closeSmartFilterHelp();
        }
        if (!event.target.closest(".smart-filter-entry")) {
            hideSmartFilterSuggestions();
        }
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeRowContextMenu();
            closeColumnMenu();
            closeSmartFilterHelp();
        }
    });

    document.addEventListener("selectionchange", () => {
        if (document.activeElement === elements.searchInput) {
            scheduleSmartFilterSuggestionRefresh();
        }
    });

    if (elements.treegridWrap) {
        elements.treegridWrap.addEventListener("scroll", () => {
            closeRowContextMenu();
            scheduleVirtualRender(false);
        }, { passive: true });
    }

    window.addEventListener("resize", () => {
        closeRowContextMenu();
        state.virtualHeightCache.clear();
        state.lastVirtualRangeKey = "";
        scheduleVirtualRender(true);
    });

    renderColumnOptions();
    syncFilterInputState();
    render();
}());
