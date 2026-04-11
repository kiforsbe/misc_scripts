(function () {
    const report = JSON.parse(document.getElementById("netflixWatchStatusReport").textContent);
    const rows = Array.isArray(report.rows) ? report.rows : [];
    const columns = Array.isArray(report.columns) ? report.columns : [];
    const nameColumn = columns.find((column) => column.key === "title") || { key: "title", header: "Title", align: "left" };
    const optionalColumns = columns.filter((column) => column.key !== nameColumn.key);
    const defaultColumnOrder = optionalColumns.map((column) => column.key);
    const defaultVisibleColumns = new Set(["year"]);
    const rowMap = new Map(rows.map((row) => [row.id, row]));
    const childrenMap = new Map();

    rows.forEach((row) => {
        const key = row.parent_id || "ROOT";
        if (!childrenMap.has(key)) {
            childrenMap.set(key, []);
        }
        childrenMap.get(key).push(row);
    });

    const state = {
        query: "",
        selectedId: rows[0] ? rows[0].id : null,
        expanded: new Set(),
        shownColumns: new Set(defaultColumnOrder.filter((key) => defaultVisibleColumns.has(key))),
        columnOrder: [...defaultColumnOrder],
        draggingColumnKey: null,
        dragInsertPosition: "before",
    };

    const elements = {
        sourceCsv: document.getElementById("sourceCsv"),
        searchInput: document.getElementById("searchInput"),
        columnPickerMenu: document.getElementById("columnPickerMenu"),
        columnOptions: document.getElementById("columnOptions"),
        treegridHeaderRow: document.getElementById("treegridHeaderRow"),
        resultBody: document.getElementById("resultBody"),
        summaryGrid: document.getElementById("summaryGrid"),
        selectionTitle: document.getElementById("selectionTitle"),
        selectionSubtitle: document.getElementById("selectionSubtitle"),
        selectionDetails: document.getElementById("selectionDetails"),
        selectionContext: document.getElementById("selectionContext"),
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

    function renderSummary() {
        const summary = report.summary || {};
        const cards = [
            { label: "Entries", value: summary.entries ?? 0 },
            { label: "Movies", value: summary.unique_movies ?? 0 },
            { label: "Series", value: summary.unique_series ?? 0 },
        ];
        elements.summaryGrid.innerHTML = cards.map((card) => `
            <div class="summary-card">
                <strong>${escapeHtml(card.value)}</strong>
                <span>${escapeHtml(card.label)}</span>
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
                <strong>Reserved for future thumbnail support</strong>
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
            elements.selectionContext.innerHTML = '<div class="empty-state">No selection context.</div>';
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

        const contextItems = [
            { label: "Series / Parent", value: ancestors.map((item) => item.title).join(" / ") },
            { label: "Tree Depth", value: row.level },
            { label: "Thumbnail Status", value: row.thumbnail && row.thumbnail.status ? row.thumbnail.status : "not_requested" },
        ];
        elements.selectionContext.innerHTML = selectionItems(contextItems);
    }

    function renderHeader() {
        const gutterHeader = '<th class="gutter-column" aria-hidden="true"></th>';
        const trailingGutterHeader = '<th class="gutter-column right-gutter-column" aria-hidden="true"></th>';
        const fixedHeader = `
            <th class="column-${escapeHtml(nameColumn.key)} ${nameColumn.align === "right" ? "align-right" : ""}">
                <span class="column-header-button is-fixed">${escapeHtml(nameColumn.header || nameColumn.key)}</span>
            </th>
        `;
        const dataHeaders = visibleOptionalColumns().map((column, index) => `
            <th class="optional-column-header column-${escapeHtml(column.key)} ${column.align === "right" ? "align-right" : ""}" data-column-key="${escapeHtml(column.key)}" draggable="true">
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
        return `
            <div class="treecell ${row.watch_state === "unwatched" ? "is-unwatched" : ""}" style="--level:${row.level}">
                <span class="tree-indent"></span>
                <button class="tree-toggle ${row.has_children ? "" : "is-hidden"}" data-action="toggle" data-row-id="${escapeHtml(row.id)}" aria-label="${escapeHtml(toggleLabel)}">
                    ${row.has_children ? (expanded ? "−" : "+") : ""}
                </button>
                <button class="tree-name-button" data-action="select" data-row-id="${escapeHtml(row.id)}">
                    <div class="tree-title-block">
                        <span class="tree-title">${escapeHtml(displayTitle(row))}</span>
                        <span class="tree-subtitle">${escapeHtml(rowSubtitle(row))}</span>
                    </div>
                </button>
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

    function renderRows() {
        const visibleRows = flattenVisibleRows(null);
        const visibleColumns = visibleOptionalColumns();
        elements.resultBody.innerHTML = visibleRows.map((row) => `
            <tr class="tree-row ${row.id === state.selectedId ? "is-selected" : ""} ${row.watch_state === "unwatched" ? "is-unwatched" : ""}" data-row-id="${escapeHtml(row.id)}">
                <td class="gutter-column">${gutterCellMarkup(row)}</td>
                <td class="column-${escapeHtml(nameColumn.key)} ${nameColumn.align === "right" ? "align-right" : ""}">${cellMarkup(row, nameColumn)}</td>
                ${visibleColumns.map((column) => `<td class="column-${escapeHtml(column.key)} ${column.align === "right" ? "align-right" : ""}">${cellMarkup(row, column)}</td>`).join("")}
                <td class="gutter-column right-gutter-column"></td>
            </tr>
        `).join("");
    }

    function render() {
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

    elements.sourceCsv.textContent = report.meta && report.meta.source_csv ? report.meta.source_csv : "";
    renderColumnOptions();
    renderSummary();
    render();
}());
