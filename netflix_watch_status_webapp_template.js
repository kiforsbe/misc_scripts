(function () {
    const report = JSON.parse(document.getElementById("netflixWatchStatusReport").textContent);
    const rows = Array.isArray(report.rows) ? report.rows : [];
    const columns = Array.isArray(report.columns) ? report.columns : [];
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
        expanded: new Set(rows.filter((row) => row.has_children).map((row) => row.id)),
    };

    const elements = {
        sourceCsv: document.getElementById("sourceCsv"),
        generatedAt: document.getElementById("generatedAt"),
        visibleCount: document.getElementById("visibleCount"),
        searchInput: document.getElementById("searchInput"),
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
        elements.selectionTitle.textContent = row.title || formatItemType(row.item_type);
        elements.selectionSubtitle.textContent = subtitleForRow(row, ancestors);

        renderPreview(row);

        const detailItems = [
            { label: "Type", value: formatItemType(row.item_type) },
            { label: "Title", value: row.title },
            { label: "Release Year", value: row.year },
            { label: "Season", value: row.season },
            { label: "Season Title", value: row.season_title },
            { label: "Episode", value: row.episode },
            { label: "Episode Title", value: row.episode_title },
            { label: "Views", value: row.views },
            { label: "Watch Years", value: row.watch_dates },
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
        elements.treegridHeaderRow.innerHTML = columns.map((column, index) => `
            <th class="${column.align === "right" ? "align-right" : ""}">${escapeHtml(column.header || column.key || `Column ${index + 1}`)}</th>
        `).join("");
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
            const parts = [];
            if (row.season) {
                parts.push(`Season ${row.season}`);
            }
            if (row.watch_dates) {
                parts.push(`Watched ${row.watch_dates}`);
            }
            return parts.join(" • ");
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
            <div class="treecell" style="--level:${row.level}">
                <span class="tree-indent"></span>
                <button class="tree-toggle ${row.has_children ? "" : "is-hidden"}" data-action="toggle" data-row-id="${escapeHtml(row.id)}" aria-label="${escapeHtml(toggleLabel)}">
                    ${row.has_children ? (expanded ? "−" : "+") : ""}
                </button>
                <button class="tree-name-button" data-action="select" data-row-id="${escapeHtml(row.id)}">
                    <div class="tree-title-block">
                        <span class="tree-title">${escapeHtml(row.title)}</span>
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
            return row.episode_title ? escapeHtml(row.episode_title) : `<span class="item-pill type-${escapeHtml(row.item_type)}">${escapeHtml(formatItemType(row.item_type))}</span>`;
        }
        return escapeHtml(row[column.key] || "");
    }

    function renderRows() {
        const visibleRows = flattenVisibleRows(null);
        elements.visibleCount.textContent = String(visibleRows.length);
        elements.resultBody.innerHTML = visibleRows.map((row) => `
            <tr class="tree-row ${row.id === state.selectedId ? "is-selected" : ""}" data-row-id="${escapeHtml(row.id)}">
                ${columns.map((column) => `<td class="${column.align === "right" ? "align-right" : ""}">${cellMarkup(row, column)}</td>`).join("")}
            </tr>
        `).join("");
    }

    function render() {
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

    elements.sourceCsv.textContent = report.meta && report.meta.source_csv ? report.meta.source_csv : "";
    elements.generatedAt.textContent = report.meta && report.meta.generated_at ? report.meta.generated_at : "";
    renderHeader();
    renderSummary();
    render();
}());
