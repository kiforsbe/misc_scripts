(function () {
    const report = SMARTLS_REPORT;
    const nodeMap = new Map();
    const nameColumn = { key: "name_path", label: "Name", className: "col-name" };
    const optionalColumns = [
        { key: "type", label: "Type", sortKey: "entryType", className: "col-type", type: "badge" },
        { key: "size", label: "Size", sortKey: "size_bytes", className: "col-size", type: "size" },
        { key: "modified", label: "Modified", sortKey: "modified_ts", className: "col-modified", type: "date" },
        { key: "created", label: "Created", sortKey: "created_ts", className: "col-meta-field", type: "date" },
        { key: "accessed", label: "Accessed", sortKey: "accessed_ts", className: "col-meta-field", type: "date" },
        { key: "children", label: "Children", sortKey: "direct_children", className: "col-meta-field", type: "number" },
        { key: "recursive_files", label: "Recursive files", sortKey: "recursive_files", className: "col-meta-field", type: "number" },
        { key: "mime", label: "Mime", sortKey: "mime_type", className: "col-meta-field", type: "text" },
        { key: "extension", label: "Extension", sortKey: "extension", className: "col-meta-field", type: "text" },
        { key: "full_path", label: "Full path", sortKey: "absolute_path", className: "col-path", type: "text" },
        { key: "owner", label: "Owner", sortKey: "owner", className: "col-meta-field", type: "text" },
        { key: "group", label: "Group", sortKey: "group", className: "col-meta-field", type: "text" },
        { key: "permissions", label: "Permissions", sortKey: "permissions_text", className: "col-meta-field", type: "text" },
    ];
    const state = {
        query: "",
        type: "all",
        ext: "all",
        sortKey: "name_path",
        sortDirection: "asc",
        expanded: new Set(),
        shownColumns: new Set(["type", "size", "modified"]),
    };

    const elements = {
        rootPath: document.getElementById("rootPath"),
        generatedAt: document.getElementById("generatedAt"),
        sortMode: document.getElementById("sortMode"),
        summaryGrid: document.getElementById("summaryGrid"),
        searchInput: document.getElementById("searchInput"),
        typeFilter: document.getElementById("typeFilter"),
        extFilter: document.getElementById("extFilter"),
        resetFilters: document.getElementById("resetFilters"),
        expandAll: document.getElementById("expandAll"),
        collapseAll: document.getElementById("collapseAll"),
        columnPickerButton: document.getElementById("columnPickerButton"),
        columnPickerMenu: document.getElementById("columnPickerMenu"),
        columnOptions: document.getElementById("columnOptions"),
        treegridColgroup: document.getElementById("treegridColgroup"),
        treegridHeaderRow: document.getElementById("treegridHeaderRow"),
        resultBody: document.getElementById("resultBody"),
        resultCount: document.getElementById("resultCount"),
        activeDirectory: document.getElementById("activeDirectory"),
        breadcrumb: document.getElementById("breadcrumb"),
        currentPath: document.getElementById("currentPath"),
        folderCount: document.getElementById("folderCount"),
    };

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/\"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatSize(sizeBytes) {
        const units = ["B", "KB", "MB", "GB", "TB"];
        let value = Number(sizeBytes || 0);
        let index = 0;
        while (value >= 1024 && index < units.length - 1) {
            value /= 1024;
            index += 1;
        }
        return index === 0 ? `${Math.round(value)} ${units[index]}` : `${value.toFixed(1)} ${units[index]}`;
    }

    function formatShortDate(timestamp) {
        if (!timestamp) {
            return { primary: "-", secondary: "", full: "-" };
        }
        const date = new Date(Number(timestamp) * 1000);
        const now = new Date();
        const sameYear = date.getFullYear() === now.getFullYear();
        const primary = new Intl.DateTimeFormat(undefined, {
            month: "short",
            day: "numeric",
            ...(sameYear ? {} : { year: "numeric" }),
        }).format(date);
        const secondary = new Intl.DateTimeFormat(undefined, {
            hour: "2-digit",
            minute: "2-digit",
        }).format(date);
        const full = new Intl.DateTimeFormat(undefined, {
            year: "numeric",
            month: "long",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
        }).format(date);
        return { primary, secondary, full };
    }

    function compareValues(left, right) {
        if (left === right) {
            return 0;
        }
        if (left === null || left === undefined || left === "") {
            return 1;
        }
        if (right === null || right === undefined || right === "") {
            return -1;
        }
        if (typeof left === "number" && typeof right === "number") {
            return left - right;
        }
        return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
    }

    function visibleOptionalColumns() {
        return optionalColumns.filter((option) => state.shownColumns.has(option.key));
    }

    function sortLabelForKey(key) {
        if (key === nameColumn.key) {
            return nameColumn.label;
        }
        const column = optionalColumns.find((item) => item.sortKey === key || item.key === key);
        return column ? column.label : key;
    }

    function updateSortDisplay() {
        elements.sortMode.textContent = `Sort: ${sortLabelForKey(state.sortKey)} (${state.sortDirection})`;
    }

    function sortIndicatorMarkup(direction) {
        if (direction === "none") {
            return "";
        }
        return `<span class="sort-chevron sort-chevron-${direction}" aria-hidden="true"></span>`;
    }

    const summaryCards = [
        ["Folders scanned", report.summary.folders_scanned],
        ["Folders matched", report.summary.folders_matched],
        ["Files listed", report.summary.files_listed],
        ["Total size", formatSize(report.summary.total_size_bytes)],
        ["Avg files/folder", report.summary.avg_files_per_folder],
        ["Emptiest folder", report.summary.emptiest_folder || "-"],
        ["Largest file", report.summary.largest_file || "-"],
    ];

    const extValues = [...new Set(report.entries.map((entry) => entry.extension).filter(Boolean))].sort();

    function buildNodeTree() {
        nodeMap.clear();

        report.directories.forEach((directory) => {
            nodeMap.set(directory.path, {
                ...directory,
                key: directory.path,
                entryType: "d",
                children: [],
            });
        });

        report.entries.forEach((entry) => {
            const key = entry.name_path;
            const existing = nodeMap.get(key) || { children: [] };
            nodeMap.set(key, {
                ...existing,
                ...entry,
                key,
                entryType: entry.type,
                children: existing.children || [],
            });
        });

        nodeMap.forEach((node) => {
            node.children = [];
        });

        nodeMap.forEach((node) => {
            if (!node.parent_path) {
                return;
            }
            const parent = nodeMap.get(node.parent_path);
            if (parent) {
                parent.children.push(node.key);
            }
        });

        state.expanded = new Set(report.directories.map((directory) => directory.path));
    }

    function populateStaticSections() {
        elements.rootPath.textContent = `Root: ${report.meta.root_path}`;
        const generatedAt = formatShortDate(report.meta.generated_at_ts);
        elements.generatedAt.textContent = `Generated: ${generatedAt.primary} ${generatedAt.secondary}`.trim();
        elements.generatedAt.title = generatedAt.full;
        updateSortDisplay();
        elements.folderCount.textContent = `${report.directories.length} folders in tree`;
        elements.summaryGrid.innerHTML = summaryCards.map(([label, value]) => `
            <article class="summary-card">
                <span>${escapeHtml(label)}</span>
                <strong>${escapeHtml(value)}</strong>
            </article>
        `).join("");
        elements.extFilter.innerHTML += extValues.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("");
    }

    function renderColumnOptions() {
        elements.columnOptions.innerHTML = optionalColumns.map((option) => `
            <label class="column-option">
                <input type="checkbox" value="${escapeHtml(option.key)}" ${state.shownColumns.has(option.key) ? "checked" : ""}>
                <span>${escapeHtml(option.label)}</span>
            </label>
        `).join("");
        renderTableColumns();
    }

    function renderTableColumns() {
        const visibleColumns = visibleOptionalColumns();
        elements.treegridColgroup.innerHTML = [
            `<col class="${nameColumn.className}">`,
            ...visibleColumns.map((column) => `<col class="${column.className}">`),
        ].join("");
        elements.treegridHeaderRow.innerHTML = [
            (() => {
                const sortDirection = state.sortKey === nameColumn.key ? state.sortDirection : "none";
                return `
                <th>
                    <button type="button" class="sort-button" data-sort="${escapeHtml(nameColumn.key)}" aria-sort="${sortDirection}" title="Sort by ${escapeHtml(nameColumn.label)}">
                        <span class="sort-label">${escapeHtml(nameColumn.label)}</span>
                        ${sortIndicatorMarkup(sortDirection)}
                    </button>
                </th>
            `;
            })(),
            ...visibleColumns.map((column) => {
                const sortDirection = state.sortKey === column.sortKey ? state.sortDirection : "none";
                return `
                <th>
                    <button type="button" class="sort-button" data-sort="${escapeHtml(column.sortKey)}" aria-sort="${sortDirection}" title="Sort by ${escapeHtml(column.label)}">
                        <span class="sort-label">${escapeHtml(column.label)}</span>
                        ${sortIndicatorMarkup(sortDirection)}
                    </button>
                </th>
            `;
            }),
        ].join("");
    }

    function buildBreadcrumb() {
        const rootDirectory = report.directories.find((directory) => directory.parent_path === null) || report.directories[0];
        if (!rootDirectory) {
            elements.breadcrumb.innerHTML = "";
            return;
        }
        elements.breadcrumb.innerHTML = [
            `<span class="breadcrumb-item">${escapeHtml(rootDirectory.path)}</span>`,
            '<span class="breadcrumb-separator">/</span>',
            '<span>Use folder rows to expand or collapse descendants</span>',
        ].join("");
    }

    function nodeMatches(node) {
        const query = state.query.trim().toLowerCase();
        const typeMatches = state.type === "all" || node.entryType === state.type;
        const extMatches = state.ext === "all" || node.extension === state.ext;
        const queryMatches = !query || [
            node.name_path,
            node.absolute_path,
            node.extension,
            node.mime_type,
            node.owner,
            node.group,
            node.permissions_text,
        ]
            .filter(Boolean)
            .join(" ")
            .toLowerCase()
            .includes(query);
        return Boolean(node.matched) && typeMatches && extMatches && queryMatches;
    }

    function sortValue(node, key) {
        if (key === "entryType") {
            return node.entryType === "d" ? "Directory" : "File";
        }
        if (key === "direct_children") {
            return Number(node.direct_children ?? 0);
        }
        if (key === "recursive_files") {
            return Number(node.recursive_files ?? 0);
        }
        if (key === "absolute_path") {
            return node.absolute_path || node.path || node.name_path || "";
        }
        return node[key];
    }

    function childNodes(node) {
        const direction = state.sortDirection === "asc" ? 1 : -1;
        return node.children
            .map((key) => nodeMap.get(key))
            .filter(Boolean)
            .sort((left, right) => direction * compareValues(sortValue(left, state.sortKey), sortValue(right, state.sortKey)));
    }

    function displayName(node) {
        if (node.name) {
            return String(node.name);
        }
        return String(node.name_path || node.path || "-");
    }

    function iconForNode(node) {
        if (node.entryType === "d") {
            return state.expanded.has(node.key) ? "📂" : "📁";
        }
        const extension = String(node.extension || "").toLowerCase();
        const mime = String(node.mime_type || "").toLowerCase();
        if ([".py", ".js", ".ts", ".tsx", ".jsx", ".json", ".html", ".css", ".md", ".sh", ".ps1", ".bat", ".yml", ".yaml", ".toml"].includes(extension)) {
            return "🧩";
        }
        if ([".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"].includes(extension) || mime.startsWith("image/")) {
            return "🖼️";
        }
        if ([".mp3", ".flac", ".wav", ".m4a", ".ogg"].includes(extension) || mime.startsWith("audio/")) {
            return "🎵";
        }
        if ([".mp4", ".mkv", ".avi", ".mov", ".webm"].includes(extension) || mime.startsWith("video/")) {
            return "🎬";
        }
        if ([".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz"].includes(extension)) {
            return "🗜️";
        }
        return "📄";
    }

    function renderDateCell(timestamp) {
        const value = formatShortDate(timestamp);
        return `
            <td class="meta-cell date-cell" title="${escapeHtml(value.full)}">
                <span class="date-inline">${escapeHtml(`${value.primary} ${value.secondary}`.trim())}</span>
            </td>
        `;
    }

    function renderOptionalCell(node, column) {
        if (column.type === "badge") {
            return `<td><span class="badge">${node.entryType === "d" ? "Directory" : "File"}</span></td>`;
        }
        if (column.type === "size") {
            return `<td class="meta-cell" title="${escapeHtml(String(node.size_bytes ?? 0))}">${formatSize(node.size_bytes)}</td>`;
        }
        if (column.type === "date") {
            return renderDateCell(sortValue(node, column.sortKey));
        }

        let value = "-";
        if (column.key === "children") {
            value = node.direct_children ?? 0;
        } else if (column.key === "recursive_files") {
            value = node.recursive_files ?? 0;
        } else if (column.key === "mime") {
            value = node.mime_type || "-";
        } else if (column.key === "extension") {
            value = node.extension || "-";
        } else if (column.key === "full_path") {
            value = node.absolute_path || "-";
        } else if (column.key === "owner") {
            value = node.owner || "-";
        } else if (column.key === "group") {
            value = node.group || "-";
        } else if (column.key === "permissions") {
            value = node.permissions_text || node.permissions_octal || "-";
        }
        return `<td class="meta-cell" title="${escapeHtml(String(value))}">${escapeHtml(value)}</td>`;
    }

    function collectVisibleRows() {
        const hasFilter = Boolean(state.query.trim()) || state.type !== "all" || state.ext !== "all";
        const rows = [];
        const directMatchCount = { value: 0 };
        const direction = state.sortDirection === "asc" ? 1 : -1;
        const rootNodes = [...nodeMap.values()]
            .filter((node) => !node.parent_path)
            .sort((left, right) => direction * compareValues(sortValue(left, state.sortKey), sortValue(right, state.sortKey)));

        function visit(node, depth) {
            const children = childNodes(node);
            const childRows = [];
            let descendantVisible = false;
            children.forEach((child) => {
                const childResult = visit(child, depth + 1);
                if (childResult.visible) {
                    descendantVisible = true;
                    childRows.push(...childResult.rows);
                }
            });

            const selfMatches = nodeMatches(node);
            if (selfMatches) {
                directMatchCount.value += 1;
            }

            const visible = selfMatches || descendantVisible;
            if (!visible) {
                return { visible: false, rows: [] };
            }

            const currentRow = { node, depth, ancestorOnly: !selfMatches && descendantVisible };
            const rowsForNode = [currentRow];
            const shouldShowChildren = hasFilter || node.entryType !== "d" || state.expanded.has(node.key);
            if (node.entryType === "d" && shouldShowChildren) {
                rowsForNode.push(...childRows);
            }
            return { visible: true, rows: rowsForNode };
        }

        rootNodes.forEach((node) => {
            const result = visit(node, 0);
            if (result.visible) {
                rows.push(...result.rows);
            }
        });

        return { rows, directMatchCount: directMatchCount.value };
    }

    function renderTable() {
        const { rows, directMatchCount } = collectVisibleRows();
        renderTableColumns();
        elements.resultCount.textContent = `${rows.length} visible rows · ${directMatchCount} direct matches`;
        elements.activeDirectory.textContent = `${state.expanded.size} expanded folders`;
        elements.currentPath.textContent = "Filesystem Tree";
        updateSortDisplay();

        const visibleColumns = visibleOptionalColumns();

        if (!rows.length) {
            elements.resultBody.innerHTML = `<tr><td colspan="${1 + visibleColumns.length}"><div class="empty-state">No entries match the current browser filters.</div></td></tr>`;
            return;
        }

        elements.resultBody.innerHTML = rows.map(({ node, depth, ancestorOnly }) => {
            const hasChildren = node.entryType === "d" && node.children.length > 0;
            const expanded = state.expanded.has(node.key);
            const toggle = hasChildren
                ? `<button type="button" class="tree-toggle" data-toggle="${escapeHtml(node.key)}" aria-label="${expanded ? "Collapse" : "Expand"} ${escapeHtml(node.name_path || node.path)}">${expanded ? "−" : "+"}</button>`
                : '<span class="tree-toggle placeholder">+</span>';
            const rowClass = ancestorOnly ? "tree-row ancestor-row" : "tree-row";
            const nameContent = hasChildren
                ? `<button type="button" class="tree-name-button" data-toggle="${escapeHtml(node.key)}" aria-label="${expanded ? "Collapse" : "Expand"} ${escapeHtml(node.name_path || node.path)}"><span class="entry-icon" aria-hidden="true">${iconForNode(node)}</span><span>${escapeHtml(displayName(node))}</span>${ancestorOnly ? '<span class="ancestor-pill">Ancestor context</span>' : ""}</button>`
                : `<div class="tree-name-text"><span class="entry-icon" aria-hidden="true">${iconForNode(node)}</span><span>${escapeHtml(displayName(node))}</span>${ancestorOnly ? '<span class="ancestor-pill">Ancestor context</span>' : ""}</div>`;
            return `
                <tr class="${rowClass}" data-entry-type="${escapeHtml(node.entryType)}">
                    <td class="tree-name-cell">
                        <div class="tree-name-content">
                            <span class="tree-indent" style="width: ${depth * 22}px"></span>
                            ${toggle}
                            <div class="tree-name-block">
                                ${nameContent}
                            </div>
                        </div>
                    </td>
                    ${visibleColumns.map((column) => renderOptionalCell(node, column)).join("")}
                </tr>
            `;
        }).join("");

        elements.resultBody.querySelectorAll("[data-toggle]").forEach((button) => {
            button.addEventListener("click", () => {
                const key = button.dataset.toggle;
                if (!key) {
                    return;
                }
                if (state.expanded.has(key)) {
                    state.expanded.delete(key);
                } else {
                    state.expanded.add(key);
                }
                renderTable();
            });
        });
    }

    function wireControls() {
        elements.searchInput.addEventListener("input", (event) => {
            state.query = event.target.value;
            renderTable();
        });

        elements.typeFilter.addEventListener("change", (event) => {
            state.type = event.target.value;
            renderTable();
        });

        elements.extFilter.addEventListener("change", (event) => {
            state.ext = event.target.value;
            renderTable();
        });

        elements.resetFilters.addEventListener("click", () => {
            state.query = "";
            state.type = "all";
            state.ext = "all";
            state.sortKey = "name_path";
            state.sortDirection = "asc";
            state.shownColumns = new Set(["type", "size", "modified"]);
            elements.searchInput.value = "";
            elements.typeFilter.value = "all";
            elements.extFilter.value = "all";
            state.expanded = new Set(report.directories.map((directory) => directory.path));
            renderColumnOptions();
            renderTable();
        });

        elements.expandAll.addEventListener("click", () => {
            state.expanded = new Set(report.directories.map((directory) => directory.path));
            renderTable();
        });

        elements.collapseAll.addEventListener("click", () => {
            const rootPaths = report.directories.filter((directory) => !directory.parent_path).map((directory) => directory.path);
            state.expanded = new Set(rootPaths);
            renderTable();
        });

        elements.treegridHeaderRow.addEventListener("click", (event) => {
            const button = event.target.closest("button[data-sort]");
            if (!button) {
                return;
            }
            const { sort } = button.dataset;
            if (!sort) {
                return;
            }
            if (state.sortKey === sort) {
                state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
            } else {
                state.sortKey = sort;
                state.sortDirection = "asc";
            }
            renderTable();
        });

        elements.columnPickerButton.addEventListener("click", () => {
            const isOpen = !elements.columnPickerMenu.hidden;
            elements.columnPickerMenu.hidden = isOpen;
            elements.columnPickerButton.setAttribute("aria-expanded", String(!isOpen));
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
            renderTable();
        });

        document.addEventListener("click", (event) => {
            if (!elements.columnPickerMenu.hidden && !event.target.closest("#columnPicker")) {
                elements.columnPickerMenu.hidden = true;
                elements.columnPickerButton.setAttribute("aria-expanded", "false");
            }
        });
    }

    buildNodeTree();
    populateStaticSections();
    renderColumnOptions();
    buildBreadcrumb();
    wireControls();
    renderTable();
})();