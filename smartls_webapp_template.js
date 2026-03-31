(function () {
    const report = SMARTLS_REPORT;
    const nodeMap = new Map();
    const state = {
        query: "",
        type: "all",
        ext: "all",
        sortKey: "name_path",
        sortDirection: "asc",
        expanded: new Set(),
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
        elements.generatedAt.textContent = `Generated: ${report.meta.generated_at}`;
        elements.sortMode.textContent = `Sort: ${report.meta.sort}`;
        elements.folderCount.textContent = `${report.directories.length} folders in tree`;
        elements.summaryGrid.innerHTML = summaryCards.map(([label, value]) => `
            <article class="summary-card">
                <span>${escapeHtml(label)}</span>
                <strong>${escapeHtml(value)}</strong>
            </article>
        `).join("");
        elements.extFilter.innerHTML += extValues.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("");
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
        const queryMatches = !query || [node.name_path, node.absolute_path, node.extension, node.mime_type, node.owner, node.group]
            .filter(Boolean)
            .join(" ")
            .toLowerCase()
            .includes(query);
        return Boolean(node.matched) && typeMatches && extMatches && queryMatches;
    }

    function childNodes(node) {
        const direction = state.sortDirection === "asc" ? 1 : -1;
        return node.children
            .map((key) => nodeMap.get(key))
            .filter(Boolean)
            .sort((left, right) => direction * compareValues(left[state.sortKey], right[state.sortKey]));
    }

    function metadataText(node) {
        if (node.entryType === "d") {
            const flags = [];
            if (node.is_empty) {
                flags.push("empty");
            } else if (node.is_sparse) {
                flags.push("sparse");
            }
            return [`${node.direct_files ?? 0} files`, `${node.direct_dirs ?? 0} dirs`, flags.join(", ")].filter(Boolean).join(" · ");
        }
        return [node.extension || node.type, node.mime_type || "-", node.owner || null].filter(Boolean).join(" · ");
    }

    function collectVisibleRows() {
        const hasFilter = Boolean(state.query.trim()) || state.type !== "all" || state.ext !== "all";
        const rows = [];
        const directMatchCount = { value: 0 };
        const rootNodes = [...nodeMap.values()]
            .filter((node) => !node.parent_path)
            .sort((left, right) => compareValues(left[state.sortKey], right[state.sortKey]));

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
        elements.resultCount.textContent = `${rows.length} visible rows · ${directMatchCount} direct matches`;
        elements.activeDirectory.textContent = `${state.expanded.size} expanded folders`;
        elements.currentPath.textContent = "Filesystem Tree";

        if (!rows.length) {
            elements.resultBody.innerHTML = '<tr><td colspan="7"><div class="empty-state">No entries match the current browser filters.</div></td></tr>';
            return;
        }

        elements.resultBody.innerHTML = rows.map(({ node, depth, ancestorOnly }) => {
            const hasChildren = node.entryType === "d" && node.children.length > 0;
            const expanded = state.expanded.has(node.key);
            const toggle = hasChildren
                ? `<button type="button" class="tree-toggle" data-toggle="${escapeHtml(node.key)}" aria-label="${expanded ? "Collapse" : "Expand"} ${escapeHtml(node.name_path || node.path)}">${expanded ? "−" : "+"}</button>`
                : '<span class="tree-toggle placeholder">+</span>';
            const rowClass = ancestorOnly ? "tree-row ancestor-row" : "tree-row";
            const subline = ancestorOnly ? "Ancestor context" : metadataText(node);
            return `
                <tr class="${rowClass}" data-entry-type="${escapeHtml(node.entryType)}">
                    <td class="tree-name-cell">
                        <div class="tree-name-content">
                            <span class="tree-indent" style="width: ${depth * 22}px"></span>
                            ${toggle}
                            <div class="tree-name-block">
                                <div class="tree-name-text">${escapeHtml(node.name_path || node.path)}</div>
                                <div class="name-subline">${escapeHtml(subline)}</div>
                                <div class="path-meta">${escapeHtml(node.absolute_path || "-")}</div>
                            </div>
                        </div>
                    </td>
                    <td><span class="badge">${node.entryType === "d" ? "Directory" : "File"}</span></td>
                    <td>${formatSize(node.size_bytes)}</td>
                    <td>${escapeHtml(node.modified || "-")}</td>
                    <td>${escapeHtml(node.created || "-")}</td>
                    <td>${escapeHtml(node.depth ?? depth)}</td>
                    <td>
                        <div class="details">children ${escapeHtml(node.direct_children ?? 0)}</div>
                        <div class="details">mime ${escapeHtml(node.mime_type || "-")}</div>
                        <div class="details">path ${escapeHtml(node.path || node.name_path)}</div>
                    </td>
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
            elements.searchInput.value = "";
            elements.typeFilter.value = "all";
            elements.extFilter.value = "all";
            state.expanded = new Set(report.directories.map((directory) => directory.path));
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

        document.querySelectorAll("th button[data-sort]").forEach((button) => {
            button.addEventListener("click", () => {
                const { sort } = button.dataset;
                if (state.sortKey === sort) {
                    state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
                } else {
                    state.sortKey = sort;
                    state.sortDirection = "asc";
                }
                renderTable();
            });
        });
    }

    buildNodeTree();
    populateStaticSections();
    buildBreadcrumb();
    wireControls();
    renderTable();
})();