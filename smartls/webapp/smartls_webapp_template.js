(function () {
    const report = JSON.parse(document.getElementById("smartlsReport").textContent);
    const FILTER_INPUT_DEBOUNCE_MS = 800;
    const VIRTUAL_ROW_OVERSCAN = 10;
    const VIRTUAL_ROW_ESTIMATE = 44;
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
        { key: "relative_path", label: "Relative path", sortKey: "path", className: "col-path", type: "text" },
        { key: "full_path", label: "Full path", sortKey: "absolute_path", className: "col-path", type: "text" },
        { key: "owner", label: "Owner", sortKey: "owner", className: "col-meta-field", type: "text" },
        { key: "group", label: "Group", sortKey: "group", className: "col-meta-field", type: "text" },
        { key: "permissions", label: "Permissions", sortKey: "permissions_text", className: "col-meta-field", type: "text" },
    ];
    const defaultColumnOrder = optionalColumns.map((column) => column.key);
    const state = {
        query: "",
        compiledFilter: (node) => Boolean(node.matched),
        filterSummary: "Smart filter ready",
        filterError: "",
        filterInputTimer: null,
        sortKey: "name_path",
        sortDirection: "asc",
        expanded: new Set(),
        shownColumns: new Set(["type", "size", "modified"]),
        columnOrder: [...defaultColumnOrder],
        draggingColumnKey: null,
        selectedKey: null,
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
        addressValue: document.getElementById("addressValue"),
        generatedAt: document.getElementById("generatedAt"),
        sortMode: document.getElementById("sortMode"),
        summaryGrid: document.getElementById("summaryGrid"),
        selectionDetails: document.getElementById("selectionDetails"),
        searchInput: document.getElementById("searchInput"),
        filterStatus: document.getElementById("filterStatus"),
        resetFilters: document.getElementById("resetFilters"),
        expandAll: document.getElementById("expandAll"),
        collapseAll: document.getElementById("collapseAll"),
        columnPickerButton: document.getElementById("columnPickerButton"),
        columnPickerMenu: document.getElementById("columnPickerMenu"),
        columnOptions: document.getElementById("columnOptions"),
        treegridColgroup: document.getElementById("treegridColgroup"),
        treegridHeaderRow: document.getElementById("treegridHeaderRow"),
        treegridWrap: document.querySelector(".treegrid-wrap"),
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

    function displayPath(value) {
        return String(value ?? "").replace(/\\/g, "/");
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
        return state.columnOrder
            .map((key) => optionalColumns.find((option) => option.key === key))
            .filter(Boolean)
            .filter((option) => state.shownColumns.has(option.key));
    }

    function visibleColumnKeys() {
        return visibleOptionalColumns().map((column) => column.key).join("|");
    }

    function orderedOptionalColumns() {
        return state.columnOrder
            .map((key) => optionalColumns.find((option) => option.key === key))
            .filter(Boolean);
    }

    function sortLabelForKey(key) {
        if (key === nameColumn.key) {
            return nameColumn.label;
        }
        const column = optionalColumns.find((item) => item.sortKey === key || item.key === key);
        return column ? column.label : key;
    }

    function updateSortDisplay() {
        if (elements.sortMode) {
            elements.sortMode.textContent = `Sort: ${sortLabelForKey(state.sortKey)} (${state.sortDirection})`;
        }
    }

    function sortIndicatorMarkup(direction) {
        if (direction === "none") {
            return "";
        }
        return `<span class="sort-chevron sort-chevron-${direction}" aria-hidden="true"></span>`;
    }

    function rootDirectories() {
        return report.directories.filter((directory) => directory.parent_path === null);
    }

    function defaultExpandedPaths() {
        return new Set(rootDirectories().map((directory) => directory.path));
    }

    function selectedNode() {
        return state.selectedKey ? nodeMap.get(state.selectedKey) || null : null;
    }

    function selectedDirectoryNode() {
        const node = selectedNode();
        if (!node) {
            return null;
        }
        if (node.entryType === "d") {
            return node;
        }
        return node.parent_path ? nodeMap.get(node.parent_path) || null : null;
    }

    function setSelectedKey(key) {
        if (!key || !nodeMap.has(key)) {
            return;
        }
        state.selectedKey = key;
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

    function clearDragState() {
        state.draggingColumnKey = null;
        elements.treegridHeaderRow.querySelectorAll(".optional-column-header").forEach((header) => {
            header.classList.remove("drag-over");
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

    function normalizeSizeToken(token) {
        const match = String(token || "").trim().match(/^(-?\d+(?:\.\d+)?)(B|KB|MB|GB|TB)?$/i);
        if (!match) {
            throw new Error(`Invalid size token: ${token}`);
        }
        const value = Number.parseFloat(match[1]);
        const unit = (match[2] || "B").toUpperCase();
        const units = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 };
        return Math.trunc(value * units[unit]);
    }

    function normalizeDurationToken(token) {
        const match = String(token || "").trim().match(/^(-?\d+(?:\.\d+)?)([smhdw])$/i);
        if (!match) {
            throw new Error(`Invalid time token: ${token}`);
        }
        const value = Number.parseFloat(match[1]);
        const unit = match[2].toLowerCase();
        const secondsPerUnit = { s: 1, m: 60, h: 3600, d: 86400, w: 604800 };
        return value * secondsPerUnit[unit];
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

    function parseSizeExpr(expr) {
        return parseNumericExpr(transformNumericExpr(expr, normalizeSizeToken));
    }

    function parseTimeExpr(expr) {
        return parseNumericExpr(transformNumericExpr(expr, normalizeDurationToken));
    }

    function ageSeconds(timestamp) {
        if (!timestamp) {
            return Number.POSITIVE_INFINITY;
        }
        return Math.max(0, (Date.now() / 1000) - Number(timestamp));
    }

    function normalizeExtension(extension) {
        const value = String(extension || "").trim().toLowerCase();
        if (!value) {
            return "";
        }
        return value.startsWith(".") ? value : `.${value}`;
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

    function createStringMatcher(rawValue) {
        const value = String(rawValue || "").trim();
        if (!value) {
            throw new Error("Filter value cannot be empty");
        }
        if (value.includes("*") || value.includes("?")) {
            const regex = globToRegExp(value);
            return (candidate) => regex.test(String(candidate || ""));
        }
        const lowered = value.toLowerCase();
        return (candidate) => String(candidate || "").toLowerCase().includes(lowered);
    }

    function pathTail(value, segmentCount) {
        const normalized = displayPath(value || "");
        const parts = normalized.split("/").filter(Boolean);
        if (!parts.length) {
            return normalized || "-";
        }
        return parts.slice(-segmentCount).join("/");
    }

    function searchBlob(node) {
        return [
            displayName(node),
            node.name_path,
            displayPath(node.absolute_path),
            displayPath(node.path),
            node.extension,
            node.mime_type,
            node.owner,
            node.group,
            node.permissions_text,
            node.permissions_octal,
        ].filter(Boolean).join(" ").toLowerCase();
    }

    function normalizeTypeToken(value) {
        const lowered = String(value || "").trim().toLowerCase();
        if (["f", "file", "files"].includes(lowered)) {
            return "f";
        }
        if (["d", "dir", "dirs", "directory", "directories", "folder", "folders"].includes(lowered)) {
            return "d";
        }
        throw new Error(`Unknown type filter: ${value}`);
    }

    function buildFieldPredicate(fieldName, rawValue) {
        const field = ({
            modified: "mtime",
            mtime: "mtime",
            created: "ctime",
            ctime: "ctime",
            accessed: "atime",
            atime: "atime",
            extension: "ext",
            ext: "ext",
            depth: "depth",
            "depth-filter": "depth",
            count: "count",
            children: "count",
            files: "files",
            dirs: "dirs",
            type: "type",
            name: "name",
            size: "size",
            empty: "empty",
            sparse: "sparse",
            mime: "mime",
            owner: "owner",
            group: "group",
            permissions: "permissions",
            permission: "permissions",
            perm: "permissions",
            path: "path",
            full: "path",
            relative: "relative",
        })[String(fieldName || "").trim().toLowerCase()];

        if (!field) {
            throw new Error(`Unknown filter field: ${fieldName}`);
        }

        if (["empty", "sparse"].includes(field)) {
            if (rawValue) {
                throw new Error(`${field} does not take a value`);
            }
        }

        if (field === "count") {
            const matcher = parseNumericExpr(rawValue);
            return (node) => node.entryType === "d" && matcher(Number(node.direct_children ?? 0));
        }
        if (field === "files") {
            const matcher = parseNumericExpr(rawValue);
            return (node) => node.entryType === "d" && matcher(Number(node.recursive_files ?? 0));
        }
        if (field === "dirs") {
            const matcher = parseNumericExpr(rawValue);
            return (node) => node.entryType === "d" && matcher(Number(node.direct_dirs ?? 0));
        }
        if (field === "size") {
            const matcher = parseSizeExpr(rawValue);
            return (node) => matcher(Number(node.size_bytes ?? 0));
        }
        if (field === "mtime") {
            const matcher = parseTimeExpr(rawValue);
            return (node) => matcher(ageSeconds(node.modified_ts));
        }
        if (field === "ctime") {
            const matcher = parseTimeExpr(rawValue);
            return (node) => matcher(ageSeconds(node.created_ts));
        }
        if (field === "atime") {
            const matcher = parseTimeExpr(rawValue);
            return (node) => matcher(ageSeconds(node.accessed_ts));
        }
        if (field === "name") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(displayName(node));
        }
        if (field === "ext") {
            const allowed = new Set(String(rawValue || "").split(",").map((part) => normalizeExtension(part)).filter(Boolean));
            if (!allowed.size) {
                throw new Error("Extension filter cannot be empty");
            }
            return (node) => node.entryType === "f" && allowed.has(normalizeExtension(node.extension));
        }
        if (field === "depth") {
            const matcher = parseNumericExpr(rawValue);
            return (node) => matcher(Number(node.depth ?? 0));
        }
        if (field === "type") {
            const expected = normalizeTypeToken(rawValue);
            return (node) => node.entryType === expected;
        }
        if (field === "empty") {
            return (node) => node.entryType === "d" && Number(node.recursive_files ?? 0) === 0;
        }
        if (field === "sparse") {
            return (node) => node.entryType === "d" && Number(node.recursive_files ?? 0) <= 3;
        }
        if (field === "mime") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(node.mime_type || "");
        }
        if (field === "owner") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(node.owner || "");
        }
        if (field === "group") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(node.group || "");
        }
        if (field === "permissions") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(node.permissions_text || node.permissions_octal || "");
        }
        if (field === "path") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(displayPath(node.absolute_path || node.path || node.name_path || ""));
        }
        if (field === "relative") {
            const matcher = createStringMatcher(rawValue);
            return (node) => matcher(displayPath(node.path || node.name_path || ""));
        }
        throw new Error(`Unsupported filter field: ${fieldName}`);
    }

    function buildFreeTextPredicate(token) {
        const lowered = String(token || "").trim().toLowerCase();
        if (!lowered) {
            return () => true;
        }
        return (node) => searchBlob(node).includes(lowered);
    }

    function compileSmartFilter(text) {
        const trimmed = String(text || "").trim();
        if (!trimmed) {
            return {
                predicate: (node) => Boolean(node.matched),
                summary: "Smart filter ready",
            };
        }

        const tokens = tokenizeQuery(trimmed);
        const groups = [[]];
        let negateNext = false;
        let clauseCount = 0;

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
                const canonicalField = ({
                    modified: "mtime",
                    created: "ctime",
                    accessed: "atime",
                    extension: "ext",
                    permission: "permissions",
                    perm: "permissions",
                })[fieldName.toLowerCase()] || fieldName.toLowerCase();
                const isValueLess = ["empty", "sparse"].includes(canonicalField);
                let rawValue = "";
                if (!isValueLess) {
                    if (index + 1 >= tokens.length) {
                        throw new Error(`Missing value for --${fieldName}`);
                    }
                    rawValue = tokens[index + 1];
                    index += 1;
                }
                predicate = buildFieldPredicate(canonicalField, rawValue);
            } else {
                const separatorIndex = token.indexOf(":");
                if (separatorIndex > 0) {
                    const fieldName = token.slice(0, separatorIndex);
                    const rawValue = token.slice(separatorIndex + 1);
                    const knownField = [
                        "name", "size", "mtime", "modified", "ctime", "created", "atime", "accessed", "ext", "extension", "depth", "depth-filter", "type",
                        "count", "children", "files", "dirs", "empty", "sparse", "mime", "owner", "group", "permissions", "permission", "perm", "path", "full", "relative",
                    ].includes(fieldName.toLowerCase());
                    if (knownField) {
                        predicate = buildFieldPredicate(fieldName, rawValue);
                    } else {
                        predicate = buildFreeTextPredicate(token);
                    }
                } else {
                    predicate = buildFreeTextPredicate(token);
                }
            }

            if (negateNext) {
                const inner = predicate;
                predicate = (node) => !inner(node);
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
            predicate: (node) => Boolean(node.matched) && groups.some((group) => group.every((filter) => filter(node))),
            summary: `${clauseCount} clause${clauseCount === 1 ? "" : "s"} active${groups.length > 1 ? ` across ${groups.length} groups` : ""}`,
        };
    }

    function compactDisplayValue(value, segmentCount) {
        const normalized = displayPath(value || "");
        if (!normalized) {
            return "-";
        }
        const tail = pathTail(normalized, segmentCount);
        return tail.length < normalized.length ? `.../${tail}` : tail;
    }

    function renderFilterStatus() {
        if (!elements.filterStatus) {
            return;
        }
        elements.filterStatus.classList.toggle("is-error", Boolean(state.filterError));
        elements.filterStatus.textContent = state.filterError || state.filterSummary;
        elements.filterStatus.title = state.filterError || state.filterSummary;
    }

    function applySmartFilterQuery(nextQuery) {
        state.query = nextQuery;
        try {
            const compiled = compileSmartFilter(state.query);
            state.compiledFilter = compiled.predicate;
            state.filterSummary = compiled.summary;
            state.filterError = "";
        } catch (error) {
            state.filterError = error instanceof Error ? error.message : String(error);
        }
        renderTable();
    }

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

        if (!state.selectedKey) {
            const firstRoot = rootDirectories()[0];
            state.selectedKey = firstRoot ? firstRoot.path : report.entries[0]?.name_path || null;
        }

        state.expanded = defaultExpandedPaths();
    }

    function populateStaticSections() {
        elements.addressValue.title = displayPath(report.meta.root_path);
        const generatedAt = formatShortDate(report.meta.generated_at_ts);
        if (elements.generatedAt) {
            elements.generatedAt.textContent = `Generated ${generatedAt.primary} ${generatedAt.secondary}`.trim();
            elements.generatedAt.title = generatedAt.full;
        }
        updateSortDisplay();
        const summaryCards = [
            { label: "Folders scanned", value: String(report.summary.folders_scanned), title: String(report.summary.folders_scanned) },
            { label: "Folders matched", value: String(report.summary.folders_matched), title: String(report.summary.folders_matched) },
            { label: "Files listed", value: String(report.summary.files_listed), title: String(report.summary.files_listed) },
            { label: "Total size", value: formatSize(report.summary.total_size_bytes), title: String(report.summary.total_size_bytes ?? 0) },
        ];
        elements.summaryGrid.innerHTML = summaryCards.map(({ label, value, title }) => `
            <article class="summary-card">
                <span>${escapeHtml(label)}</span>
                <strong title="${escapeHtml(title)}">${escapeHtml(value)}</strong>
            </article>
        `).join("");
        renderFilterStatus();
    }

    function selectionItems(node) {
        if (!node) {
            return [["Selection", "Nothing selected"]];
        }

        const modified = formatShortDate(node.modified_ts);
        const items = [
            ["Name", displayName(node)],
            ["Type", node.entryType === "d" ? "Directory" : "File"],
            ["Path", displayPath(node.absolute_path || node.path || node.name_path || "-")],
        ];

        if (node.entryType === "d") {
            items.push(["Children", String(node.direct_children ?? 0)]);
            items.push(["Recursive files", String(node.recursive_files ?? 0)]);
        } else {
            items.push(["Size", formatSize(node.size_bytes)]);
            items.push(["Extension", node.extension || "-"]);
        }

        items.push(["Modified", `${modified.primary} ${modified.secondary}`.trim()]);
        return items;
    }

    function renderSelectionDetails() {
        elements.selectionDetails.innerHTML = selectionItems(selectedNode()).map(([label, value]) => `
            <div class="selection-row">
                <span class="selection-label">${escapeHtml(label)}</span>
                <span class="selection-value" title="${escapeHtml(value)}">${escapeHtml(value)}</span>
            </div>
        `).join("");
    }

    function renderColumnOptions() {
        elements.columnOptions.innerHTML = orderedOptionalColumns().map((option) => `
            <label class="column-option">
                <input class="column-toggle-input" type="checkbox" value="${escapeHtml(option.key)}" ${state.shownColumns.has(option.key) ? "checked" : ""}>
                <span class="column-toggle" aria-hidden="true"></span>
                <span class="column-option-label">${escapeHtml(option.label)}</span>
            </label>
        `).join("");
        state.virtualHeightCache.clear();
        state.lastVirtualRangeKey = "";
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
                <th class="optional-column-header" data-column-key="${escapeHtml(column.key)}">
                    <button type="button" class="sort-button draggable-column-button" data-sort="${escapeHtml(column.sortKey)}" data-column-key="${escapeHtml(column.key)}" draggable="true" aria-sort="${sortDirection}" title="Sort by ${escapeHtml(column.label)}">
                        <span class="sort-label">${escapeHtml(column.label)}</span>
                        ${sortIndicatorMarkup(sortDirection)}
                    </button>
                </th>
            `;
            }),
        ].join("");
    }

    function buildBreadcrumb() {
        const node = selectedDirectoryNode() || selectedNode();
        if (!node) {
            elements.breadcrumb.innerHTML = "";
            return;
        }

        const chain = [];
        let current = node;
        while (current) {
            chain.unshift(current);
            current = current.parent_path ? nodeMap.get(current.parent_path) || null : null;
        }

        elements.breadcrumb.innerHTML = chain.map((item, index) => `
            <button type="button" class="breadcrumb-item ${index === chain.length - 1 ? "current" : ""}" data-select-path="${escapeHtml(item.key)}" title="${escapeHtml(displayPath(item.absolute_path || item.path || item.key))}" aria-current="${index === chain.length - 1 ? "location" : "false"}">${escapeHtml(displayName(item))}</button>${index < chain.length - 1 ? '<span class="breadcrumb-separator">/</span>' : ''}
        `).join("");
    }

    function nodeMatches(node) {
        return state.compiledFilter(node);
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
            return displayPath(node.absolute_path || node.path || node.name_path || "");
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
        const pathValue = displayPath(node.name_path || node.path || "-");
        const parts = pathValue.split("/").filter(Boolean);
        return parts.length ? parts[parts.length - 1] : pathValue;
    }

    function iconForNode(node) {
        if (node.entryType === "d") {
            return "folder";
        }
        const extension = String(node.extension || "").toLowerCase();
        const mime = String(node.mime_type || "").toLowerCase();
        if ([".py", ".js", ".ts", ".tsx", ".jsx", ".json", ".html", ".css", ".md", ".sh", ".ps1", ".bat", ".yml", ".yaml", ".toml"].includes(extension)) {
            return "code";
        }
        if ([".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"].includes(extension) || mime.startsWith("image/")) {
            return "image";
        }
        if ([".mp3", ".flac", ".wav", ".m4a", ".ogg"].includes(extension) || mime.startsWith("audio/")) {
            return "audio";
        }
        if ([".mp4", ".mkv", ".avi", ".mov", ".webm"].includes(extension) || mime.startsWith("video/")) {
            return "video";
        }
        if ([".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz"].includes(extension)) {
            return "archive";
        }
        return "file";
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
            return `<td class="meta-cell numeric-cell" title="${escapeHtml(String(node.size_bytes ?? 0))}">${formatSize(node.size_bytes)}</td>`;
        }
        if (column.type === "date") {
            return renderDateCell(sortValue(node, column.sortKey));
        }

        let value = "-";
        let cellClass = "meta-cell";
        if (column.key === "children") {
            value = node.direct_children ?? 0;
            cellClass = "meta-cell numeric-cell";
        } else if (column.key === "recursive_files") {
            value = node.recursive_files ?? 0;
            cellClass = "meta-cell numeric-cell";
        } else if (column.key === "mime") {
            value = node.mime_type || "-";
        } else if (column.key === "extension") {
            value = node.extension || "-";
        } else if (column.key === "relative_path") {
            value = displayPath(node.path || "-");
        } else if (column.key === "full_path") {
            value = displayPath(node.absolute_path || "-");
        } else if (column.key === "owner") {
            value = node.owner || "-";
        } else if (column.key === "group") {
            value = node.group || "-";
        } else if (column.key === "permissions") {
            value = node.permissions_text || node.permissions_octal || "-";
        }
        return `<td class="${cellClass}" title="${escapeHtml(String(value))}">${escapeHtml(value)}</td>`;
    }

    function virtualRowCacheKey(row) {
        return `${row.node.key}|${row.depth}|${row.ancestorOnly ? 1 : 0}|${visibleColumnKeys()}`;
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
        const { node, depth, ancestorOnly } = row;
        const hasChildren = node.entryType === "d" && node.children.length > 0;
        const expanded = state.expanded.has(node.key);
        const toggle = hasChildren
            ? `<button type="button" class="tree-toggle" data-toggle="${escapeHtml(node.key)}" aria-label="${expanded ? "Collapse" : "Expand"} ${escapeHtml(displayPath(node.name_path || node.path))}">${expanded ? "−" : "+"}</button>`
            : '<span class="tree-toggle placeholder">+</span>';
        const iconType = iconForNode(node);
        const rowClass = ["tree-row", ancestorOnly ? "ancestor-row" : "", node.key === state.selectedKey ? "selected-row" : ""]
            .filter(Boolean)
            .join(" ");
        const nameContent = `
            <button type="button" class="tree-name-button" data-select-path="${escapeHtml(node.key)}" title="${escapeHtml(displayPath(node.absolute_path || node.path || node.key))}">
                <span class="entry-icon entry-icon-${escapeHtml(iconType)}" aria-hidden="true"></span>
                <span class="entry-name">${escapeHtml(displayName(node))}</span>
                ${ancestorOnly ? '<span class="ancestor-pill">Ancestor context</span>' : ""}
            </button>
        `;

        return `
            <tr class="${rowClass}" data-entry-type="${escapeHtml(node.entryType)}" data-entry-key="${escapeHtml(node.key)}" data-virtual-index="${index}" aria-selected="${node.key === state.selectedKey ? "true" : "false"}">
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
    }

    function renderVirtualBody(forceMeasurement) {
        recalculateVirtualMetrics();
        const visibleColumns = visibleOptionalColumns();
        const colSpan = 1 + visibleColumns.length;

        if (!state.virtualRows.length) {
            elements.resultBody.innerHTML = `<tr><td colspan="${colSpan}"><div class="empty-state">No entries match the current browser filters.</div></td></tr>`;
            return;
        }

        const range = currentVirtualRange();
        const rangeKey = `${range.start}:${range.end}:${range.topOffset}:${range.bottomOffset}:${state.selectedKey || ""}`;
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

    function collectVisibleRows() {
        const hasFilter = Boolean(state.query.trim());
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
        state.virtualRows = rows;
        state.lastVirtualRangeKey = "";
        const selected = selectedNode();
        const selectedDirectory = selectedDirectoryNode();
        if (elements.resultCount) {
            elements.resultCount.textContent = `${rows.length} visible items · ${directMatchCount} direct matches`;
        }
        if (elements.folderCount) {
            elements.folderCount.textContent = `${report.directories.length} folders`;
        }
        if (elements.activeDirectory) {
            elements.activeDirectory.textContent = selectedDirectory
                ? `Selected: ${compactDisplayValue(selectedDirectory.path || selectedDirectory.name_path || "-", 2)}`
                : `${state.expanded.size} expanded folders`;
            elements.activeDirectory.title = selectedDirectory
                ? displayPath(selectedDirectory.path || selectedDirectory.name_path || "-")
                : `${state.expanded.size} expanded folders`;
        }
        if (elements.currentPath) {
            elements.currentPath.textContent = selected ? displayName(selected) : "Filesystem Tree";
        }
        updateSortDisplay();
        buildBreadcrumb();
        renderSelectionDetails();
        renderFilterStatus();

        if (!state.selectedKey && rows[0]) {
            setSelectedKey(rows[0].node.key);
        }

        renderVirtualBody(true);
    }

    function wireControls() {
        elements.searchInput.addEventListener("input", (event) => {
            const nextQuery = event.target.value;
            if (state.filterInputTimer) {
                window.clearTimeout(state.filterInputTimer);
            }
            state.filterSummary = nextQuery.trim() ? "Waiting for typing to pause..." : "Smart filter ready";
            state.filterError = "";
            renderFilterStatus();
            state.filterInputTimer = window.setTimeout(() => {
                state.filterInputTimer = null;
                applySmartFilterQuery(nextQuery);
            }, FILTER_INPUT_DEBOUNCE_MS);
        });

        if (elements.resetFilters) {
            elements.resetFilters.addEventListener("click", () => {
                if (state.filterInputTimer) {
                    window.clearTimeout(state.filterInputTimer);
                    state.filterInputTimer = null;
                }
                state.query = "";
                state.compiledFilter = (node) => Boolean(node.matched);
                state.filterSummary = "Smart filter ready";
                state.filterError = "";
                state.sortKey = "name_path";
                state.sortDirection = "asc";
                state.shownColumns = new Set(["type", "size", "modified"]);
                state.columnOrder = [...defaultColumnOrder];
                elements.searchInput.value = "";
                state.expanded = defaultExpandedPaths();
                renderColumnOptions();
                renderTable();
            });
        }

        if (elements.expandAll) {
            elements.expandAll.addEventListener("click", () => {
                state.expanded = new Set(report.directories.map((directory) => directory.path));
                renderTable();
            });
        }

        if (elements.collapseAll) {
            elements.collapseAll.addEventListener("click", () => {
                state.expanded = defaultExpandedPaths();
                renderTable();
            });
        }

        if (elements.columnPickerButton) {
            elements.columnPickerButton.addEventListener("click", () => {
                if (!elements.columnPickerMenu.hidden) {
                    closeColumnMenu();
                    return;
                }
                const rect = elements.columnPickerButton.getBoundingClientRect();
                openColumnMenu(rect.left, rect.bottom + 6);
            });
        }

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

        elements.treegridHeaderRow.addEventListener("dragstart", (event) => {
            const button = event.target.closest("button[data-column-key]");
            if (!button) {
                return;
            }
            state.draggingColumnKey = button.dataset.columnKey || null;
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
            elements.treegridHeaderRow.querySelectorAll(".optional-column-header").forEach((candidate) => {
                candidate.classList.toggle("drag-over", candidate === header);
            });
        });

        elements.treegridHeaderRow.addEventListener("drop", (event) => {
            const header = event.target.closest("th.optional-column-header");
            if (!header || !state.draggingColumnKey) {
                return;
            }
            event.preventDefault();
            moveColumnBefore(state.draggingColumnKey, header.dataset.columnKey || "");
            clearDragState();
            renderColumnOptions();
            renderTable();
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
            renderTable();
        });

        elements.resultBody.addEventListener("click", (event) => {
            const toggle = event.target.closest("[data-toggle]");
            if (toggle) {
                const key = toggle.dataset.toggle;
                if (!key) {
                    return;
                }
                setSelectedKey(key);
                if (state.expanded.has(key)) {
                    state.expanded.delete(key);
                } else {
                    state.expanded.add(key);
                }
                renderTable();
                return;
            }

            const selectTarget = event.target.closest("[data-select-path]");
            if (selectTarget) {
                const key = selectTarget.dataset.selectPath;
                if (key) {
                    setSelectedKey(key);
                    renderTable();
                }
                return;
            }

            const row = event.target.closest("tr[data-entry-key]");
            if (row?.dataset.entryKey) {
                setSelectedKey(row.dataset.entryKey);
                renderTable();
            }
        });

        elements.resultBody.addEventListener("dblclick", (event) => {
            const row = event.target.closest("tr[data-entry-key]");
            if (!row?.dataset.entryKey) {
                return;
            }
            const node = nodeMap.get(row.dataset.entryKey);
            if (!node || node.entryType !== "d") {
                return;
            }
            setSelectedKey(node.key);
            if (state.expanded.has(node.key)) {
                state.expanded.delete(node.key);
            } else {
                state.expanded.add(node.key);
            }
            renderTable();
        });

        elements.breadcrumb.addEventListener("click", (event) => {
            const button = event.target.closest("[data-select-path]");
            if (!button?.dataset.selectPath) {
                return;
            }
            setSelectedKey(button.dataset.selectPath);
            renderTable();
        });

        document.addEventListener("click", (event) => {
            if (!elements.columnPickerMenu.hidden && !event.target.closest("#columnPickerMenu") && !event.target.closest("#columnPickerButton")) {
                closeColumnMenu();
            }
        });

        document.addEventListener("keydown", (event) => {
            if (event.key === "Escape") {
                closeColumnMenu();
            }
        });
    }

    buildNodeTree();
    populateStaticSections();
    renderColumnOptions();
    wireControls();
    renderTable();
})();