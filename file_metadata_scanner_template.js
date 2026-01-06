// File Metadata Explorer - JavaScript

class FileMetadataExplorer {
    constructor(metadata) {
        this.metadata = metadata;
        this.currentPath = null;
        this.selectedNode = null;
        this.sortColumn = 'name';
        this.sortDirection = 'asc';
        this.showHidden = false;
        this.showExtended = false;
        this.showThumbnails = true;
        this.searchTerm = '';
        this.isSearching = false;
        this.searchTimeout = null;
        
        // Hover preview
        this.currentPreviewItem = null;
        
        // Context menu
        this.contextMenuTarget = null;
        
        // Build folder structure
        this.rootFolder = this.buildFolderStructure();
        
        // Initialize
        this.init();
    }
    
    init() {
        this.renderHeader();
        this.renderFolderTree();
        this.setupEventListeners();
        
        // Setup browser history navigation
        window.addEventListener('popstate', (e) => {
            if (e.state && e.state.path) {
                this.selectFolder(e.state.path, false);
            }
        });
        
        // Initialize from URL hash or select root folder
        const hash = window.location.hash.substring(1);
        const initialPath = hash ? decodeURIComponent(hash) : (this.rootFolder ? this.rootFolder.path : null);
        
        if (initialPath) {
            this.selectFolder(initialPath, true);
        }
    }
    
    buildFolderStructure() {
        const folders = this.metadata.filter(item => item.type === 'directory');
        
        if (folders.length === 0) return null;
        
        // Find root folder (the one scanned)
        const rootFolder = folders[0];
        
        // Build tree structure
        const folderMap = {};
        folders.forEach(folder => {
            folderMap[folder.path] = {
                ...folder,
                children: [],
                files: []
            };
        });
        
        // Add files to their parent folders
        this.metadata.filter(item => item.type === 'file').forEach(file => {
            const parentPath = this.getParentPath(file.path);
            if (folderMap[parentPath]) {
                folderMap[parentPath].files.push(file);
            }
        });
        
        // Build parent-child relationships
        folders.forEach(folder => {
            const parentPath = this.getParentPath(folder.path);
            if (parentPath && folderMap[parentPath] && folder.path !== rootFolder.path) {
                folderMap[parentPath].children.push(folderMap[folder.path]);
            }
        });
        
        return folderMap[rootFolder.path];
    }
    
    getParentPath(path) {
        // Handle both Windows and Unix paths
        const separator = path.includes('\\') ? '\\' : '/';
        const parts = path.split(separator).filter(p => p !== '');
        
        // If only one part (e.g., 'I:' from 'I:\'), there's no parent
        if (parts.length <= 1) {
            return '';
        }
        
        parts.pop();
        const parentPath = parts.join(separator);
        
        // For Windows, if we only have a drive letter left, add the backslash
        if (separator === '\\' && parentPath.match(/^[A-Z]:$/i)) {
            return parentPath + '\\';
        }
        
        return parentPath || '';
    }
    
    renderHeader() {
        const rootPath = this.rootFolder ? this.rootFolder.path : 'Unknown';
        const scanDate = this.metadata.length > 0 ? new Date(this.metadata[0].created_time).toLocaleString() : 'Unknown';
        
        document.getElementById('rootPath').textContent = `Root: ${rootPath}`;
        document.getElementById('scanDate').textContent = `Scanned: ${new Date().toLocaleString()}`;
        
        const folderCount = this.metadata.filter(item => item.type === 'directory').length;
        document.getElementById('folderCount').textContent = `${folderCount} folders`;
    }
    
    renderFolderTree() {
        const treeContainer = document.getElementById('folderTree');
        treeContainer.innerHTML = '';
        
        if (!this.rootFolder) {
            treeContainer.innerHTML = '<div class="empty-state"><div class="empty-state-text">No folders found</div></div>';
            return;
        }
        
        const rootNode = this.createTreeNode(this.rootFolder, true);
        treeContainer.appendChild(rootNode);
    }
    
    createTreeNode(folder, isRoot = false) {
        const node = document.createElement('div');
        node.className = 'tree-node';
        node.dataset.path = folder.path;
        
        const item = document.createElement('div');
        item.className = 'tree-item';
        if (this.currentPath === folder.path) {
            item.classList.add('selected');
        }
        
        // Toggle for children
        const toggle = document.createElement('span');
        toggle.className = 'tree-toggle';
        if (folder.children.length > 0) {
            toggle.textContent = isRoot ? '▼' : '▶';
            toggle.onclick = (e) => {
                e.stopPropagation();
                this.toggleFolder(node);
            };
        }
        item.appendChild(toggle);
        
        // Folder icon
        const icon = document.createElement('span');
        icon.className = 'tree-icon';
        item.appendChild(icon);
        
        // Folder name
        const name = document.createElement('span');
        name.textContent = folder.name || 'Root';
        item.appendChild(name);
        
        item.onclick = () => this.selectFolder(folder.path);
        
        node.appendChild(item);
        
        // Children
        if (folder.children.length > 0) {
            const children = document.createElement('div');
            children.className = 'tree-children';
            if (!isRoot) {
                children.classList.add('collapsed');
            }
            
            folder.children
                .sort((a, b) => a.name.localeCompare(b.name))
                .forEach(child => {
                    children.appendChild(this.createTreeNode(child));
                });
            
            node.appendChild(children);
        }
        
        return node;
    }
    
    toggleFolder(node) {
        const children = node.querySelector('.tree-children');
        const toggle = node.querySelector('.tree-toggle');
        
        if (children) {
            children.classList.toggle('collapsed');
            toggle.textContent = children.classList.contains('collapsed') ? '▶' : '▼';
        }
    }
    
    selectFolder(path, addToHistory = true) {
        this.currentPath = path;
        
        // Hide hover preview when navigating
        this.hideHoverPreview();
        
        // Add to browser history
        if (addToHistory) {
            const url = new URL(window.location);
            url.hash = encodeURIComponent(path);
            window.history.pushState({ path: path }, '', url);
        }
        
        // Update selected state in tree
        document.querySelectorAll('.tree-item').forEach(item => {
            item.classList.remove('selected');
        });
        const selectedNode = document.querySelector(`[data-path="${path}"] .tree-item`);
        if (selectedNode) {
            selectedNode.classList.add('selected');
            selectedNode.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
        
        // Update Up button state
        this.updateUpButton();
        
        // Render file list
        this.renderFileList();
        this.renderBreadcrumb();
    }
    
    navigateUp() {
        if (!this.currentPath) return;
        
        const parentPath = this.getParentPath(this.currentPath);
        
        // Check if we have a parent folder
        const parentFolder = this.findFolder(parentPath);
        if (parentFolder) {
            this.selectFolder(parentPath);
        } else if (this.rootFolder && this.currentPath !== this.rootFolder.path) {
            // If parent not found but we're not at root, go to root
            this.selectFolder(this.rootFolder.path);
        }
    }
    
    updateUpButton() {
        const upBtn = document.getElementById('upBtn');
        // Disable if at root
        const isAtRoot = this.rootFolder && this.currentPath === this.rootFolder.path;
        upBtn.disabled = isAtRoot;
    }
    
    renderBreadcrumb() {
        const breadcrumb = document.getElementById('breadcrumb');
        breadcrumb.innerHTML = '';
        
        if (!this.currentPath) return;
        
        const separator = this.currentPath.includes('\\') ? '\\' : '/';
        const parts = this.currentPath.split(separator).filter(p => p);
        
        let currentPath = '';
        parts.forEach((part, index) => {
            if (index > 0) {
                const sep = document.createElement('span');
                sep.className = 'breadcrumb-separator';
                sep.textContent = separator;
                breadcrumb.appendChild(sep);
            }
            
            currentPath += (index > 0 ? separator : '') + part;
            const item = document.createElement('a');
            item.className = 'breadcrumb-item';
            item.textContent = part;
            item.dataset.path = currentPath;
            item.onclick = () => this.selectFolder(item.dataset.path);
            breadcrumb.appendChild(item);
        });
    }
    
    renderFileList() {
        const tbody = document.getElementById('fileListBody');
        tbody.innerHTML = '';
        
        // Find current folder data
        const folder = this.findFolder(this.currentPath);
        if (!folder) {
            this.showEmptyState();
            return;
        }
        
        let items;
        let totalFolders, totalFiles;
        
        // Check if searching
        if (this.searchTerm && this.searchTerm.length > 0) {
            // Recursive search
            this.isSearching = true;
            items = this.searchRecursive(folder, this.searchTerm);
            totalFolders = items.filter(i => i.type === 'directory').length;
            totalFiles = items.filter(i => i.type === 'file').length;
            
            document.getElementById('currentPath').textContent = `Search results in "${folder.name || 'Root'}"`;
        } else {
            // Normal folder view
            this.isSearching = false;
            items = [
                ...folder.children.map(f => ({ ...f, type: 'directory' })),
                ...folder.files
            ];
            totalFolders = folder.children.length;
            totalFiles = folder.files.length;
            
            document.getElementById('currentPath').textContent = folder.name || 'Root';
        }
        
        // Apply filters (hidden files, etc.)
        items = this.filterItems(items);
        
        // Update count
        document.getElementById('itemCount').textContent = 
            `${items.length} items (${totalFolders} folders, ${totalFiles} files)`;
        
        if (items.length === 0) {
            this.showEmptyState();
            return;
        }
        
        // Sort items
        items = this.sortItems(items);
        
        // Render rows
        items.forEach(item => {
            const row = this.createFileRow(item);
            tbody.appendChild(row);
        });
    }
    
    filterItems(items) {
        let filtered = items;
        
        // Filter hidden
        if (!this.showHidden) {
            filtered = filtered.filter(item => !item.is_hidden);
        }
        
        return filtered;
    }
    
    searchRecursive(folder, searchTerm) {
        const term = searchTerm.toLowerCase();
        let results = [];
        
        // Search in current folder's files
        folder.files.forEach(file => {
            if (file.name.toLowerCase().includes(term) || file.path.toLowerCase().includes(term)) {
                results.push(file);
            }
        });
        
        // Search in subfolders
        folder.children.forEach(child => {
            // Check if folder name matches
            if (child.name.toLowerCase().includes(term) || child.path.toLowerCase().includes(term)) {
                results.push({ ...child, type: 'directory' });
            }
            
            // Recursively search in child folders
            results = results.concat(this.searchRecursive(child, term));
        });
        
        return results;
    }
    
    sortItems(items) {
        const direction = this.sortDirection === 'asc' ? 1 : -1;
        
        return items.sort((a, b) => {
            // Directories first
            if (a.type === 'directory' && b.type !== 'directory') return -1;
            if (a.type !== 'directory' && b.type === 'directory') return 1;
            
            // Then sort by column
            let aVal = a[this.sortColumn];
            let bVal = b[this.sortColumn];
            
            if (this.sortColumn === 'size') {
                aVal = a.size || 0;
                bVal = b.size || 0;
                return (aVal - bVal) * direction;
            }
            
            if (typeof aVal === 'string') {
                return aVal.localeCompare(bVal) * direction;
            }
            
            return (aVal > bVal ? 1 : -1) * direction;
        });
    }
    
    createFileRow(item) {
        const row = document.createElement('tr');
        row.dataset.path = item.path;
        row.onclick = () => {
            if (item.type === 'directory') {
                this.selectFolder(item.path);
            } else {
                this.showFileDetails(item);
            }
        };
        
        // Add context menu for right-click
        row.oncontextmenu = (e) => {
            this.showContextMenu(item, e);
        };
        
        // Add hover preview for files
        if (item.type === 'file') {
            row.onmouseenter = (e) => this.showHoverPreview(item, e);
            row.onmouseleave = () => this.hideHoverPreview();
            row.onmousemove = (e) => {
                // Update position as mouse moves
                this.showHoverPreview(item, e);
            };
        }
        
        // Name column
        const nameCell = document.createElement('td');
        const nameDiv = document.createElement('div');
        nameDiv.className = 'file-name-cell';
        
        const icon = document.createElement('span');
        icon.className = `file-icon ${this.getFileIconClass(item)}`;
        nameDiv.appendChild(icon);
        
        // Add thumbnail if available
        if (this.showThumbnails && item.static_thumbnail) {
            const thumb = document.createElement('img');
            thumb.className = 'file-thumbnail';
            thumb.src = THUMBNAIL_PATH + item.static_thumbnail;
            thumb.alt = item.name;
            thumb.onerror = () => thumb.style.display = 'none';
            nameDiv.appendChild(thumb);
        }
        
        // Show name with path breadcrumbs if searching
        if (this.isSearching) {
            const pathContainer = document.createElement('div');
            pathContainer.className = 'file-path-container';
            
            // Create breadcrumb path
            const relativePath = this.getRelativePath(item.path, this.currentPath);
            if (relativePath) {
                const breadcrumbDiv = this.createPathBreadcrumbs(item.path, relativePath);
                pathContainer.appendChild(breadcrumbDiv);
            }
            
            // File/folder name
            const nameSpan = document.createElement('span');
            nameSpan.className = 'file-name';
            nameSpan.textContent = item.name;
            pathContainer.appendChild(nameSpan);
            
            nameDiv.appendChild(pathContainer);
        } else {
            const nameSpan = document.createElement('span');
            nameSpan.textContent = item.name;
            nameDiv.appendChild(nameSpan);
        }
        
        nameCell.appendChild(nameDiv);
        row.appendChild(nameCell);
        
        // Type column
        const typeCell = document.createElement('td');
        typeCell.textContent = item.type === 'directory' ? 'Folder' : (item.extension || 'File');
        row.appendChild(typeCell);
        
        // Size column
        const sizeCell = document.createElement('td');
        sizeCell.textContent = item.size_human || '';
        row.appendChild(sizeCell);
        
        // Modified column
        const modifiedCell = document.createElement('td');
        modifiedCell.textContent = this.formatDate(item.modified_time);
        row.appendChild(modifiedCell);
        
        // Created column
        const createdCell = document.createElement('td');
        createdCell.textContent = this.formatDate(item.created_time);
        row.appendChild(createdCell);
        
        // Attributes column
        const attrCell = document.createElement('td');
        const attrs = [];
        if (item.is_hidden) attrs.push('<span class="attribute-badge hidden">H</span>');
        if (item.is_readonly) attrs.push('<span class="attribute-badge readonly">R</span>');
        if (item.is_system) attrs.push('<span class="attribute-badge system">S</span>');
        attrCell.innerHTML = attrs.join(' ');
        row.appendChild(attrCell);
        
        return row;
    }
    
    getFileIconClass(item) {
        if (item.type === 'directory') return 'directory';
        
        const ext = item.extension ? item.extension.toLowerCase() : '';
        
        const videoExts = ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'];
        const audioExts = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'];
        const imageExts = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'];
        
        if (videoExts.includes(ext)) return 'video';
        if (audioExts.includes(ext)) return 'audio';
        if (imageExts.includes(ext)) return 'image';
        
        return 'file';
    }
    
    formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            const date = new Date(dateStr);
            return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch {
            return dateStr;
        }
    }
    
    findFolder(path) {
        const find = (folder) => {
            if (folder.path === path) return folder;
            for (const child of folder.children) {
                const found = find(child);
                if (found) return found;
            }
            return null;
        };
        return find(this.rootFolder);
    }
    
    getRelativePath(itemPath, basePath) {
        if (itemPath === basePath) return '';
        
        // Get the separator used in the paths
        const separator = itemPath.includes('\\') ? '\\' : '/';
        
        // Normalize paths for comparison
        const normalizedItem = itemPath.replace(/[\\\/]/g, separator);
        const normalizedBase = basePath.replace(/[\\\/]/g, separator);
        
        if (!normalizedItem.startsWith(normalizedBase)) {
            return itemPath; // Item is not under base path
        }
        
        // Get relative portion
        let relative = normalizedItem.substring(normalizedBase.length);
        
        // Remove leading separator
        if (relative.startsWith(separator)) {
            relative = relative.substring(separator.length);
        }
        
        // Remove the item's own name from the path
        const parts = relative.split(separator);
        if (parts.length > 1) {
            parts.pop(); // Remove last part (the item name itself)
            return parts.join(separator);
        }
        
        return '';
    }
    
    createPathBreadcrumbs(fullPath, relativePath) {
        const breadcrumbDiv = document.createElement('div');
        breadcrumbDiv.className = 'path-breadcrumbs';
        
        if (!relativePath) return breadcrumbDiv;
        
        const separator = relativePath.includes('\\') ? '\\' : '/';
        const parts = relativePath.split(separator).filter(p => p);
        
        // Build up the path incrementally
        const currentFolder = this.findFolder(this.currentPath);
        if (!currentFolder) return breadcrumbDiv;
        
        let accumulatedPath = this.currentPath;
        const pathSeparator = accumulatedPath.includes('\\') ? '\\' : '/';
        
        parts.forEach((part, index) => {
            if (index > 0) {
                const sep = document.createElement('span');
                sep.className = 'breadcrumb-sep';
                sep.textContent = separator;
                breadcrumbDiv.appendChild(sep);
            }
            
            accumulatedPath += pathSeparator + part;
            
            const crumb = document.createElement('a');
            crumb.className = 'breadcrumb-link';
            crumb.textContent = part;
            crumb.dataset.path = accumulatedPath;
            crumb.onclick = (e) => {
                e.stopPropagation();
                this.selectFolder(crumb.dataset.path);
            };
            breadcrumbDiv.appendChild(crumb);
        });
        
        // Add final separator
        if (parts.length > 0) {
            const sep = document.createElement('span');
            sep.className = 'breadcrumb-sep';
            sep.textContent = separator;
            breadcrumbDiv.appendChild(sep);
        }
        
        return breadcrumbDiv;
    }
    
    showEmptyState() {
        const tbody = document.getElementById('fileListBody');
        tbody.innerHTML = `
            <tr>
                <td colspan="6">
                    <div class="empty-state">
                        <div class="empty-state-icon">📭</div>
                        <div class="empty-state-text">No items to display</div>
                    </div>
                </td>
            </tr>
        `;
    }
    
    showHoverPreview(item, event) {
        const preview = document.getElementById('hoverPreview');
        const content = document.getElementById('hoverPreviewContent');
        
        // Update content if item changed
        if (this.currentPreviewItem !== item) {
            this.updatePreviewContent(item, content);
            this.currentPreviewItem = item;
        }
        
        // Position and show immediately
        this.positionPreview(preview, event);
        preview.classList.add('show');
    }
    
    updatePreviewContent(item, content) {
        let html = '';
        
        // Show thumbnail if available (prefer animated)
        if (item.animated_thumbnail || item.static_thumbnail) {
            const thumb = item.animated_thumbnail || item.static_thumbnail;
            html += `<img src="${THUMBNAIL_PATH}${thumb}" class="preview-thumbnail" alt="${item.name}">`;
        }
        
        html += '<div class="preview-info">';
        html += `<div class="preview-name">${item.name}</div>`;
        html += '<div class="preview-details">';
        html += `<span class="preview-label">Type:</span><span class="preview-value">${item.type === 'directory' ? 'Folder' : item.extension || 'File'}</span>`;
        html += `<span class="preview-label">Size:</span><span class="preview-value">${item.size_human}</span>`;
        html += `<span class="preview-label">Modified:</span><span class="preview-value">${this.formatDate(item.modified_time)}</span>`;
        
        // Add extended metadata if available
        if (item.extended_metadata && !item.extended_metadata.error) {
            if (item.extended_metadata.duration_human) {
                html += `<span class="preview-label">Duration:</span><span class="preview-value">${item.extended_metadata.duration_human}</span>`;
            }
            if (item.extended_metadata.video_width && item.extended_metadata.video_height) {
                html += `<span class="preview-label">Resolution:</span><span class="preview-value">${item.extended_metadata.video_width}x${item.extended_metadata.video_height}</span>`;
            }
            if (item.extended_metadata.video_codec) {
                html += `<span class="preview-label">Codec:</span><span class="preview-value">${item.extended_metadata.video_codec}</span>`;
            }
        }
        
        html += '</div></div>';
        
        content.innerHTML = html;
    }
    
    positionPreview(preview, event) {
        // Position popup near mouse
        const x = event.clientX + 15;
        const y = event.clientY + 15;
        
        // Ensure popup stays within viewport
        preview.style.left = x + 'px';
        preview.style.top = y + 'px';
        
        // Adjust if off-screen (need to wait a frame for content to render)
        requestAnimationFrame(() => {
            const rect = preview.getBoundingClientRect();
            if (rect.right > window.innerWidth) {
                preview.style.left = (x - rect.width - 30) + 'px';
            }
            if (rect.bottom > window.innerHeight) {
                preview.style.top = (y - rect.height - 30) + 'px';
            }
        });
    }
    
    hideHoverPreview() {
        const preview = document.getElementById('hoverPreview');
        preview.classList.remove('show');
        this.currentPreviewItem = null;
    }
    
    showContextMenu(item, event) {
        event.preventDefault();
        
        const menu = document.getElementById('contextMenu');
        this.contextMenuTarget = item;
        
        // Position menu at mouse
        menu.style.left = event.pageX + 'px';
        menu.style.top = event.pageY + 'px';
        menu.classList.add('show');
        
        // Adjust if off-screen
        setTimeout(() => {
            const rect = menu.getBoundingClientRect();
            if (rect.right > window.innerWidth) {
                menu.style.left = (event.pageX - rect.width) + 'px';
            }
            if (rect.bottom > window.innerHeight) {
                menu.style.top = (event.pageY - rect.height) + 'px';
            }
        }, 0);
    }
    
    hideContextMenu() {
        const menu = document.getElementById('contextMenu');
        menu.classList.remove('show');
        this.contextMenuTarget = null;
    }
    
    handleContextMenuAction(action) {
        if (!this.contextMenuTarget) return;
        
        const item = this.contextMenuTarget;
        
        switch (action) {
            case 'copy-name':
                this.copyToClipboard(item.name);
                break;
            case 'copy-path':
                this.copyToClipboard(item.path);
                break;
            case 'copy-size':
                this.copyToClipboard(item.size_human || item.size.toString());
                break;
            case 'copy-metadata':
                this.copyToClipboard(JSON.stringify(item, null, 2));
                break;
            case 'show-details':
                this.showFileDetails(item);
                break;
        }
        
        this.hideContextMenu();
    }
    
    copyToClipboard(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => {
                console.log('Copied to clipboard:', text);
            }).catch(err => {
                console.error('Failed to copy:', err);
                this.fallbackCopyToClipboard(text);
            });
        } else {
            this.fallbackCopyToClipboard(text);
        }
    }
    
    fallbackCopyToClipboard(text) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        
        try {
            document.execCommand('copy');
            console.log('Copied to clipboard (fallback):', text);
        } catch (err) {
            console.error('Fallback copy failed:', err);
        }
        
        document.body.removeChild(textArea);
    }
    
    showFileDetails(item) {
        const modal = document.getElementById('detailsModal');
        const modalBody = document.getElementById('modalBody');
        
        let html = '<div class="detail-section">';
        html += '<h3>General Information</h3>';
        html += '<div class="detail-grid">';
        html += `<div class="detail-label">Name:</div><div class="detail-value">${item.name}</div>`;
        html += `<div class="detail-label">Path:</div><div class="detail-value">${item.path}</div>`;
        html += `<div class="detail-label">Type:</div><div class="detail-value">${item.type}</div>`;
        html += `<div class="detail-label">Extension:</div><div class="detail-value">${item.extension || 'N/A'}</div>`;
        html += `<div class="detail-label">Size:</div><div class="detail-value">${item.size_human} (${item.size.toLocaleString()} bytes)</div>`;
        html += `<div class="detail-label">Created:</div><div class="detail-value">${this.formatDate(item.created_time)}</div>`;
        html += `<div class="detail-label">Modified:</div><div class="detail-value">${this.formatDate(item.modified_time)}</div>`;
        html += `<div class="detail-label">Accessed:</div><div class="detail-value">${this.formatDate(item.accessed_time)}</div>`;
        html += '</div></div>';
        
        // Attributes
        html += '<div class="detail-section">';
        html += '<h3>Attributes</h3>';
        html += '<div class="detail-grid">';
        html += `<div class="detail-label">Hidden:</div><div class="detail-value">${item.is_hidden ? 'Yes' : 'No'}</div>`;
        html += `<div class="detail-label">Read-only:</div><div class="detail-value">${item.is_readonly ? 'Yes' : 'No'}</div>`;
        html += `<div class="detail-label">System:</div><div class="detail-value">${item.is_system ? 'Yes' : 'No'}</div>`;
        html += '</div></div>';
        
        // Thumbnails
        if (item.static_thumbnail || item.animated_thumbnail) {
            html += '<div class="detail-section">';
            html += '<h3>Thumbnail</h3>';
            
            // Prefer animated thumbnail, fallback to static
            if (item.animated_thumbnail && item.static_thumbnail) {
                // Use static as default, swap to animated on hover
                html += '<div class="thumbnail-wrapper">';
                html += `<img src="${THUMBNAIL_PATH}${item.static_thumbnail}" 
                             class="thumbnail-preview thumbnail-hoverable" 
                             data-static="${THUMBNAIL_PATH}${item.static_thumbnail}"
                             data-animated="${THUMBNAIL_PATH}${item.animated_thumbnail}"
                             alt="Thumbnail">`;
                html += '<div class="thumbnail-play-overlay"><div class="play-icon"></div></div>';
                html += '</div>';
            } else if (item.animated_thumbnail) {
                html += `<img src="${THUMBNAIL_PATH}${item.animated_thumbnail}" class="thumbnail-preview" alt="Animated thumbnail">`;
            } else if (item.static_thumbnail) {
                html += `<img src="${THUMBNAIL_PATH}${item.static_thumbnail}" class="thumbnail-preview" alt="Static thumbnail">`;
            }
            
            html += '</div>';
        }
        
        // Extended metadata
        if (item.extended_metadata && Object.keys(item.extended_metadata).length > 0) {
            html += '<div class="detail-section">';
            html += '<h3>Extended Metadata</h3>';
            html += '<div class="detail-grid">';
            for (const [key, value] of Object.entries(item.extended_metadata)) {
                if (typeof value === 'object') {
                    html += `<div class="detail-label">${key}:</div><div class="detail-value">${JSON.stringify(value, null, 2)}</div>`;
                } else {
                    html += `<div class="detail-label">${key}:</div><div class="detail-value">${value}</div>`;
                }
            }
            html += '</div></div>';
        }
        
        modalBody.innerHTML = html;
        modal.classList.add('show');
        
        // Setup hover listeners for animated thumbnails
        const hoverableThumbnails = modalBody.querySelectorAll('.thumbnail-hoverable');
        hoverableThumbnails.forEach(img => {
            const staticSrc = img.dataset.static;
            const animatedSrc = img.dataset.animated;
            const overlay = img.parentElement.querySelector('.thumbnail-play-overlay');
            
            img.addEventListener('mouseenter', () => {
                img.src = animatedSrc;
                if (overlay) overlay.style.opacity = '0';
            });
            
            img.addEventListener('mouseleave', () => {
                img.src = staticSrc;
                if (overlay) overlay.style.opacity = '1';
            });
        });
    }
    
    setupEventListeners() {
        // Navigation buttons - use browser's back/forward
        document.getElementById('backBtn').onclick = () => window.history.back();
        document.getElementById('forwardBtn').onclick = () => window.history.forward();
        document.getElementById('upBtn').onclick = () => this.navigateUp();
        
        // Keyboard navigation
        document.addEventListener('keydown', (e) => {
            if (e.altKey && e.key === 'ArrowLeft') {
                e.preventDefault();
                window.history.back();
            } else if (e.altKey && e.key === 'ArrowRight') {
                e.preventDefault();
                window.history.forward();
            } else if (e.altKey && e.key === 'ArrowUp') {
                e.preventDefault();
                this.navigateUp();
            }
        });
        
        // Resizable sidebar
        const resizeHandle = document.getElementById('resizeHandle');
        const sidebar = document.getElementById('sidebar');
        let isResizing = false;
        
        resizeHandle.addEventListener('mousedown', (e) => {
            isResizing = true;
            resizeHandle.classList.add('resizing');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        });
        
        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;
            
            const newWidth = e.clientX;
            if (newWidth >= 200 && newWidth <= 600) {
                sidebar.style.width = newWidth + 'px';
            }
        });
        
        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                resizeHandle.classList.remove('resizing');
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });
        
        // Sort columns
        document.querySelectorAll('.sortable').forEach(th => {
            th.onclick = () => {
                const column = th.dataset.column;
                if (this.sortColumn === column) {
                    this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    this.sortColumn = column;
                    this.sortDirection = 'asc';
                }
                
                // Update UI
                document.querySelectorAll('.sortable').forEach(h => {
                    h.classList.remove('sort-asc', 'sort-desc');
                });
                th.classList.add(`sort-${this.sortDirection}`);
                
                this.renderFileList();
            };
        });
        
        // View options
        document.getElementById('showHidden').onchange = (e) => {
            this.showHidden = e.target.checked;
            this.renderFileList();
        };
        
        document.getElementById('showExtended').onchange = (e) => {
            this.showExtended = e.target.checked;
            this.renderFileList();
        };
        
        document.getElementById('showThumbnails').onchange = (e) => {
            this.showThumbnails = e.target.checked;
            this.renderFileList();
        };
        
        // Search
        const searchInput = document.getElementById('searchInput');
        searchInput.oninput = () => {
            // Clear previous timeout
            if (this.searchTimeout) {
                clearTimeout(this.searchTimeout);
            }
            
            // Set new timeout to delay search
            this.searchTimeout = setTimeout(() => {
                this.searchTerm = searchInput.value;
                this.renderFileList();
            }, 750); // 750ms delay
        };
        
        document.getElementById('clearSearch').onclick = () => {
            // Clear any pending search
            if (this.searchTimeout) {
                clearTimeout(this.searchTimeout);
            }
            
            searchInput.value = '';
            this.searchTerm = '';
            this.renderFileList();
        };
        
        // Modal
        const modal = document.getElementById('detailsModal');
        const closeBtn = modal.querySelector('.close');
        
        closeBtn.onclick = () => {
            modal.classList.remove('show');
        };
        
        window.onclick = (e) => {
            if (e.target === modal) {
                modal.classList.remove('show');
            }
        };
        
        // Get file list container for scroll handlers
        const fileListContainer = document.querySelector('.file-list-container');
        
        // Hide preview and context menu on scroll
        if (fileListContainer) {
            fileListContainer.addEventListener('scroll', () => {
                this.hideHoverPreview();
                this.hideContextMenu();
            });
        }
        
        document.addEventListener('click', () => {
            this.hideHoverPreview();
        });
        
        // Context menu
        const contextMenu = document.getElementById('contextMenu');
        
        // Handle menu item clicks
        contextMenu.addEventListener('click', (e) => {
            const item = e.target.closest('.context-menu-item');
            if (item) {
                const action = item.dataset.action;
                this.handleContextMenuAction(action);
            }
        });
        
        // Hide context menu on click outside
        document.addEventListener('click', (e) => {
            if (!contextMenu.contains(e.target)) {
                this.hideContextMenu();
            }
        });
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const explorer = new FileMetadataExplorer(METADATA);
});
