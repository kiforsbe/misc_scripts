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
        
        // Build folder structure
        this.rootFolder = this.buildFolderStructure();
        
        // Initialize
        this.init();
    }
    
    init() {
        this.renderHeader();
        this.renderFolderTree();
        this.setupEventListeners();
        
        // Select root folder by default
        if (this.rootFolder) {
            this.selectFolder(this.rootFolder.path);
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
        const parts = path.split(separator);
        parts.pop();
        return parts.join(separator);
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
    
    selectFolder(path) {
        this.currentPath = path;
        
        // Update selected state in tree
        document.querySelectorAll('.tree-item').forEach(item => {
            item.classList.remove('selected');
        });
        const selectedNode = document.querySelector(`[data-path="${path}"] .tree-item`);
        if (selectedNode) {
            selectedNode.classList.add('selected');
            selectedNode.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
        
        // Render file list
        this.renderFileList();
        this.renderBreadcrumb();
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
        
        document.getElementById('currentPath').textContent = folder.name || 'Root';
        
        // Get items (subfolders + files)
        let items = [
            ...folder.children.map(f => ({ ...f, type: 'directory' })),
            ...folder.files
        ];
        
        // Apply filters
        items = this.filterItems(items);
        
        // Update count
        document.getElementById('itemCount').textContent = 
            `${items.length} items (${folder.children.length} folders, ${folder.files.length} files)`;
        
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
        
        // Filter by search term
        if (this.searchTerm) {
            const term = this.searchTerm.toLowerCase();
            filtered = filtered.filter(item => 
                item.name.toLowerCase().includes(term) ||
                item.path.toLowerCase().includes(term)
            );
        }
        
        return filtered;
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
        
        const nameSpan = document.createElement('span');
        nameSpan.textContent = item.name;
        nameDiv.appendChild(nameSpan);
        
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
            html += '<h3>Thumbnails</h3>';
            if (item.static_thumbnail) {
                html += `<img src="${THUMBNAIL_PATH}${item.static_thumbnail}" class="thumbnail-preview" alt="Static thumbnail">`;
            }
            if (item.animated_thumbnail) {
                html += `<img src="${THUMBNAIL_PATH}${item.animated_thumbnail}" class="thumbnail-preview" alt="Animated thumbnail">`;
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
    }
    
    setupEventListeners() {
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
            this.searchTerm = searchInput.value;
            this.renderFileList();
        };
        
        document.getElementById('clearSearch').onclick = () => {
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
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const explorer = new FileMetadataExplorer(METADATA);
});
