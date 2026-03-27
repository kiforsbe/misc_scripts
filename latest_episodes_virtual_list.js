(function (globalScope) {
    class VirtualList {
        constructor(options) {
            if (!options || !options.container || typeof options.renderItem !== 'function') {
                throw new Error('VirtualList requires a container and a renderItem callback.');
            }

            this.container = options.container;
            this.renderItem = options.renderItem;
            this.getItemKey = typeof options.getItemKey === 'function' ? options.getItemKey : ((item, index) => index);
            this.getItemEstimatedHeight = typeof options.getItemEstimatedHeight === 'function'
                ? options.getItemEstimatedHeight
                : (() => 96);
            this.overscan = Number.isFinite(options.overscan) ? Math.max(1, options.overscan) : 6;
            this.items = [];
            this.offsets = [];
            this.heights = [];
            this.totalHeight = 0;
            this.heightCache = new Map();
            this.lastRangeKey = '';
            this.renderScheduled = false;
            this.forceRenderScheduled = false;

            this.handleScroll = this.handleScroll.bind(this);
            this.handleResize = this.handleResize.bind(this);

            this.createStructure();
            this.attachEvents();
        }

        createStructure() {
            this.container.innerHTML = '';

            this.topSpacer = document.createElement('div');
            this.topSpacer.className = 'virtual-list-top-spacer';

            this.content = document.createElement('div');
            this.content.className = 'virtual-list-content';

            this.bottomSpacer = document.createElement('div');
            this.bottomSpacer.className = 'virtual-list-bottom-spacer';

            this.container.appendChild(this.topSpacer);
            this.container.appendChild(this.content);
            this.container.appendChild(this.bottomSpacer);
        }

        attachEvents() {
            this.container.addEventListener('scroll', this.handleScroll, { passive: true });
            window.addEventListener('resize', this.handleResize);
        }

        destroy() {
            this.container.removeEventListener('scroll', this.handleScroll);
            window.removeEventListener('resize', this.handleResize);
        }

        setItems(items) {
            this.items = Array.isArray(items) ? items.slice() : [];
            this.pruneHeightCache();
            this.recalculateMetrics();
            this.render(true);
        }

        rerender() {
            this.scheduleRender(true);
        }

        scrollToIndex(index, options = {}) {
            if (!Number.isInteger(index) || index < 0 || index >= this.items.length) {
                return;
            }

            this.recalculateMetrics();

            const itemTop = this.offsets[index] || 0;
            const itemHeight = this.heights[index] || this.getEstimatedHeight(this.items[index], index);
            const viewportHeight = this.container.clientHeight || 0;
            const align = options.align || 'start';
            let targetTop = itemTop;

            if (align === 'center') {
                targetTop = itemTop - (viewportHeight / 2) + (itemHeight / 2);
            } else if (align === 'end') {
                targetTop = itemTop - viewportHeight + itemHeight;
            } else if (align === 'nearest') {
                const currentTop = this.container.scrollTop;
                const currentBottom = currentTop + viewportHeight;
                const itemBottom = itemTop + itemHeight;

                if (itemTop < currentTop) {
                    targetTop = itemTop;
                } else if (itemBottom > currentBottom) {
                    targetTop = itemBottom - viewportHeight;
                } else {
                    targetTop = currentTop;
                }
            }

            this.container.scrollTo({
                top: Math.max(0, targetTop),
                behavior: options.behavior || 'auto'
            });

            this.scheduleRender(true);
        }

        handleScroll() {
            this.scheduleRender(false);
        }

        handleResize() {
            this.scheduleRender(true);
        }

        scheduleRender(force) {
            if (force) {
                this.forceRenderScheduled = true;
            }

            if (this.renderScheduled) {
                return;
            }

            this.renderScheduled = true;
            requestAnimationFrame(() => {
                const shouldForce = this.forceRenderScheduled;
                this.renderScheduled = false;
                this.forceRenderScheduled = false;
                this.render(shouldForce);
            });
        }

        pruneHeightCache() {
            const nextKeys = new Set(this.items.map((item, index) => this.getItemKey(item, index)));
            Array.from(this.heightCache.keys()).forEach((key) => {
                if (!nextKeys.has(key)) {
                    this.heightCache.delete(key);
                }
            });
        }

        getEstimatedHeight(item, index) {
            const estimatedHeight = Number(this.getItemEstimatedHeight(item, index));
            return Number.isFinite(estimatedHeight) && estimatedHeight > 0 ? estimatedHeight : 96;
        }

        recalculateMetrics() {
            const offsets = new Array(this.items.length);
            const heights = new Array(this.items.length);
            let runningOffset = 0;

            for (let index = 0; index < this.items.length; index++) {
                const item = this.items[index];
                const key = this.getItemKey(item, index);
                const height = this.heightCache.get(key) || this.getEstimatedHeight(item, index);

                offsets[index] = runningOffset;
                heights[index] = height;
                runningOffset += height;
            }

            this.offsets = offsets;
            this.heights = heights;
            this.totalHeight = runningOffset;
        }

        findStartIndex(scrollTop) {
            let low = 0;
            let high = this.offsets.length - 1;
            let result = 0;

            while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                const itemTop = this.offsets[mid];
                const itemBottom = itemTop + this.heights[mid];

                if (itemBottom >= scrollTop) {
                    result = mid;
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }

            return result;
        }

        getVisibleRange() {
            if (!this.items.length) {
                return { start: 0, end: -1, topOffset: 0, bottomOffset: 0 };
            }

            const scrollTop = this.container.scrollTop || 0;
            const viewportHeight = this.container.clientHeight || 0;
            const viewportBottom = scrollTop + viewportHeight;
            const start = Math.max(0, this.findStartIndex(scrollTop) - this.overscan);
            let end = start;

            while (end < this.items.length && this.offsets[end] < viewportBottom) {
                end += 1;
            }

            end = Math.min(this.items.length - 1, end + this.overscan);

            const topOffset = this.offsets[start] || 0;
            const renderedHeight = end >= start
                ? (this.offsets[end] + this.heights[end]) - topOffset
                : 0;
            const bottomOffset = Math.max(0, this.totalHeight - topOffset - renderedHeight);

            return { start, end, topOffset, bottomOffset };
        }

        render(force) {
            this.recalculateMetrics();

            if (!this.items.length) {
                this.lastRangeKey = 'empty';
                this.topSpacer.style.height = '0px';
                this.bottomSpacer.style.height = '0px';
                this.content.innerHTML = '';
                return;
            }

            const range = this.getVisibleRange();
            const rangeKey = `${range.start}:${range.end}:${range.topOffset}:${range.bottomOffset}`;

            if (!force && rangeKey === this.lastRangeKey) {
                return;
            }

            this.lastRangeKey = rangeKey;
            this.topSpacer.style.height = `${range.topOffset}px`;
            this.bottomSpacer.style.height = `${range.bottomOffset}px`;

            let html = '';
            for (let index = range.start; index <= range.end; index++) {
                html += `<div class="virtual-list-row" data-virtual-index="${index}">${this.renderItem(this.items[index], index)}</div>`;
            }

            this.content.innerHTML = html;
            this.measureVisibleItems();
        }

        measureVisibleItems() {
            const rows = this.content.querySelectorAll('.virtual-list-row');
            let hasChanged = false;

            rows.forEach((row) => {
                const index = Number(row.getAttribute('data-virtual-index'));
                if (!Number.isInteger(index) || index < 0 || index >= this.items.length) {
                    return;
                }

                const item = this.items[index];
                const key = this.getItemKey(item, index);
                const measuredHeight = Math.ceil(row.getBoundingClientRect().height);
                if (!measuredHeight) {
                    return;
                }

                const cachedHeight = this.heightCache.get(key);
                if (cachedHeight !== measuredHeight) {
                    this.heightCache.set(key, measuredHeight);
                    hasChanged = true;
                }
            });

            if (hasChanged) {
                this.scheduleRender(true);
            }
        }
    }

    globalScope.VirtualList = VirtualList;
})(typeof window !== 'undefined' ? window : globalThis);