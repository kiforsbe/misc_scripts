import logging
import threading
import time


class ResourceMonitor:
    """Monitors system resources used by the DLNA server"""
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            'cpu_usage': [],
            'memory_usage': [],
            'network_stats': {
                'bytes_sent': 0,
                'bytes_received': 0,
                'connections': 0
            },
            'active_streams': 0,
            'cache_stats': {
                'thumbnail_hits': 0,
                'thumbnail_misses': 0,
                'metadata_hits': 0,
                'metadata_misses': 0
            }
        }
        self.last_cpu_check = time.time()
        self.last_metric_log = time.time()
        self._start_monitoring()

    def _start_monitoring(self):
        """Start resource monitoring in background thread with optimized sampling"""
        try:
            import psutil
            self.process = psutil.Process()
            
            def monitor_loop():
                while True:
                    try:
                        current_time = time.time()
                        
                        # Only sample CPU every 5 seconds to reduce overhead
                        if current_time - self.last_cpu_check >= 5:
                            cpu_percent = self.process.cpu_percent(interval=0.1)  # Reduced interval
                            self.metrics['cpu_usage'].append(cpu_percent)
                            if len(self.metrics['cpu_usage']) > 12:  # Keep last minute
                                self.metrics['cpu_usage'].pop(0)
                            self.last_cpu_check = current_time

                        # Sample memory usage
                        memory_info = self.process.memory_info()
                        memory_mb = memory_info.rss / (1024 * 1024)
                        self.metrics['memory_usage'].append(memory_mb)
                        if len(self.metrics['memory_usage']) > 12:
                            self.metrics['memory_usage'].pop(0)
                        
                        # Log metrics every minute
                        if current_time - self.last_metric_log >= 60:
                            avg_cpu = sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0
                            avg_mem = sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0
                            
                            self.logger.info(
                                f"Resource Usage - CPU: {avg_cpu:.1f}%, Memory: {avg_mem:.1f}MB, "
                                f"Streams: {self.metrics['active_streams']}, "
                                f"Cache Hits: {self.metrics['cache_stats']['thumbnail_hits'] + self.metrics['cache_stats']['metadata_hits']}"
                            )
                            self.last_metric_log = current_time
                        
                        time.sleep(5)  # Reduced polling frequency
                        
                    except Exception as e:
                        self.logger.error(f"Error in resource monitoring: {e}")
                        time.sleep(5)

            threading.Thread(target=monitor_loop, daemon=True, name="ResourceMonitor").start()
            self.logger.info("Resource monitoring started")
            
        except ImportError:
            self.logger.warning("psutil not installed, resource monitoring disabled")

    def track_stream(self, started=True):
        """Track active media streams"""
        if started:
            self.metrics['active_streams'] += 1
        else:
            self.metrics['active_streams'] = max(0, self.metrics['active_streams'] - 1)

    def track_network(self, bytes_sent=0, bytes_received=0):
        """Track network usage"""
        self.metrics['network_stats']['bytes_sent'] += bytes_sent
        self.metrics['network_stats']['bytes_received'] += bytes_received

    def track_cache(self, cache_type, hit=True):
        """Track cache hits/misses"""
        if hit:
            self.metrics['cache_stats'][f'{cache_type}_hits'] += 1
        else:
            self.metrics['cache_stats'][f'{cache_type}_misses'] += 1
