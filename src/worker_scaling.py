import os
import time
import threading
import random
from datetime import datetime
from typing import Dict, Any, List, Optional

import psutil

from src.logging_utils import logger

class ContainerAwareResourceMonitor:
    """Monitor resources with awareness of containerized environments."""
    
    def __init__(self):
        """Initialize the resource monitor with container detection."""
        self.is_container = self._detect_container()
        logger.info(f"Resource monitor initialized: {'container' if self.is_container else 'standard'} environment detected")
        
    def _detect_container(self) -> bool:
        """Detect if running in a container environment."""
        # Check for Docker
        if os.path.exists('/.dockerenv'):
            return True
        
        # Check for Kubernetes
        if os.path.exists('/var/run/secrets/kubernetes.io'):
            return True
        
        # Check cgroups for container evidence
        try:
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                if 'docker' in content or 'kubepods' in content:
                    return True
        except (IOError, FileNotFoundError):
            pass
            
        return False
    
    def get_container_limits(self) -> Dict[str, Optional[float]]:
        """Get resource limits from container metadata if available."""
        cpu_limit = None
        memory_limit = None
        
        # Try to read memory limit from cgroups
        try:
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                memory_limit = int(f.read().strip())
                # Convert from bytes to MB
                memory_limit = memory_limit / (1024 * 1024)
        except (IOError, FileNotFoundError):
            pass
            
        # Try to read CPU quota and period from cgroups
        try:
            with open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'r') as f:
                cpu_quota = int(f.read().strip())
            with open('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'r') as f:
                cpu_period = int(f.read().strip())
                
            if cpu_quota > 0 and cpu_period > 0:
                cpu_limit = cpu_quota / cpu_period
        except (IOError, FileNotFoundError):
            pass
            
        return {
            'cpu_limit': cpu_limit,
            'memory_limit_mb': memory_limit
        }
    
    def get_memory_usage(self) -> float:
        """Get memory usage percentage, container-aware if possible."""
        if self.is_container:
            # Try container-specific approach first
            try:
                with open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r') as f:
                    usage = int(f.read().strip())
                with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                    limit = int(f.read().strip())
                    
                # Avoid division by zero or unrealistic limits (sometimes reported as very large numbers)
                if limit and limit < 2**60:  # Sanity check for the limit
                    return usage / limit
            except (IOError, FileNotFoundError, ValueError):
                pass
        
        # Fall back to psutil
        return psutil.virtual_memory().percent / 100.0
    
    def get_cpu_usage(self) -> float:
        """Get CPU usage percentage, container-aware if possible."""
        if self.is_container:
            # For CPU in containers, we'll need multiple samples
            try:
                # Read CPU stats at two points in time
                with open('/sys/fs/cgroup/cpu/cpuacct.usage', 'r') as f:
                    usage1 = int(f.read().strip())
                time.sleep(0.1)  # Brief delay
                with open('/sys/fs/cgroup/cpu/cpuacct.usage', 'r') as f:
                    usage2 = int(f.read().strip())
                
                # Calculate usage over the period
                cpu_delta = usage2 - usage1
                time_delta = 0.1 * 1e9  # Convert to nanoseconds
                
                # Get number of CPUs
                try:
                    with open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'r') as f:
                        quota = int(f.read().strip())
                    with open('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'r') as f:
                        period = int(f.read().strip())
                    num_cpus = quota / period if quota > 0 else os.cpu_count() or 1
                except (IOError, FileNotFoundError):
                    num_cpus = os.cpu_count() or 1
                
                # Calculate percentage
                return (cpu_delta / time_delta) / num_cpus
            except (IOError, FileNotFoundError, ValueError):
                pass
        
        # Fall back to psutil
        return psutil.cpu_percent(interval=None) / 100.0
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics, container-aware if possible."""
        cpu_usage = self.get_cpu_usage()
        memory_usage = self.get_memory_usage()
        
        # Get container limits if available
        limits = {}
        if self.is_container:
            limits = self.get_container_limits()
        
        # Memory information
        memory = psutil.virtual_memory()
        
        # Combine standard and container-specific metrics
        return {
            'cpu_percent': cpu_usage,
            'memory_percent': memory_usage,
            'memory_available_gb': memory.available / (1024**3),
            'memory_available_mb': memory.available / (1024**2),
            'is_container': self.is_container,
            'container_cpu_limit': limits.get('cpu_limit'),
            'container_memory_limit_mb': limits.get('memory_limit_mb'),
            'timestamp': datetime.now().isoformat(),
        }

class AdaptiveWorkerScaler:
    """
    Dynamically adjusts worker count based on system resource availability.
    Optimized for resource-intensive workloads with a memory-first scaling strategy.
    Container-aware for cloud environments.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the adaptive worker scaler.
        
        Args:
            config (dict): Configuration dictionary for worker scaling
        """
        # Worker count boundaries
        self.min_workers = config.get("MIN_WORKERS", 1)
        self.max_workers = config.get("MAX_WORKERS", 8)
        self.initial_workers = config.get("INITIAL_WORKERS", min(4, self.max_workers))
        
        # CPU utilization targets
        self.target_cpu_usage = config.get("TARGET_CPU_USAGE", 0.7)  # 70%
        self.cpu_scale_up_threshold = config.get("CPU_SCALE_UP_THRESHOLD", 0.8)  # 80%
        self.cpu_scale_down_threshold = config.get("CPU_SCALE_DOWN_THRESHOLD", 0.5)  # 50%
        
        # Memory utilization targets
        self.target_memory_usage = config.get("TARGET_MEMORY_USAGE", 0.6)  # 60%
        self.memory_scale_up_threshold = config.get("MEMORY_SCALE_UP_THRESHOLD", 0.75)  # 75%
        self.memory_scale_down_threshold = config.get("MEMORY_SCALE_DOWN_THRESHOLD", 0.45)  # 45%
        self.memory_limit_threshold = config.get("MEMORY_LIMIT_THRESHOLD", 0.85)  # 85%
        
        # Memory reservation per worker
        self.memory_per_worker_mb = config.get("MEMORY_PER_WORKER_MB", 512)  # 512MB per worker
        
        # Scaling behavior controls
        self.scaling_cooldown = config.get("SCALING_COOLDOWN_SECONDS", 60)  # 1 minute
        self.scaling_sensitivity = config.get("SCALING_SENSITIVITY", 2)  # Readings needed to confirm a trend
        
        # Internal state tracking
        self.current_workers = self.initial_workers
        self.last_scaling_time = 0
        self.lock = threading.Lock()
        self.worker_history: List[Dict[str, Any]] = []
        
        # Scaling trend trackers
        self.consecutive_high_cpu = 0
        self.consecutive_low_cpu = 0
        self.consecutive_high_memory = 0
        self.consecutive_low_memory = 0
        
        # Sampling configuration
        self.sample_count = config.get("METRIC_SAMPLE_COUNT", 3)
        self.sample_interval = config.get("METRIC_SAMPLE_INTERVAL_SECONDS", 1)
        
        # Initialize container-aware resource monitor
        self.resource_monitor = ContainerAwareResourceMonitor()
        
        # Log initialization
        container_status = "container-aware" if self.resource_monitor.is_container else "standard"
        logger.info(
            f"Adaptive worker scaling initialized ({container_status}). "
            f"Initial workers: {self.initial_workers}, "
            f"Min: {self.min_workers}, Max: {self.max_workers}, "
            f"Memory per worker: {self.memory_per_worker_mb}MB"
        )
    
    def get_worker_count(self) -> int:
        """
        Dynamically calculate optimal worker count based on system metrics.
        
        Returns:
            int: Recommended number of workers
        """
        # Get current system metrics
        metrics = self._get_system_metrics()
        
        with self.lock:
            # Check cooldown period first - FIXED: moved inside lock
            current_time = time.time()
            if current_time - self.last_scaling_time < self.scaling_cooldown:
                return self.current_workers
                
            # Extract metrics safely after lock acquisition
            cpu_usage = metrics['cpu_percent']
            memory_usage = metrics['memory_percent']
            memory_available_mb = metrics['memory_available_mb']
            
            previous_workers = self.current_workers
            
            # Calculate max workers based on available memory
            max_workers_by_memory = int(memory_available_mb / self.memory_per_worker_mb)
            max_workers_by_memory = max(self.min_workers, min(max_workers_by_memory, self.max_workers))
            
            # Use container CPU limit if available
            container_cpu_limit = metrics.get('container_cpu_limit')
            if container_cpu_limit and container_cpu_limit > 0:
                # Adjust max workers based on CPU limit
                cpu_based_max_workers = max(1, int(container_cpu_limit))
                self.max_workers = min(self.max_workers, cpu_based_max_workers)
                logger.debug(f"Adjusting max workers to {self.max_workers} based on container CPU limit")
            
            # Use container memory limit if available
            container_memory_limit_mb = metrics.get('container_memory_limit_mb')
            if container_memory_limit_mb and container_memory_limit_mb > 0:
                # Reserve 20% for system overhead
                usable_memory = container_memory_limit_mb * 0.8
                memory_based_max_workers = max(1, int(usable_memory / self.memory_per_worker_mb))
                self.max_workers = min(self.max_workers, memory_based_max_workers)
                logger.debug(f"Adjusting max workers to {self.max_workers} based on container memory limit")
            
            # Emergency scale down for critical memory pressure
            if memory_usage > self.memory_limit_threshold and self.current_workers > self.min_workers:
                safe_worker_count = max(self.min_workers, self.current_workers - 1)
                
                if memory_usage > 0.92:  # Critical memory usage
                    safe_worker_count = self.min_workers
                
                # Record and log the emergency reduction
                self._record_adjustment(
                    previous_workers, 
                    safe_worker_count, 
                    metrics, 
                    "EMERGENCY_MEMORY"
                )
                
                self.current_workers = safe_worker_count
                return self.current_workers
            
            # Memory-based scaling (primary factor)
            if memory_usage > self.memory_scale_up_threshold:
                self.consecutive_high_memory += 1
                self.consecutive_low_memory = 0
                
                if self.consecutive_high_memory >= self.scaling_sensitivity:
                    # Scale down to prevent memory exhaustion
                    new_worker_count = min(
                        previous_workers - 1, 
                        max_workers_by_memory
                    )
                    if new_worker_count < previous_workers and new_worker_count >= self.min_workers:
                        self._record_adjustment(
                            previous_workers, 
                            new_worker_count, 
                            metrics, 
                            "MEMORY_HIGH"
                        )
                        self.current_workers = new_worker_count
                        self.consecutive_high_memory = 0
            
            elif memory_usage < self.memory_scale_down_threshold:
                self.consecutive_low_memory += 1
                self.consecutive_high_memory = 0
                
                if self.consecutive_low_memory >= self.scaling_sensitivity:
                    # Scale up based on memory availability
                    new_worker_count = min(
                        previous_workers + 1,
                        max_workers_by_memory,
                        self.max_workers
                    )
                    if new_worker_count > previous_workers:
                        self._record_adjustment(
                            previous_workers, 
                            new_worker_count, 
                            metrics, 
                            "MEMORY_LOW"
                        )
                        self.current_workers = new_worker_count
                        self.consecutive_low_memory = 0
            
            # Secondary CPU-based scaling (only if memory is stable)
            elif (memory_usage >= self.memory_scale_down_threshold and 
                  memory_usage <= self.memory_scale_up_threshold):
                # CPU scaling logic
                if cpu_usage > self.cpu_scale_up_threshold and self.current_workers < self.max_workers:
                    self.consecutive_high_cpu += 1
                    self.consecutive_low_cpu = 0
                    
                    if self.consecutive_high_cpu >= self.scaling_sensitivity:
                        # Check if we have enough memory to add a worker
                        if self.current_workers < max_workers_by_memory:
                            new_worker_count = min(self.current_workers + 1, self.max_workers)
                            self._record_adjustment(
                                previous_workers, 
                                new_worker_count, 
                                metrics, 
                                "CPU_HIGH"
                            )
                            self.current_workers = new_worker_count
                        
                        self.consecutive_high_cpu = 0
                
                elif cpu_usage < self.cpu_scale_down_threshold and self.current_workers > self.min_workers:
                    self.consecutive_low_cpu += 1
                    self.consecutive_high_cpu = 0
                    
                    if self.consecutive_low_cpu >= self.scaling_sensitivity:
                        new_worker_count = max(self.min_workers, self.current_workers - 1)
                        self._record_adjustment(
                            previous_workers, 
                            new_worker_count, 
                            metrics, 
                            "CPU_LOW"
                        )
                        self.current_workers = new_worker_count
                        
                        self.consecutive_low_cpu = 0
                else:
                    # Reset counters when in normal range
                    if (cpu_usage <= self.cpu_scale_up_threshold and 
                        cpu_usage >= self.cpu_scale_down_threshold):
                        self.consecutive_high_cpu = 0
                        self.consecutive_low_cpu = 0
            
            # Periodically log memory status
            if random.random() < 0.1:  # ~10% probability to avoid log spam
                logger.info(
                    f"Resource status: "
                    f"Memory {memory_usage:.1%}, "
                    f"Available: {metrics['memory_available_gb']:.2f}GB, "
                    f"Max workers by memory: {max_workers_by_memory}, "
                    f"Current: {self.current_workers}"
                )
        
        return self.current_workers
    
    def _get_system_metrics(self) -> Dict[str, float]:
        """
        Collect system metrics with multiple samples for accuracy.
        Container-aware if running in a containerized environment.
        
        Returns:
            dict: System resource utilization metrics
        """
        # Take multiple samples
        metrics_samples = []
        
        for _ in range(self.sample_count):
            metrics_samples.append(self.resource_monitor.get_system_metrics())
            time.sleep(self.sample_interval)
        
        # Average the metrics
        cpu_samples = [m['cpu_percent'] for m in metrics_samples]
        memory_samples = [m['memory_percent'] for m in metrics_samples]
        
        # Combine with the most recent complete metrics
        final_metrics = metrics_samples[-1].copy()
        final_metrics['cpu_percent'] = sum(cpu_samples) / len(cpu_samples)
        final_metrics['memory_percent'] = sum(memory_samples) / len(memory_samples)
        
        return final_metrics
    
    def _record_adjustment(
        self, 
        previous_workers: int, 
        new_workers: int, 
        metrics: Dict[str, float], 
        reason: str
    ) -> None:
        """
        Record a worker count adjustment with detailed metrics.
        
        Args:
            previous_workers (int): Number of workers before adjustment
            new_workers (int): Number of workers after adjustment
            metrics (dict): System resource metrics
            reason (str): Reason for the scaling adjustment
        """
        # Update last scaling time
        self.last_scaling_time = time.time()
        
        # Create adjustment record
        adjustment = {
            'timestamp': datetime.now().isoformat(),
            'previous_workers': previous_workers,
            'new_workers': new_workers,
            'reason': reason,
            'cpu_usage': metrics['cpu_percent'],
            'memory_usage': metrics['memory_percent'],
            'memory_available_gb': metrics['memory_available_gb'],
        }
        
        # Add to adjustment history
        self.worker_history.append(adjustment)
        
        # Log memory allocation details
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
        logger.info(
            f"Worker count adjusted: {previous_workers} → {new_workers} ({reason}). "
            f"Memory allocation: "
            f"Total: {total_memory_gb:.2f}GB, "
            f"Per worker: {self.memory_per_worker_mb/1024:.2f}GB, "
            f"Total allocated: {(new_workers * self.memory_per_worker_mb)/1024:.2f}GB"
        )
    
    def get_adjustment_history(self) -> List[Dict[str, Any]]:
        """
        Retrieve the history of worker count adjustments.
        
        Returns:
            list: Adjustment history records
        """
        with self.lock:
            return self.worker_history.copy()

class AdaptiveSemaphore:
    """
    Thread-safe semaphore that can dynamically adjust its counter.
    Handles currently-in-use permits robustly.
    """
    
    def __init__(self, initial_value: int):
        """
        Initialize the adaptive semaphore.
        
        Args:
            initial_value (int): Initial number of permits
        """
        if initial_value < 0:
            raise ValueError("Semaphore value cannot be negative")
            
        self.current_value = initial_value  # Current configured value
        self.available_permits = initial_value  # Actually available permits
        self.lock = threading.Lock()
        self._real_semaphore = threading.Semaphore(initial_value)
        
        # Track pending decreases when permits are in use
        self.pending_decrease = 0
    
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a permit from the semaphore.
        
        Args:
            blocking (bool): Whether to block waiting for a permit
            timeout (float): Maximum time to wait for a permit
        
        Returns:
            bool: Whether a permit was acquired
        """
        acquired = False
        
        # First acquire our tracking lock to update internal state
        with self.lock:
            # Check if we're allowed to acquire based on current settings
            if self.available_permits <= 0:
                return False
                
            # Reserve the permit in our tracking
            self.available_permits -= 1
        
        # Now actually acquire the permit from the real semaphore
        try:
            if timeout is not None:
                acquired = self._real_semaphore.acquire(blocking=blocking, timeout=timeout)
            else:
                acquired = self._real_semaphore.acquire(blocking=blocking)
            
            if not acquired:
                # Failed to acquire, release our tracking reservation
                with self.lock:
                    self.available_permits += 1
        except:
            # On any error, release our tracking reservation
            with self.lock:
                self.available_permits += 1
            raise
            
        return acquired
    
    def release(self) -> None:
        """
        Release a permit back to the semaphore.
        Also processes any pending decreases.
        """
        with self.lock:
            # Release the real semaphore
            self._real_semaphore.release()
            self.available_permits += 1
            
            # Process any pending decrease
            if self.pending_decrease > 0:
                # We can now successfully reduce a permit
                self._real_semaphore.acquire(blocking=False)
                self.available_permits -= 1
                self.pending_decrease -= 1
                logger.debug(f"Processed pending permit decrease, {self.pending_decrease} remaining")
    
    def adjust_value(self, new_value: int) -> None:
        """
        Dynamically adjust the number of available permits.
        Handles adjustments gracefully even when permits are in use.
        
        Args:
            new_value (int): New number of permits
        """
        if new_value < 0:
            raise ValueError("Semaphore value cannot be negative")
            
        with self.lock:
            current_value = self.current_value
            
            if new_value > current_value:
                # Increase permits
                for _ in range(new_value - current_value):
                    self._real_semaphore.release()
                    self.available_permits += 1
                
                logger.info(f"Increased worker semaphore: {current_value} → {new_value}")
                
            elif new_value < current_value:
                # Decrease permits - try to acquire what we can immediately
                decrease_amount = current_value - new_value
                acquired_count = 0
                pending_count = 0
                
                for _ in range(decrease_amount):
                    if self.available_permits > 0:
                        # We have unused permits that can be removed immediately
                        if self._real_semaphore.acquire(blocking=False):
                            self.available_permits -= 1
                            acquired_count += 1
                        else:
                            # This is unexpected - our tracking doesn't match reality
                            # Log the inconsistency but continue
                            logger.warning("Semaphore state inconsistency detected")
                    else:
                        # Record as pending - will be removed when permits are released
                        self.pending_decrease += 1
                        pending_count += 1
                
                if acquired_count > 0:
                    logger.info(f"Decreased worker semaphore by {acquired_count} immediately")
                    
                if pending_count > 0:
                    logger.info(f"Scheduled {pending_count} permits for future decrease")
            
            # Update tracking value
            self.current_value = new_value
    
    @property
    def active_permits(self) -> int:
        """
        Get the number of currently acquired permits.
        
        Returns:
            int: Number of permits in use
        """
        with self.lock:
            return self.current_value - self.available_permits