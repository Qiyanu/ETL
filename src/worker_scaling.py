import time
import threading
import random
from datetime import datetime
from typing import Dict, Any, List

import psutil

from src.logging_utils import logger

class AdaptiveWorkerScaler:
    """
    Dynamically adjusts worker count based on system resource availability.
    Optimized for resource-intensive workloads with a memory-first scaling strategy.
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
        
        logger.info(
            f"Adaptive worker scaling initialized. "
            f"Initial workers: {self.initial_workers}, "
            f"Min: {self.min_workers}, Max: {self.max_workers}, "
            f"Memory per worker: {self.memory_per_worker_mb}MB"
        )
    
    def _get_system_metrics(self) -> Dict[str, float]:
        """
        Collect system metrics with multiple samples for accuracy.
        
        Returns:
            dict: System resource utilization metrics
        """
        # Take multiple samples
        cpu_samples = []
        memory_samples = []
        
        for _ in range(self.sample_count):
            cpu_samples.append(psutil.cpu_percent(interval=None) / 100.0)
            memory = psutil.virtual_memory()
            memory_samples.append(memory.percent / 100.0)
            time.sleep(self.sample_interval)
        
        # Memory information
        memory = psutil.virtual_memory()
        
        # Compile metrics
        return {
            'cpu_percent': sum(cpu_samples) / len(cpu_samples),
            'memory_percent': sum(memory_samples) / len(memory_samples),
            'memory_available_gb': memory.available / (1024**3),
            'memory_available_mb': memory.available / (1024**2),
            'timestamp': datetime.now().isoformat(),
        }
    
    def get_worker_count(self) -> int:
        """
        Dynamically calculate optimal worker count based on system metrics.
        
        Returns:
            int: Recommended number of workers
        """
        # Get current system metrics
        metrics = self._get_system_metrics()
        cpu_usage = metrics['cpu_percent']
        memory_usage = metrics['memory_percent']
        memory_available_mb = metrics['memory_available_mb']
        
        # Check cooldown period
        current_time = time.time()
        if current_time - self.last_scaling_time < self.scaling_cooldown:
            return self.current_workers
        
        with self.lock:
            previous_workers = self.current_workers
            
            # Calculate max workers based on available memory
            max_workers_by_memory = int(memory_available_mb / self.memory_per_worker_mb)
            max_workers_by_memory = max(self.min_workers, min(max_workers_by_memory, self.max_workers))
            
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
    """
    
    def __init__(self, initial_value: int):
        """
        Initialize the adaptive semaphore.
        
        Args:
            initial_value (int): Initial number of permits
        """
        self.value = initial_value
        self.lock = threading.Lock()
        self._real_semaphore = threading.Semaphore(initial_value)
    
    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """
        Acquire a permit from the semaphore.
        
        Args:
            blocking (bool): Whether to block waiting for a permit
            timeout (float): Maximum time to wait for a permit
        
        Returns:
            bool: Whether a permit was acquired
        """
        return self._real_semaphore.acquire(blocking, timeout)
    
    def release(self) -> None:
        """Release a permit back to the semaphore."""
        return self._real_semaphore.release()
    
    def adjust_value(self, new_value: int) -> None:
        """
        Dynamically adjust the number of available permits.
        
        Args:
            new_value (int): New number of permits
        """
        with self.lock:
            current_value = self.value
            
            if new_value > current_value:
                # Increase permits
                for _ in range(new_value - current_value):
                    self.release()
                logger.info(f"Increased worker semaphore: {current_value} → {new_value}")
            elif new_value < current_value:
                # Decrease permits
                acquired_count = 0
                for _ in range(current_value - new_value):
                    # Non-blocking acquire to avoid deadlock
                    if self._real_semaphore.acquire(blocking=False):
                        acquired_count += 1
                
                if acquired_count > 0:
                    logger.info(
                        f"Decreased worker semaphore by {acquired_count} "
                        f"(target: {current_value} → {new_value})"
                    )
                
                # Update tracking value
                self.value = current_value - acquired_count
                return
            
            # Update tracking value
            self.value = new_value
    
    @property
    def current_value(self) -> int:
        """
        Get the current number of available permits.
        
        Returns:
            int: Current number of permits
        """
        with self.lock:
            return self.value