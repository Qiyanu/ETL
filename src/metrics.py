import time
import threading
from typing import Union, Dict, List, Any

class Metrics:
    """
    A lightweight, thread-safe metrics collection class.
    
    Provides functionality to:
    - Start and stop timers
    - Record numeric values
    - Increment counters
    - Generate summary statistics
    """
    
    def __init__(self):
        """
        Initialize the metrics collector.
        
        Uses a thread lock to ensure thread-safe operations.
        """
        self.metrics: Dict[str, Union[List[float], int]] = {}
        self.start_times: Dict[str, float] = {}
        self.lock = threading.Lock()
    
    def start_timer(self, metric_name: str) -> None:
        """
        Start a timer for a specific metric.
        
        Args:
            metric_name (str): Name of the metric to time
        """
        self.start_times[metric_name] = time.time()
    
    def stop_timer(self, metric_name: str) -> Union[float, None]:
        """
        Stop a timer and record the elapsed time.
        
        Args:
            metric_name (str): Name of the metric to stop timing
        
        Returns:
            float or None: Elapsed time in seconds, or None if timer not found
        """
        if metric_name in self.start_times:
            elapsed = time.time() - self.start_times[metric_name]
            self.record_value(f"{metric_name}_seconds", elapsed)
            del self.start_times[metric_name]
            return elapsed
        return None
    
    def record_value(self, metric_name: str, value: float) -> None:
        """
        Record a numeric value for a specific metric.
        
        Args:
            metric_name (str): Name of the metric
            value (float): Numeric value to record
        """
        with self.lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []
            
            if isinstance(self.metrics[metric_name], list):
                self.metrics[metric_name].append(value)
    
    def increment_counter(self, metric_name: str, increment: int = 1) -> None:
        """
        Increment a counter metric.
        
        Args:
            metric_name (str): Name of the counter metric
            increment (int, optional): Amount to increment. Defaults to 1.
        """
        with self.lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = 0
            
            if isinstance(self.metrics[metric_name], int):
                self.metrics[metric_name] += increment
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of all collected metrics.
        
        Returns:
            Dict containing summary statistics for list-based metrics,
            and current values for counter metrics.
        """
        summary: Dict[str, Any] = {}
        
        with self.lock:
            for name, values in self.metrics.items():
                if isinstance(values, list):
                    if values:  # Check if list is not empty
                        summary[name] = {
                            "count": len(values),
                            "sum": sum(values),
                            "avg": sum(values) / len(values),
                            "min": min(values),
                            "max": max(values)
                        }
                    else:
                        summary[name] = None
                else:
                    summary[name] = values
        
        return summary
    
    def reset(self) -> None:
        """
        Reset all metrics and timers.
        Useful for starting a new measurement cycle.
        """
        with self.lock:
            self.metrics.clear()
            self.start_times.clear()

# Global metrics collector instance
metrics = Metrics()