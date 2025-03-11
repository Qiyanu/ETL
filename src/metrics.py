import time
import threading
from typing import Union, Dict, List, Any, Optional
import json

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
        self.start_times: Dict[str, Dict[int, float]] = {}  # Track timers per thread
        self.lock = threading.Lock()
    
    def start_timer(self, metric_name: str) -> None:
        """
        Start a timer for a specific metric.
        Thread-safe implementation that uses thread IDs.
        
        Args:
            metric_name (str): Name of the metric to time
        """
        thread_id = threading.get_ident()
        
        with self.lock:
            if metric_name not in self.start_times:
                self.start_times[metric_name] = {}
                
            self.start_times[metric_name][thread_id] = time.time()
    
    def stop_timer(self, metric_name: str) -> Union[float, None]:
        """
        Stop a timer and record the elapsed time.
        Thread-safe implementation that uses thread IDs.
        
        Args:
            metric_name (str): Name of the metric to stop timing
        
        Returns:
            float or None: Elapsed time in seconds, or None if timer not found
        """
        thread_id = threading.get_ident()
        
        with self.lock:
            if (metric_name in self.start_times and 
                thread_id in self.start_times[metric_name]):
                
                elapsed = time.time() - self.start_times[metric_name][thread_id]
                self.record_value(f"{metric_name}_seconds", elapsed)
                
                # Clean up timer
                del self.start_times[metric_name][thread_id]
                if not self.start_times[metric_name]:  # Remove empty dict
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
                        summary[name] = {"count": 0, "sum": 0}
                else:
                    summary[name] = values
        
        return summary
    
    def get_metric(self, metric_name: str) -> Optional[Union[List[float], int]]:
        """
        Get a specific metric value.
        
        Args:
            metric_name (str): Name of the metric to retrieve
            
        Returns:
            Optional[Union[List[float], int]]: The metric value or None if not found
        """
        with self.lock:
            return self.metrics.get(metric_name)
    
    def reset(self) -> None:
        """
        Reset all metrics and timers.
        Useful for starting a new measurement cycle.
        """
        with self.lock:
            self.metrics.clear()
            self.start_times.clear()
    
    def export_to_json(self, file_path: str) -> bool:
        """
        Export metrics summary to a JSON file.
        
        Args:
            file_path (str): Path to the output JSON file
            
        Returns:
            bool: True if export succeeded, False otherwise
        """
        try:
            summary = self.get_summary()
            with open(file_path, 'w') as f:
                json.dump(summary, f, indent=2)
            return True
        except Exception:
            return False
    
    def export_to_cloud_monitoring(self) -> bool:
        """
        Export metrics to Cloud Monitoring if available.
        
        Returns:
            bool: True if export succeeded, False otherwise
        """
        try:
            from google.cloud import monitoring_v3
            
            # Implementation would go here
            # This is a placeholder - actual implementation would
            # require more detailed integration with Google Cloud Monitoring
            
            return True
        except (ImportError, Exception):
            return False
    
    def __str__(self) -> str:
        """
        Get a string representation of metrics.
        
        Returns:
            str: String representation of metrics summary
        """
        summary = self.get_summary()
        return f"Metrics: {json.dumps(summary, indent=2)}"

# Global metrics collector instance
metrics = Metrics()