"""
Data Lineage Tracking for NYC TLC Data Platform

This module implements comprehensive data lineage tracking to understand
the flow of data through the pipeline.
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum
import logging
import os

logger = logging.getLogger(__name__)


class LineageEventType(Enum):
    """Types of lineage events"""
    DATASET_CREATED = "DATASET_CREATED"
    DATASET_READ = "DATASET_READ"
    DATASET_WRITTEN = "DATASET_WRITTEN"
    TRANSFORMATION_APPLIED = "TRANSFORMATION_APPLIED"
    DATASET_DELETED = "DATASET_DELETED"


class DataLineageTracker:
    """Tracks data lineage across the pipeline"""
    
    def __init__(self, lineage_storage_path: str = "/home/steodhiambo/nyc-tlc-data-platform/lineage"):
        self.lineage_storage_path = lineage_storage_path
        os.makedirs(lineage_storage_path, exist_ok=True)
        
        # In-memory cache for lineage data
        self.lineage_cache = {}
    
    def record_lineage_event(
        self, 
        event_type: LineageEventType, 
        dataset_name: str, 
        operation_details: Dict[str, Any],
        upstream_datasets: List[str] = None,
        downstream_datasets: List[str] = None,
        job_id: str = None,
        run_id: str = None
    ) -> str:
        """
        Record a lineage event
        
        Args:
            event_type: Type of lineage event
            dataset_name: Name of the dataset involved
            operation_details: Details about the operation performed
            upstream_datasets: List of upstream datasets
            downstream_datasets: List of downstream datasets
            job_id: ID of the job performing the operation
            run_id: ID of the run
            
        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        
        lineage_event = {
            "event_id": event_id,
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type.value,
            "dataset_name": dataset_name,
            "operation_details": operation_details,
            "upstream_datasets": upstream_datasets or [],
            "downstream_datasets": downstream_datasets or [],
            "job_id": job_id,
            "run_id": run_id
        }
        
        # Save lineage event to file
        self._save_lineage_event(lineage_event)
        
        # Update lineage cache
        self._update_lineage_cache(dataset_name, lineage_event)
        
        logger.info(f"Recorded lineage event: {event_type.value} for {dataset_name}")
        
        return event_id
    
    def _save_lineage_event(self, lineage_event: Dict[str, Any]):
        """Save lineage event to persistent storage"""
        # Create a file for the day to organize events
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        date_dir = os.path.join(self.lineage_storage_path, date_str)
        os.makedirs(date_dir, exist_ok=True)
        
        # Save event to JSON file
        event_file = os.path.join(date_dir, f"{lineage_event['event_id']}.json")
        with open(event_file, 'w') as f:
            json.dump(lineage_event, f, indent=2, default=str)
    
    def _update_lineage_cache(self, dataset_name: str, lineage_event: Dict[str, Any]):
        """Update the in-memory lineage cache"""
        if dataset_name not in self.lineage_cache:
            self.lineage_cache[dataset_name] = []
        
        self.lineage_cache[dataset_name].append(lineage_event)
    
    def get_lineage(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get lineage information for a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Lineage information
        """
        # Load from cache if available
        if dataset_name in self.lineage_cache:
            events = self.lineage_cache[dataset_name]
        else:
            # Load from persistent storage
            events = self._load_lineage_events(dataset_name)
            self.lineage_cache[dataset_name] = events
        
        # Organize events by type
        lineage_info = {
            "dataset_name": dataset_name,
            "events": events,
            "upstream_datasets": set(),
            "downstream_datasets": set(),
            "transformation_history": [],
            "access_history": []
        }
        
        for event in events:
            # Track upstream and downstream datasets
            lineage_info["upstream_datasets"].update(event.get("upstream_datasets", []))
            lineage_info["downstream_datasets"].update(event.get("downstream_datasets", []))
            
            # Track transformations
            if event["event_type"] == LineageEventType.TRANSFORMATION_APPLIED.value:
                lineage_info["transformation_history"].append({
                    "timestamp": event["timestamp"],
                    "operation": event["operation_details"],
                    "job_id": event.get("job_id")
                })
            
            # Track access patterns
            if event["event_type"] in [LineageEventType.DATASET_READ.value, LineageEventType.DATASET_WRITTEN.value]:
                lineage_info["access_history"].append({
                    "timestamp": event["timestamp"],
                    "event_type": event["event_type"],
                    "job_id": event.get("job_id"),
                    "run_id": event.get("run_id")
                })
        
        # Convert sets to lists for JSON serialization
        lineage_info["upstream_datasets"] = list(lineage_info["upstream_datasets"])
        lineage_info["downstream_datasets"] = list(lineage_info["downstream_datasets"])
        
        return lineage_info
    
    def _load_lineage_events(self, dataset_name: str) -> List[Dict[str, Any]]:
        """Load lineage events for a dataset from persistent storage"""
        events = []
        
        # Look for events in all date directories
        for date_dir in os.listdir(self.lineage_storage_path):
            date_path = os.path.join(self.lineage_storage_path, date_dir)
            if os.path.isdir(date_path):
                for event_file in os.listdir(date_path):
                    if event_file.endswith('.json'):
                        event_path = os.path.join(date_path, event_file)
                        try:
                            with open(event_path, 'r') as f:
                                event = json.load(f)
                                # Check if this event is for our dataset
                                if event.get('dataset_name') == dataset_name:
                                    events.append(event)
                        except Exception as e:
                            logger.error(f"Error loading lineage event from {event_path}: {e}")
        
        # Sort events by timestamp
        events.sort(key=lambda x: x.get('timestamp', ''))
        return events
    
    def get_impact_analysis(self, dataset_name: str) -> Dict[str, Any]:
        """
        Perform impact analysis for a dataset
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Impact analysis results
        """
        lineage = self.get_lineage(dataset_name)
        
        # Find all downstream datasets that would be affected
        affected_datasets = set()
        queue = lineage["downstream_datasets"][:]
        
        while queue:
            current_dataset = queue.pop(0)
            if current_dataset not in affected_datasets:
                affected_datasets.add(current_dataset)
                
                # Add further downstream datasets
                current_lineage = self.get_lineage(current_dataset)
                queue.extend(current_lineage["downstream_datasets"])
        
        impact_analysis = {
            "source_dataset": dataset_name,
            "direct_downstream": lineage["downstream_datasets"],
            "total_affected_datasets": len(affected_datasets),
            "all_affected_datasets": list(affected_datasets),
            "recent_access_count": len([a for a in lineage["access_history"] 
                                      if self._is_recent(a["timestamp"])])
        }
        
        return impact_analysis
    
    def _is_recent(self, timestamp: str, hours: int = 24) -> bool:
        """Check if a timestamp is within the last N hours"""
        from datetime import datetime, timedelta
        
        try:
            event_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            return event_time >= cutoff_time
        except:
            return False
    
    def get_source_analysis(self, dataset_name: str) -> Dict[str, Any]:
        """
        Perform source analysis for a dataset
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Source analysis results
        """
        lineage = self.get_lineage(dataset_name)
        
        # Find all upstream datasets that contribute to this dataset
        contributing_datasets = set()
        queue = lineage["upstream_datasets"][:]
        
        while queue:
            current_dataset = queue.pop(0)
            if current_dataset not in contributing_datasets:
                contributing_datasets.add(current_dataset)
                
                # Add further upstream datasets
                current_lineage = self.get_lineage(current_dataset)
                queue.extend(current_lineage["upstream_datasets"])
        
        source_analysis = {
            "target_dataset": dataset_name,
            "direct_upstream": lineage["upstream_datasets"],
            "total_contributing_datasets": len(contributing_datasets),
            "all_contributing_datasets": list(contributing_datasets),
            "transformation_steps": len(lineage["transformation_history"])
        }
        
        return source_analysis
    
    def export_lineage_graph(self, output_file: str = None) -> str:
        """
        Export lineage as a graph representation
        
        Args:
            output_file: Path to save the graph (optional)
            
        Returns:
            Graph representation as string
        """
        # Collect all unique datasets
        all_datasets = set()
        all_relationships = []
        
        # Scan all lineage files to build relationships
        for date_dir in os.listdir(self.lineage_storage_path):
            date_path = os.path.join(self.lineage_storage_path, date_dir)
            if os.path.isdir(date_path):
                for event_file in os.listdir(date_path):
                    if event_file.endswith('.json'):
                        event_path = os.path.join(date_path, event_file)
                        try:
                            with open(event_path, 'r') as f:
                                event = json.load(f)
                                
                                dataset = event.get('dataset_name')
                                upstream = event.get('upstream_datasets', [])
                                downstream = event.get('downstream_datasets', [])
                                
                                all_datasets.add(dataset)
                                all_datasets.update(upstream)
                                all_datasets.update(downstream)
                                
                                for upstream_ds in upstream:
                                    all_relationships.append((upstream_ds, dataset))
                                
                                for downstream_ds in downstream:
                                    all_relationships.append((dataset, downstream_ds))
                        except Exception as e:
                            logger.error(f"Error processing lineage event from {event_path}: {e}")
        
        # Create a DOT format graph
        graph_lines = ["digraph DataLineage {"]
        graph_lines.append("  rankdir=TB;")
        graph_lines.append("  node [shape=box];")
        
        # Add nodes
        for dataset in all_datasets:
            graph_lines.append(f'  "{dataset}";')
        
        # Add edges
        for source, target in set(all_relationships):  # Use set to remove duplicates
            if source != target:  # Avoid self-references
                graph_lines.append(f'  "{source}" -> "{target}";')
        
        graph_lines.append("}")
        
        graph_content = "\n".join(graph_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(graph_content)
            logger.info(f"Lineage graph exported to {output_file}")
        
        return graph_content


def create_lineage_decorator(tracker: DataLineageTracker):
    """
    Create a decorator to automatically track lineage for functions
    
    Args:
        tracker: DataLineageTracker instance
        
    Returns:
        Decorator function
    """
    def lineage_decorator(
        dataset_name: str,
        operation_type: str,
        upstream_datasets: List[str] = None,
        downstream_datasets: List[str] = None
    ):
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Record the start of the operation
                job_id = kwargs.get('job_id', str(uuid.uuid4()))
                run_id = kwargs.get('run_id', str(uuid.uuid4()))
                
                tracker.record_lineage_event(
                    event_type=LineageEventType.TRANSFORMATION_APPLIED,
                    dataset_name=dataset_name,
                    operation_details={
                        "operation": operation_type,
                        "function_name": func.__name__,
                        "args": str(args)[:200],  # Limit length
                        "kwargs": {k: str(v)[:200] for k, v in kwargs.items()}  # Limit length
                    },
                    upstream_datasets=upstream_datasets,
                    downstream_datasets=downstream_datasets,
                    job_id=job_id,
                    run_id=run_id
                )
                
                try:
                    # Execute the original function
                    result = func(*args, **kwargs)
                    
                    # Record successful completion
                    tracker.record_lineage_event(
                        event_type=LineageEventType.DATASET_WRITTEN,
                        dataset_name=dataset_name,
                        operation_details={
                            "operation": f"{operation_type}_completed",
                            "status": "success"
                        },
                        upstream_datasets=upstream_datasets,
                        downstream_datasets=downstream_datasets,
                        job_id=job_id,
                        run_id=run_id
                    )
                    
                    return result
                except Exception as e:
                    # Record failure
                    tracker.record_lineage_event(
                        event_type=LineageEventType.DATASET_WRITTEN,
                        dataset_name=dataset_name,
                        operation_details={
                            "operation": f"{operation_type}_failed",
                            "status": "failure",
                            "error": str(e)
                        },
                        upstream_datasets=upstream_datasets,
                        downstream_datasets=downstream_datasets,
                        job_id=job_id,
                        run_id=run_id
                    )
                    
                    raise  # Re-raise the exception
            
            return wrapper
        return decorator
    
    return lineage_decorator