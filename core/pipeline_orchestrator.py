"""
New pipeline orchestrator using Transform chain pattern.
"""

import asyncio
import logging
import yaml
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Callable

from .interfaces import Transform
from .plugin_loader import get as load_transform_class
from .infra.scheduler import Scheduler

logger = logging.getLogger(__name__)


async def _drain(stages: List[Transform]) -> None:
    """Execute a pipeline by connecting transform stages."""
    
    async def seed() -> AsyncIterator[None]:
        """Seed the pipeline with a single None value."""
        yield None
    
    # Start with the seed iterator
    stream: AsyncIterator[Any] = seed()
    
    # Use AsyncExitStack to properly manage context managers
    async with AsyncExitStack() as stack:
        # Enter all stages that support async context management
        for stage in stages:
            if hasattr(stage, "__aenter__"):
                await stack.enter_async_context(stage)
        
        # Chain all stages together
        for stage in stages:
            stream = stage(stream)
        
        # Drain the final stream to execute the pipeline
        async for item in stream:
            # The sinks should handle items, so we just consume the stream
            pass


async def run_pipeline(cfg: Dict[str, Any]) -> None:
    """Run a single pipeline from configuration."""
    pipeline_name = cfg.get("name", "unnamed")
    
    try:
        logger.info(f"Starting pipeline: {pipeline_name}")
        
        # Create instances of all stages
        instances: List[Transform] = []
        for entry in cfg["chain"]:
            cls = load_transform_class(entry["class"])
            kwargs = entry.get("kwargs", {})
            instances.append(cls(**kwargs))
        
        # Execute the pipeline
        await _drain(instances)
        
        logger.info(f"Pipeline completed: {pipeline_name}")
        
    except Exception as e:
        logger.error(f"Pipeline {pipeline_name} failed: {e}", exc_info=True)


async def run_pipeline_with_schedule(cfg: Dict[str, Any]) -> None:
    """Run a pipeline with optional scheduling."""
    # For now, just run once. TODO: Add cron scheduling
    await run_pipeline(cfg)


# Global registry for scheduled pipeline configs
_scheduled_pipeline_configs = {}


def _store_pipeline_config(job_id: str, cfg: Dict[str, Any]) -> None:
    """Store pipeline config for scheduled execution."""
    _scheduled_pipeline_configs[job_id] = cfg


async def _execute_scheduled_pipeline(job_id: str) -> None:
    """Execute a scheduled pipeline by job_id. Used by APScheduler."""
    if job_id in _scheduled_pipeline_configs:
        cfg = _scheduled_pipeline_configs[job_id]
        await run_pipeline(cfg)
    else:
        logger.error(f"Pipeline config not found for job_id: {job_id}")


async def create_pipeline_runner(cfg: Dict[str, Any]) -> str:
    """Create a serializable reference for a pipeline. Returns function path for scheduling."""
    job_id = f"pipeline_{cfg.get('name', 'unnamed')}"
    _store_pipeline_config(job_id, cfg)
    return f"core.pipeline_orchestrator:_execute_scheduled_pipeline"


async def run_all_with_scheduler(pipelines_cfg: List[Dict[str, Any]], scheduler: Optional[Scheduler] = None) -> None:
    """Run all pipelines with scheduler support for cron/interval jobs."""
    if scheduler is None:
        # If no scheduler provided, fall back to run_all
        await run_all(pipelines_cfg)
        return
    
    tasks = []
    
    for pipeline_cfg in pipelines_cfg:
        pipeline_name = pipeline_cfg.get("name", "unnamed")
        
        # Check for scheduling configuration
        schedule_cfg = pipeline_cfg.get("schedule")
        
        if schedule_cfg:
            # Create a pipeline runner
            runner = await create_pipeline_runner(pipeline_cfg)
            job_id = f"pipeline_{pipeline_name}"
            
            if "cron" in schedule_cfg:
                # Schedule with cron expression
                cron_expr = schedule_cfg["cron"]
                scheduler.add_cron_job(runner, cron_expression=cron_expr, job_id=job_id)
                logger.info(f"Scheduled pipeline '{pipeline_name}' with cron: {cron_expr}")
                
            elif "interval" in schedule_cfg:
                # Schedule with interval
                interval_cfg = schedule_cfg["interval"]
                scheduler.add_interval_job(
                    runner,
                    seconds=interval_cfg.get("seconds"),
                    minutes=interval_cfg.get("minutes"),
                    hours=interval_cfg.get("hours"),
                    job_id=job_id
                )
                logger.info(f"Scheduled pipeline '{pipeline_name}' with interval: {interval_cfg}")
                
            else:
                logger.warning(f"Pipeline '{pipeline_name}' has 'schedule' config but no 'cron' or 'interval' specified")
                # Run once as fallback
                task = asyncio.create_task(
                    run_pipeline(pipeline_cfg),
                    name=f"pipeline-{pipeline_name}"
                )
                tasks.append(task)
        else:
            # No scheduling, run once
            task = asyncio.create_task(
                run_pipeline(pipeline_cfg),
                name=f"pipeline-{pipeline_name}"
            )
            tasks.append(task)
    
    # Wait for any immediate tasks to complete
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_all(pipelines_cfg: List[Dict[str, Any]]) -> None:
    """Run all pipelines concurrently."""
    tasks = []
    
    for pipeline_cfg in pipelines_cfg:
        # Check if pipeline has scheduling
        if "schedule" in pipeline_cfg:
            # TODO: Implement proper cron scheduling
            # For now, just run once
            task = asyncio.create_task(
                run_pipeline_with_schedule(pipeline_cfg),
                name=f"pipeline-{pipeline_cfg.get('name', 'unnamed')}"
            )
        else:
            task = asyncio.create_task(
                run_pipeline(pipeline_cfg),
                name=f"pipeline-{pipeline_cfg.get('name', 'unnamed')}"
            )
        
        tasks.append(task)
    
    # Wait for all pipelines to complete
    await asyncio.gather(*tasks, return_exceptions=True)


def load_pipelines_config(config_path: str = "pipelines.yml") -> List[Dict[str, Any]]:
    """Load pipeline configuration from YAML file."""
    path = Path(config_path)
    
    if not path.exists():
        logger.warning(f"Pipeline config file not found: {config_path}")
        return []
    
    with path.open() as f:
        data = yaml.safe_load(f)
    
    if "pipelines" not in data:
        logger.error(f"No 'pipelines' key found in {config_path}")
        return []
    
    pipelines = data["pipelines"]
    
    # Convert dict format to list format
    if isinstance(pipelines, dict):
        result = []
        for name, config in pipelines.items():
            config["name"] = name
            result.append(config)
        return result
    
    return pipelines
