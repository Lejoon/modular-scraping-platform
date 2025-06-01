"""
New pipeline orchestrator using Transform chain pattern.
"""

import asyncio
import logging
import yaml
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List

from .interfaces import Transform
from .plugin_loader import get as load_transform_class

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
