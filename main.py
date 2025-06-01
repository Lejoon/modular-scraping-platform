"""
Main entry point for the new pipeline-based scraper platform.
"""

import asyncio
import logging
import os
import signal
import sys

# Add project root to PYTHONPATH so imports work when running this script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.pipeline_orchestrator import run_all, load_pipelines_config
from core.plugin_loader import refresh_registry, list_available

# Make sure we run in the project root for relative paths (e.g. scraper.db)
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)


async def main():
    """Main entry point for the pipeline orchestrator."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting pipeline-based scraper platform...")
    
    # Discover and register all plugins
    logger.info("Discovering plugins...")
    refresh_registry()
    available_transforms = list_available()
    logger.info(f"Discovered {len(available_transforms)} transform classes:")
    for name, cls in available_transforms.items():
        logger.info(f"  - {name}: {cls.__name__}")
    
    # Load pipeline configuration
    pipelines_cfg = load_pipelines_config("pipelines.yml")
    if not pipelines_cfg:
        logger.error("No pipelines configured. Exiting.")
        return
    
    logger.info(f"Loaded {len(pipelines_cfg)} pipeline(s)")
    for pipeline in pipelines_cfg:
        name = pipeline.get("name", "unnamed")
        chain_len = len(pipeline.get("chain", []))
        logger.info(f"  - {name}: {chain_len} stages")
    
    # Setup graceful shutdown
    stop_event = asyncio.Event()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()
    
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, signal_handler)
    
    # Launch all pipelines
    logger.info("Starting pipelines...")
    pipeline_task = asyncio.create_task(run_all(pipelines_cfg))
    
    # Wait for shutdown signal or pipeline completion
    try:
        await asyncio.wait([
            asyncio.create_task(stop_event.wait()),
            pipeline_task
        ], return_when=asyncio.FIRST_COMPLETED)
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        # Cancel any running tasks
        if not pipeline_task.done():
            logger.info("Cancelling pipelines...")
            pipeline_task.cancel()
            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Shutdown complete")


def run_pipeline_system():
    """Entry point that can be called from other scripts."""
    asyncio.run(main())


if __name__ == "__main__":
    run_pipeline_system()
