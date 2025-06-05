"""
Main entry point with scheduling and Discord bot support.
"""

import asyncio
import logging
import os
import signal
import sys
from typing import Optional

# Add project root to PYTHONPATH so imports work when running this script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.pipeline_orchestrator import run_all_with_scheduler, load_pipelines_config
from core.plugin_loader import refresh_registry, list_available
from core.infra.scheduler import Scheduler
from core.infra.discord_bot import ScraperBot, create_bot_commands

# Make sure we run in the project root for relative paths (e.g. scraper.db)
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)


async def main_with_scheduler(
    enable_discord: bool = False, 
    discord_token: Optional[str] = None
):
    """Main entry point with scheduler and optional Discord bot."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting pipeline-based scraper platform with scheduler...")
    
    # Discover and register all plugins
    logger.info("Discovering plugins...")
    refresh_registry()
    available_transforms = list_available()
    logger.info(f"Discovered {len(available_transforms)} transform classes:")
    for name, cls in available_transforms.items():
        logger.info(f"  - {name}: {cls.__name__}")
    
    # Load pipeline configuration
    config_file = os.getenv("PIPELINES_CONFIG", "pipelines.yml")
    pipelines_cfg = load_pipelines_config(config_file)
    if not pipelines_cfg:
        logger.error(f"No pipelines configured in {config_file}. Exiting.")
        return
    
    logger.info(f"Loaded {len(pipelines_cfg)} pipeline(s)")
    for pipeline in pipelines_cfg:
        name = pipeline.get("name", "unnamed")
        chain_len = len(pipeline.get("chain", []))
        schedule_info = ""
        if "schedule" in pipeline:
            schedule_cfg = pipeline["schedule"]
            if "cron" in schedule_cfg:
                schedule_info = f" (cron: {schedule_cfg['cron']})"
            elif "interval" in schedule_cfg:
                schedule_info = f" (interval: {schedule_cfg['interval']})"
        logger.info(f"  - {name}: {chain_len} stages{schedule_info}")
    
    # Create scheduler
    scheduler = Scheduler(timezone="Europe/Stockholm")
    
    # Setup graceful shutdown
    stop_event = asyncio.Event()
    bot_task = None
    
    def signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()
    
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, signal_handler)
    
    try:
        # Start scheduler
        await scheduler.start()
        
        # Start Discord bot if enabled
        if enable_discord and discord_token:
            logger.info("Starting Discord bot...")
            bot = ScraperBot(scheduler=scheduler, pipelines_cfg=pipelines_cfg)
            create_bot_commands(bot)
            
            # Start bot in background
            bot_task = asyncio.create_task(bot.start(discord_token))
        
        # Schedule pipelines
        logger.info("Setting up pipeline schedules...")
        pipeline_task = asyncio.create_task(
            run_all_with_scheduler(pipelines_cfg, scheduler=scheduler)
        )
        
        # Wait for shutdown signal
        await stop_event.wait()
        
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        # Cleanup
        logger.info("Shutting down...")
        
        # Cancel pipeline task
        if 'pipeline_task' in locals() and not pipeline_task.done():
            logger.info("Cancelling pipelines...")
            pipeline_task.cancel()
            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass
        
        # Stop Discord bot
        if bot_task and not bot_task.done():
            logger.info("Stopping Discord bot...")
            bot_task.cancel()
            try:
                await bot_task
            except asyncio.CancelledError:
                pass
        
        # Stop scheduler
        await scheduler.stop()
        
        logger.info("Shutdown complete")


async def main():
    """Legacy main entry point - no scheduler."""
    from core.pipeline_orchestrator import run_all
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting pipeline-based scraper platform (legacy mode)...")
    
    # Discover and register all plugins
    logger.info("Discovering plugins...")
    refresh_registry()
    available_transforms = list_available()
    logger.info(f"Discovered {len(available_transforms)} transform classes:")
    for name, cls in available_transforms.items():
        logger.info(f"  - {name}: {cls.__name__}")
    
    # Load pipeline configuration
    config_file = os.getenv("PIPELINES_CONFIG", "pipelines.yml")
    pipelines_cfg = load_pipelines_config(config_file)
    if not pipelines_cfg:
        logger.error(f"No pipelines configured in {config_file}. Exiting.")
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
    # Check for scheduler mode
    mode = os.getenv("SCHEDULER_MODE", "legacy")
    discord_token = os.getenv("DISCORD_TOKEN")
    
    if mode == "scheduler":
        enable_discord = discord_token is not None
        asyncio.run(main_with_scheduler(
            enable_discord=enable_discord,
            discord_token=discord_token
        ))
    else:
        asyncio.run(main())


if __name__ == "__main__":
    run_pipeline_system()
