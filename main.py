"""
Main entry point for the modular scraping platform with scheduling support.
"""

import asyncio
import logging
import os
import sys
import signal
from typing import Optional
from dotenv import load_dotenv # Add this import

# Add project root to PYTHONPATH so imports work when running this script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.pipeline_orchestrator import run_all_with_scheduler, run_all, load_pipelines_config
from core.plugin_loader import refresh_registry, list_available
from core.infra.scheduler import Scheduler
from core.infra.discord_bot import ScraperBot, create_bot_commands


async def main():
    """Main entry point with scheduler and optional Discord bot support."""
    # Load .env file
    load_dotenv() # Add this line

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Check if scheduler mode is disabled
    scheduler_mode = os.getenv("SCHEDULER_MODE", "enabled")
    discord_token = os.getenv("DISCORD_TOKEN")
    logger.info("Discord token: %s", "set" if discord_token else "not set")
    enable_discord = discord_token is not None
    
    if scheduler_mode == "disabled":
        logger.info("Starting pipeline-based scraper platform (one-time run)...")
        await run_without_scheduler()
        return
    
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
    scheduler_timezone = os.getenv("SCHEDULER_TIMEZONE", "Europe/Stockholm")
    scheduler = Scheduler(timezone=scheduler_timezone)
    
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
        if enable_discord:
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


async def run_without_scheduler():
    """Run pipelines once without scheduler (legacy behavior)."""
    logger = logging.getLogger(__name__)
    
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
    asyncio.run(main())


if __name__ == "__main__":
    run_pipeline_system()
