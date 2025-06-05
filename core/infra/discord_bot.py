"""
Discord bot for managing scraper pipelines.
"""

import asyncio
import logging
import json
from typing import Dict, Any, List

import discord
from discord.ext import commands

from ..pipeline_orchestrator import (
    run_pipeline,
    create_pipeline_runner,
    load_pipelines_config,
)
from .scheduler import Scheduler

logger = logging.getLogger(__name__)


class ScraperBot(commands.Bot):
    """Discord bot for managing scraper pipelines."""
    
    def __init__(self, scheduler: Scheduler, pipelines_cfg: List[Dict[str, Any]], **kwargs):
        intents = discord.Intents.default()
        intents.message_content = True  # Required for slash commands
        
        super().__init__(
            command_prefix="!",
            intents=intents,
            **kwargs
        )
        
        self.scheduler = scheduler
        self.pipelines_cfg = {p["name"]: p for p in pipelines_cfg}
        
    async def setup_hook(self):
        """Called when the bot is starting up."""
        # Start scheduler after bot is ready
        await self.scheduler.start()
        logger.info(f"Bot setup complete. Available pipelines: {list(self.pipelines_cfg.keys())}")
        
        # Sync slash commands
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    async def on_ready(self):
        """Called when the bot has successfully connected to Discord."""
        logger.info(f"Bot is ready! Logged in as {self.user}")

    async def close(self):
        """Called when the bot is shutting down."""
        await self.scheduler.stop()
        await super().close()


def create_bot_commands(bot: ScraperBot):
    """Create and register Discord slash commands."""
    
    @bot.tree.command(name="run", description="Run a pipeline immediately")
    async def _run(interaction: discord.Interaction, name: str):
        """Run a pipeline immediately."""
        if name not in bot.pipelines_cfg:
            available = ", ".join(bot.pipelines_cfg.keys())
            await interaction.response.send_message(
                f"❌ Unknown pipeline '{name}'\nAvailable: {available}", 
                ephemeral=True
            )
            return
            
        await interaction.response.defer(thinking=True)
        
        try:
            await run_pipeline(bot.pipelines_cfg[name])
            await interaction.followup.send(f"✅ Pipeline **{name}** completed successfully")
        except Exception as e:
            logger.error(f"Pipeline {name} failed: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Pipeline **{name}** failed: {str(e)}")

    @bot.tree.command(name="schedule", description="Add a cron job for a pipeline")
    async def _schedule(interaction: discord.Interaction, name: str, cron: str):
        """Schedule a pipeline with a cron expression."""
        cfg = bot.pipelines_cfg.get(name)
        if not cfg:
            available = ", ".join(bot.pipelines_cfg.keys())
            await interaction.response.send_message(
                f"❌ Unknown pipeline '{name}'\nAvailable: {available}", 
                ephemeral=True
            )
            return
            
        try:
            # Validate cron expression by creating the job
            runner = await create_pipeline_runner(cfg)
            job_id = f"pipeline_{name}_manual"
            bot.scheduler.add_cron_job(runner, cron_expression=cron, job_id=job_id)
            
            await interaction.response.send_message(
                f"📆 Scheduled **{name}** with cron `{cron}`\nJob ID: `{job_id}`"
            )
        except Exception as e:
            logger.error(f"Failed to schedule pipeline {name}: {e}")
            await interaction.response.send_message(
                f"❌ Failed to schedule pipeline: {str(e)}", 
                ephemeral=True
            )

    @bot.tree.command(name="jobs", description="List all scheduled jobs")
    async def _jobs(interaction: discord.Interaction):
        """List all scheduled jobs."""
        jobs = bot.scheduler.list_jobs()
        
        if not jobs:
            await interaction.response.send_message("📋 No jobs scheduled")
            return
            
        # Format job information
        lines = []
        for job_id, meta in jobs.items():
            next_run = meta.get('next_run')
            next_run_str = next_run.strftime('%Y-%m-%d %H:%M:%S UTC') if next_run else 'N/A'
            trigger = meta.get('trigger', 'Unknown')
            
            lines.append(f"**{job_id}**")
            lines.append(f"  └─ Next run: `{next_run_str}`")
            lines.append(f"  └─ Trigger: `{trigger}`")
            lines.append("")
        
        content = "\n".join(lines)
        
        # Split into chunks if too long for Discord
        if len(content) > 2000:
            content = content[:1997] + "..."
            
        await interaction.response.send_message(f"📋 **Scheduled Jobs:**\n\n{content}")

    @bot.tree.command(name="remove", description="Remove a scheduled job")
    async def _remove(interaction: discord.Interaction, job_id: str):
        """Remove a scheduled job."""
        try:
            bot.scheduler.remove_job(job_id)
            await interaction.response.send_message(f"🗑️ Removed job: `{job_id}`")
        except Exception as e:
            logger.error(f"Failed to remove job {job_id}: {e}")
            await interaction.response.send_message(
                f"❌ Failed to remove job: {str(e)}", 
                ephemeral=True
            )

    @bot.tree.command(name="pipelines", description="List available pipelines")
    async def _pipelines(interaction: discord.Interaction):
        """List all available pipelines."""
        if not bot.pipelines_cfg:
            await interaction.response.send_message("📋 No pipelines configured")
            return
            
        lines = []
        for name, cfg in bot.pipelines_cfg.items():
            chain_len = len(cfg.get("chain", []))
            schedule_info = ""
            
            if "schedule" in cfg:
                schedule_cfg = cfg["schedule"]
                if "cron" in schedule_cfg:
                    schedule_info = f" (cron: {schedule_cfg['cron']})"
                elif "interval" in schedule_cfg:
                    schedule_info = f" (interval: {schedule_cfg['interval']})"
                    
            lines.append(f"**{name}**{schedule_info}")
            lines.append(f"  └─ {chain_len} stages")
            lines.append("")
        
        content = "\n".join(lines)
        
        if len(content) > 2000:
            content = content[:1997] + "..."
            
        await interaction.response.send_message(f"📋 **Available Pipelines:**\n\n{content}")

    return bot
