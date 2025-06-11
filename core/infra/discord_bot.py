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

    @bot.tree.command(name="health", description="Show scheduler health status")
    async def _health(interaction: discord.Interaction):
        """Show scheduler health status."""
        try:
            health = bot.scheduler.get_health_status()
            
            # Build status message
            status_emoji = "🟢" if health["scheduler_running"] else "🔴"
            status_text = "Running" if health["scheduler_running"] else "Stopped"
            
            embed = discord.Embed(
                title="📊 Scheduler Health Status",
                color=discord.Color.green() if health["scheduler_running"] else discord.Color.red(),
                timestamp=discord.utils.utcnow()
            )
            
            embed.add_field(
                name="Status",
                value=f"{status_emoji} {status_text}",
                inline=True
            )
            
            embed.add_field(
                name="Total Jobs",
                value=str(health["total_jobs"]),
                inline=True
            )
            
            embed.add_field(
                name="Active Jobs",
                value=str(health["active_jobs"]),
                inline=True
            )
            
            if health["overdue_jobs"] > 0:
                embed.add_field(
                    name="⚠️ Overdue Jobs",
                    value=f"{health['overdue_jobs']} jobs overdue",
                    inline=False
                )
                
                if len(health["overdue_job_ids"]) <= 5:
                    embed.add_field(
                        name="Overdue Job IDs",
                        value="\n".join([f"• `{job_id}`" for job_id in health["overdue_job_ids"]]),
                        inline=False
                    )
            
            embed.add_field(
                name="Timezone",
                value=health["timezone"],
                inline=True
            )
            
            # Overall health indicator
            if health["scheduler_running"] and health["overdue_jobs"] == 0:
                embed.add_field(
                    name="Overall Status",
                    value="✅ Healthy",
                    inline=False
                )
            elif health["overdue_jobs"] > 0:
                embed.add_field(
                    name="Overall Status", 
                    value=f"⚠️ Warning: {health['overdue_jobs']} overdue jobs",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Overall Status",
                    value="❌ Issue: Scheduler not running",
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to get health status: {e}")
            await interaction.response.send_message(
                f"❌ Failed to get health status: {str(e)}",
                ephemeral=True
            )

    @bot.tree.command(name="next", description="Show jobs running in the next hour")
    async def _next(interaction: discord.Interaction, hours: int = 1):
        """Show jobs running in the next N hours."""
        try:
            from datetime import datetime, timezone, timedelta
            
            jobs = bot.scheduler.get_detailed_job_status()
            now = datetime.now(timezone.utc)
            cutoff = now + timedelta(hours=hours)
            
            upcoming = []
            for job in jobs:
                if job["next_run"] and job["status"] in ["scheduled", "imminent"]:
                    next_run_dt = datetime.fromisoformat(job["next_run"].replace('Z', '+00:00'))
                    if next_run_dt <= cutoff:
                        upcoming.append((job, next_run_dt))
            
            upcoming.sort(key=lambda x: x[1])
            
            if not upcoming:
                await interaction.response.send_message(
                    f"📅 No jobs scheduled in the next {hours} hour(s)"
                )
                return
            
            lines = []
            for job, next_run_dt in upcoming[:10]:  # Limit to 10 jobs
                time_until = (next_run_dt - now).total_seconds()
                
                if time_until < 60:
                    duration = f"{time_until:.0f}s"
                elif time_until < 3600:
                    duration = f"{time_until/60:.1f}m"
                else:
                    duration = f"{time_until/3600:.1f}h"
                
                status_emoji = "🟡" if job["status"] == "imminent" else "🟢"
                lines.append(f"{status_emoji} **{job['id']}**")
                lines.append(f"  └─ In {duration} ({next_run_dt.strftime('%H:%M:%S UTC')})")
                lines.append("")
            
            content = "\n".join(lines)
            if len(content) > 1800:
                content = content[:1800] + "\n..."
            
            embed = discord.Embed(
                title=f"⏰ Jobs Running in Next {hours} Hour(s)",
                description=content,
                color=discord.Color.blue(),
                timestamp=discord.utils.utcnow()
            )
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to get upcoming jobs: {e}")
            await interaction.response.send_message(
                f"❌ Failed to get upcoming jobs: {str(e)}",
                ephemeral=True
            )

    @bot.tree.command(name="overdue", description="Show overdue jobs")
    async def _overdue(interaction: discord.Interaction):
        """Show overdue jobs."""
        try:
            jobs = bot.scheduler.get_detailed_job_status()
            overdue = [job for job in jobs if job["status"] == "overdue"]
            
            if not overdue:
                await interaction.response.send_message("✅ No overdue jobs")
                return
            
            lines = []
            for job in overdue:
                overdue_seconds = job.get("overdue_seconds", 0)
                if overdue_seconds < 60:
                    duration = f"{overdue_seconds:.0f}s"
                elif overdue_seconds < 3600:
                    duration = f"{overdue_seconds/60:.1f}m"
                else:
                    duration = f"{overdue_seconds/3600:.1f}h"
                
                lines.append(f"🔴 **{job['id']}**")
                lines.append(f"  └─ Overdue by {duration}")
                lines.append("")
            
            content = "\n".join(lines)
            if len(content) > 1800:
                content = content[:1800] + "\n..."
            
            embed = discord.Embed(
                title="🚨 Overdue Jobs",
                description=content,
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow()
            )
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to get overdue jobs: {e}")
            await interaction.response.send_message(
                f"❌ Failed to get overdue jobs: {str(e)}",
                ephemeral=True
            )

    return bot
