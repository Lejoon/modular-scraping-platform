"""
Discord bot for managing scraper pipelines.
"""

import asyncio
import logging
import json
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
import os
import discord
from discord.ext import commands
from discord import app_commands # Import app_commands

from ..pipeline_orchestrator import (
    run_pipeline,
    create_pipeline_runner,
    load_pipelines_config,
)
from .scheduler import Scheduler

logger = logging.getLogger(__name__)


# Define your admin user ID and the guild ID where admin commands should be active
# These should ideally be loaded from a configuration file or environment variables
load_dotenv()  # Load environment variables from .env file if present

ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
ADMIN_GUILD_ID = v("ADMIN_GUILD_ID")


async def is_bot_admin(interaction: discord.Interaction) -> bool:
    """Checks if the interacting user is the configured admin."""
    # Ensure client has admin_user_id attribute
    if not hasattr(interaction.client, 'admin_user_id') or interaction.client.admin_user_id is None:
        logger.warning("Admin check failed: admin_user_id not configured on bot client.")
        return False
    
    is_admin = interaction.user.id == interaction.client.admin_user_id
    if not is_admin:
        await interaction.response.send_message(
            "❌ You are not authorized to use this command.", ephemeral=True
        )
    return is_admin


class ScraperBot(commands.Bot):
    """Discord bot for managing scraper pipelines."""
    
    def __init__(self, scheduler: Scheduler, pipelines_cfg: List[Dict[str, Any]], 
                 admin_user_id: Optional[int] = ADMIN_USER_ID, 
                 admin_guild_id: Optional[int] = ADMIN_GUILD_ID, 
                 **kwargs):
        intents = discord.Intents.default()
        # intents.message_content = True # Not strictly required for slash commands only bot
        # intents.members = True # If you need to fetch member objects by ID not from interaction
        
        super().__init__(
            command_prefix="!", # Not used for slash commands but good to have a placeholder
            intents=intents,
            **kwargs
        )
        
        self.scheduler = scheduler
        self.pipelines_cfg = {p["name"]: p for p in pipelines_cfg}
        self.admin_user_id = admin_user_id
        self.admin_guild_id = admin_guild_id
        
        self.admin_command_names = ["run", "schedule", "jobs", "remove"]
        
    async def setup_hook(self):
        """Called when the bot is starting up."""
        await self.scheduler.start()
        logger.info(f"Bot setup complete. Available pipelines: {list(self.pipelines_cfg.keys())}")
        
        # Sync slash commands
        try:
            if self.admin_guild_id:
                # Sync admin commands to the specific admin guild
                admin_guild_obj = discord.Object(id=self.admin_guild_id)
                synced_admin = await self.tree.sync(guild=admin_guild_obj)
                logger.info(f"Synced {len(synced_admin)} admin command(s) to guild {self.admin_guild_id}")
                
                # Sync public commands globally (or to other guilds if needed)
                # For simplicity, this example syncs all commands. If you have truly global commands,
                # you might need a more sophisticated sync strategy or sync all globally and let permissions handle it.
                # For now, let's assume all commands are either admin (in admin_guild) or public (global).
                # If all commands are guild-specific, this global sync might not be needed or desired.
                # If you have other global commands, sync them separately without a guild argument.
                # For this example, we assume all commands are defined and will be synced.
                # If a command is not guild-specific in its decorator, it will be synced globally.
                # If it is, it's already synced above.
                
                # Let's refine: sync only non-admin commands globally if they exist
                # This part needs careful thought based on how commands are structured.
                # For now, let's assume commands are either admin (guild-specific) or global.
                # The current structure registers all commands and then syncs.
                # If a command has a guild in its decorator, tree.sync() without guild won't sync it again.
                # If a command does NOT have a guild in decorator, tree.sync(guild=...) won't sync it.
                
                # Simplest: Sync all. If a command has a guild in decorator, it goes there. Otherwise global.
                synced_global = await self.tree.sync() # Syncs global commands
                logger.info(f"Synced {len(synced_global)} global command(s)")

            else:
                # No admin guild specified, sync all commands globally
                synced = await self.tree.sync()
                logger.info(f"Synced {len(synced)} command(s) globally (admin guild not specified). Admin commands will rely on runtime checks only.")

            # Set explicit permissions for admin commands if admin_guild_id and admin_user_id are set
            if self.admin_guild_id and self.admin_user_id:
                guild_obj = discord.Object(id=self.admin_guild_id)
                admin_user_obj = discord.Object(id=self.admin_user_id) # For CommandPermission

                # Fetch commands registered to the admin guild
                # Note: get_commands() gets commands from the bot's internal tree,
                # fetch_commands() gets them from Discord API after sync.
                # We need commands from the tree that are meant for this guild.
                
                commands_in_guild = self.tree.get_commands(guild=guild_obj, type=discord.AppCommandType.chat_input)

                for cmd in commands_in_guild:
                    if cmd.name in self.admin_command_names:
                        # This command is an admin command and registered to the admin guild
                        permissions_to_set = {
                            self.admin_user_id: app_commands.CommandPermission(admin_user_obj, type=discord.AppCommandPermissionType.user, permission=True)
                        }
                        try:
                            await self.tree.edit_command_permissions(cmd, guild_obj, permissions_to_set)
                            logger.info(f"Set explicit permissions for admin command '{cmd.name}' for user {self.admin_user_id} in guild {self.admin_guild_id}")
                        except Exception as e_perm:
                            logger.error(f"Failed to set permissions for admin command '{cmd.name}' in guild {self.admin_guild_id}: {e_perm}", exc_info=True)
            elif self.admin_command_names and not self.admin_guild_id:
                 logger.warning("Admin commands are defined, but no ADMIN_GUILD_ID is set. These commands will be global and rely solely on runtime checks.")


        except Exception as e:
            logger.error(f"Failed to sync commands or set permissions: {e}", exc_info=True)


def create_bot_commands(bot: ScraperBot):
    """Create and register Discord slash commands."""

    admin_guild_object = discord.Object(id=bot.admin_guild_id) if bot.admin_guild_id else None
    
    # Define default permissions for admin commands: no one by default
    # Server owners and users with Administrator permission might still see them.
    # The explicit permission set in setup_hook for ADMIN_USER_ID is key.
    admin_default_permissions = discord.Permissions.none()


    @bot.tree.command(
        name="run", 
        description="Run a pipeline immediately",
        guild=admin_guild_object, # Register to admin guild if specified
        default_permissions=admin_default_permissions if admin_guild_object else None # Restrict by default if in admin guild
    )
    @app_commands.check(is_bot_admin)
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

    @bot.tree.command(
        name="schedule", 
        description="Add a cron job for a pipeline",
        guild=admin_guild_object,
        default_permissions=admin_default_permissions if admin_guild_object else None
    )
    @app_commands.check(is_bot_admin)
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

    @bot.tree.command(
        name="jobs", 
        description="List all scheduled jobs",
        guild=admin_guild_object,
        default_permissions=admin_default_permissions if admin_guild_object else None
    )
    @app_commands.check(is_bot_admin)
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

    @bot.tree.command(
        name="remove", 
        description="Remove a scheduled job",
        guild=admin_guild_object,
        default_permissions=admin_default_permissions if admin_guild_object else None
    )
    @app_commands.check(is_bot_admin)
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

    @bot.tree.command(name="pipelines", description="List available pipelines") # This remains a global/public command
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
