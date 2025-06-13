"""
Discord bot for managing scraper pipelines.
Admin commands are visible **only** to server administrators.

Visibility is handled via
    @app_commands.default_permissions(administrator=True)
Runtime execution is then further locked down to the configured
ADMIN_USER_ID (or any user with the Administrator permission).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from ..pipeline_orchestrator import (
    create_pipeline_runner,
    load_pipelines_config,
    run_pipeline,
)
from .scheduler import Scheduler

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

load_dotenv()  # Load variables from a .env file if present

# Convert to int if they exist, otherwise None
ADMIN_USER_ID: Optional[int] = (
    int(os.getenv("ADMIN_USER_ID")) if os.getenv("ADMIN_USER_ID") else None
)
ADMIN_GUILD_ID: Optional[int] = (
    int(os.getenv("ADMIN_GUILD_ID")) if os.getenv("ADMIN_GUILD_ID") else None
)

# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

async def is_bot_admin(interaction: discord.Interaction) -> bool:
    """Return *True* if the caller is the configured admin or has Administrator."""

    # Missing admin id on client ➔ refuse early (safety‑first)
    if not hasattr(interaction.client, "admin_user_id"):
        logger.warning("Admin check failed: admin_user_id not configured on bot client.")
        return False

    user = interaction.user
    is_admin_id = user.id == interaction.client.admin_user_id
    is_admin_perm = getattr(user.guild_permissions, "administrator", False)

    if is_admin_id or is_admin_perm:
        return True

    await interaction.response.send_message(
        "❌ You are not authorized to use this command.", ephemeral=True
    )
    return False


# ──────────────────────────────────────────────────────────────────────────
# Bot implementation
# ──────────────────────────────────────────────────────────────────────────

class ScraperBot(commands.Bot):
    """Discord bot for managing scraper pipelines."""

    def __init__(
        self,
        scheduler: Scheduler,
        pipelines_cfg: List[Dict[str, Any]],
        *,
        admin_user_id: Optional[int] = ADMIN_USER_ID,
        admin_guild_id: Optional[int] = ADMIN_GUILD_ID,
        **kwargs,
    ):  # noqa: D401
        intents = discord.Intents.default()  # Slash‑command‑only bot

        super().__init__(command_prefix="!", intents=intents, **kwargs)

        self.scheduler = scheduler
        self.pipelines_cfg = {p["name"]: p for p in pipelines_cfg}
        self.admin_user_id = admin_user_id
        self.admin_guild_id = admin_guild_id

        # Convenience list for permission syncing (kept, but not strictly required)
        self.admin_command_names: List[str] = ["run", "schedule", "jobs", "remove"]

    # ────────────────────────────────────────
    # Discord lifecycle hooks
    # ────────────────────────────────────────

    async def setup_hook(self):
        """Runs at startup before connecting to the gateway."""

        await self.scheduler.start()
        logger.info("Bot setup complete. Available pipelines: %s", list(self.pipelines_cfg))

        # ── Sync commands ───────────────────
        try:
            # (1) If ADMIN_GUILD_ID is set, we register admin‑only commands
            #     scoped to that guild so they appear instantly.
            if self.admin_guild_id:
                admin_guild_obj = discord.Object(id=self.admin_guild_id)
                synced_admin = await self.tree.sync(guild=admin_guild_obj)
                logger.info(
                    "Synced %d admin command(s) to guild %s",
                    len(synced_admin),
                    self.admin_guild_id,
                )

            # (2) Sync (or resync) global commands (e.g. /pipelines)
            synced_global = await self.tree.sync()
            logger.info("Synced %d global command(s)", len(synced_global))

        except Exception as exc:  # pragma: no cover — startup debug
            logger.exception("Failed to sync commands: %s", exc)


# ──────────────────────────────────────────────────────────────────────────
# Command registration helper
# ──────────────────────────────────────────────────────────────────────────

def create_bot_commands(bot: ScraperBot):
    """Create and register Discord application (slash) commands."""

    # Register admin commands to a specific guild if supplied
    admin_guild_obj = discord.Object(id=bot.admin_guild_id) if bot.admin_guild_id else None

    # ——————————————————————————————————————————————
    # ADMIN‑ONLY COMMANDS
    # Visible: Administrators only (via @default_permissions)
    # Executable: Only ADMIN_USER_ID or anyone with Administrator
    # ——————————————————————————————————————————————

    # /run
    @bot.tree.command(
        name="run",
        description="Run a pipeline immediately",
        guild=admin_guild_obj,
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.check(is_bot_admin)
    async def _run(interaction: discord.Interaction, name: str):
        if name not in bot.pipelines_cfg:
            available = ", ".join(bot.pipelines_cfg.keys())
            await interaction.response.send_message(
                f"❌ Unknown pipeline '{name}'\nAvailable: {available}",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)
        try:
            await run_pipeline(bot.pipelines_cfg[name])
            await interaction.followup.send(f"✅ Pipeline **{name}** completed successfully")
        except Exception as exc:
            logger.exception("Pipeline %s failed: %s", name, exc)
            await interaction.followup.send(f"❌ Pipeline **{name}** failed: {exc}")

    # /schedule
    @bot.tree.command(
        name="schedule",
        description="Add a cron job for a pipeline",
        guild=admin_guild_obj,
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.check(is_bot_admin)
    async def _schedule(interaction: discord.Interaction, name: str, cron: str):
        cfg = bot.pipelines_cfg.get(name)
        if not cfg:
            available = ", ".join(bot.pipelines_cfg.keys())
            await interaction.response.send_message(
                f"❌ Unknown pipeline '{name}'\nAvailable: {available}",
                ephemeral=True,
            )
            return

        try:
            runner = await create_pipeline_runner(cfg)
            job_id = f"pipeline_{name}_manual"
            bot.scheduler.add_cron_job(runner, cron_expression=cron, job_id=job_id)
            await interaction.response.send_message(
                f"📆 Scheduled **{name}** with cron `{cron}`\nJob ID: `{job_id}`"
            )
        except Exception as exc:
            logger.exception("Failed to schedule pipeline %s: %s", name, exc)
            await interaction.response.send_message(
                f"❌ Failed to schedule pipeline: {exc}", ephemeral=True
            )

    # /jobs
    @bot.tree.command(
        name="jobs",
        description="List all scheduled jobs",
        guild=admin_guild_obj,
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.check(is_bot_admin)
    async def _jobs(interaction: discord.Interaction):
        jobs = bot.scheduler.list_jobs()
        if not jobs:
            await interaction.response.send_message("📋 No jobs scheduled")
            return

        lines: List[str] = []
        for job_id, meta in jobs.items():
            next_run = meta.get("next_run")
            next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S UTC") if next_run else "N/A"
            trigger = meta.get("trigger", "Unknown")

            lines.append(f"**{job_id}**")
            lines.append(f"  └─ Next run: `{next_run_str}`")
            lines.append(f"  └─ Trigger: `{trigger}`")
            lines.append("")

        content = "\n".join(lines)[:1997] + ("..." if len(lines) > 1997 else "")
        await interaction.response.send_message(f"📋 **Scheduled Jobs:**\n\n{content}")

    # /remove
    @bot.tree.command(
        name="remove",
        description="Remove a scheduled job",
        guild=admin_guild_obj,
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.check(is_bot_admin)
    async def _remove(interaction: discord.Interaction, job_id: str):
        try:
            bot.scheduler.remove_job(job_id)
            await interaction.response.send_message(f"🗑️ Removed job: `{job_id}`")
        except Exception as exc:
            logger.exception("Failed to remove job %s: %s", job_id, exc)
            await interaction.response.send_message(
                f"❌ Failed to remove job: {exc}", ephemeral=True
            )

    # ——————————————————————————————————————————————
    # PUBLIC COMMANDS (visible to everyone)
    # ——————————————————————————————————————————————

    @bot.tree.command(name="pipelines", description="List available pipelines")
    async def _pipelines(interaction: discord.Interaction):
        if not bot.pipelines_cfg:
            await interaction.response.send_message("📋 No pipelines configured")
            return

        lines: List[str] = []
        for name, cfg in bot.pipelines_cfg.items():
            chain_len = len(cfg.get("chain", []))
            schedule_info = ""
            if "schedule" in cfg:
                sched_cfg = cfg["schedule"]
                if "cron" in sched_cfg:
                    schedule_info = f" (cron: {sched_cfg['cron']})"
                elif "interval" in sched_cfg:
                    schedule_info = f" (interval: {sched_cfg['interval']})"
            lines.append(f"**{name}**{schedule_info}")
            lines.append(f"  └─ {chain_len} stages\n")

        content = "\n".join(lines)[:1997] + ("..." if len(lines) > 1997 else "")
        await interaction.response.send_message(f"📋 **Available Pipelines:**\n\n{content}")

    return bot
