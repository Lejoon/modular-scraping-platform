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

from .db import Database
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

        fi_db_path = os.path.join(os.getcwd(), "db", "fi_shortinterest.db")
        self.fi_short_db = Database(fi_db_path)
        await self.fi_short_db.connect()
        logger.info("Connected FI short‐interest DB: %s", fi_db_path)
        
        registered_commands = [c.name for c in self.tree.get_commands(type=discord.InteractionType.application_command)]
        logger.info(f"Commands registered in tree before sync: {registered_commands}")


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
        await interaction.response.defer(thinking=True)  # ADDED
        jobs = bot.scheduler.list_jobs()
        if not jobs:
            await interaction.followup.send("📋 No jobs scheduled") # CHANGED
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

        content = "\\n".join(lines)[:1997] + ("..." if len(lines) > 1997 else "")
        await interaction.followup.send(f"📋 **Scheduled Jobs:**\\n\\n{content}") # CHANGED

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
        await interaction.response.defer(thinking=True)  # ADDED
        if not bot.pipelines_cfg:
            await interaction.followup.send("📋 No pipelines configured") # CHANGED
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

        content = "\\n".join(lines)[:1997] + ("..." if len(lines) > 1997 else "")
        await interaction.followup.send(f"📋 **Available Pipelines:**\\n\\n{content}") # CHANGED
    
    logger.info("Attempting to define /short command...")

    async def company_autocomplete(
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """Autocomplete for company names in the /short command."""
        
        # The 'bot' variable from the outer scope of create_bot_commands is the ScraperBot instance.
        if not hasattr(bot, 'fi_short_db'):
            logger.warning("fi_short_db attribute not found on bot for autocomplete.")
            return []

        db_conn = bot.fi_short_db
        
        try:
            # Check if the connection object exists and if the internal aiosqlite connection is None
            if db_conn._connection is None: 
                await db_conn.connect() # This method should handle its own logging for success/failure
        except Exception as e:
            logger.error(f"Failed to connect/reconnect fi_short_db for autocomplete: {e}")
            return []

        choices = []
        try:
            query = """
                SELECT DISTINCT company_name 
                FROM short_positions_history 
                WHERE LOWER(company_name) LIKE ? 
                ORDER BY company_name 
                LIMIT 5
            """
            rows = await db_conn.fetch_all(query, (f"{current.lower()}%",))
            
            if rows:
                choices = [
                    app_commands.Choice(name=str(row["company_name"]), value=str(row["company_name"]))
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Error during company autocomplete database query: {e}")
            # For more detailed debugging, you might want to log the full traceback
            # import traceback
            # logger.error(traceback.format_exc())
        
        return choices

    @bot.tree.command(name="short", description="Show short interest plot for a company (last 3 months)")
    @app_commands.autocomplete(company=company_autocomplete)
    async def _short(interaction: discord.Interaction, company: str):
        import pandas as pd
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib.ticker as mticker
        from matplotlib import rcParams
        import io

        await interaction.response.defer(thinking=True)

        db = bot.fi_short_db                   # use dedicated DB
        # Find canonical company name
        sql_name = """
            SELECT company_name
              FROM short_positions_history
             WHERE LOWER(company_name) LIKE ?
             LIMIT 1
        """
        row = await db.fetch_one(sql_name, (f"%{company.lower()}%",))
        if not row:
            return await interaction.followup.send(
                f"Kan inte hitta någon blankning för {company}."
            )
        company_name = row["company_name"]

        now = pd.Timestamp.now()
        ago = now - pd.DateOffset(months=3)
        sql_data = """
            SELECT event_timestamp, position_percent
              FROM short_positions_history
             WHERE company_name = ?
               AND event_timestamp BETWEEN ? AND ?
             ORDER BY event_timestamp
        """
        rows = await db.fetch_all(
            sql_data,
            (
                company_name,
                ago.strftime("%Y-%m-%d %H:%M"),
                now.strftime("%Y-%m-%d %H:%M"),
            ),
        )
        if not rows:
            return await interaction.followup.send(
                f"Company: {company_name}, no data available."
            )

        df = pd.DataFrame([dict(r) for r in rows])
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        df.set_index("event_timestamp", inplace=True)
        daily = df.resample("D").last().ffill()
        daily["position_percent"] /= 100

        # Plot
        plt.figure(figsize=(4, 2))
        rcParams.update({"font.size": 7})
        plt.rcParams["font.family"] = ["sans-serif"]
        plt.rcParams["font.sans-serif"] = ["Arial", "Helvetica", "DejaVu Sans"]
        plt.plot(daily.index, daily["position_percent"],
                 marker="o", linestyle="-", color="#7289DA", markersize=3)
        plt.title(f"{company_name}, Shorts % Last 3m".upper(),
                  fontsize=6, weight="bold", loc="left")
        plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
        plt.grid(True, which="both", linestyle="-", linewidth=0.5,
                 color="gray", alpha=0.3)
        for s in plt.gca().spines.values():
            s.set_visible(False)
        plt.tick_params(axis="x", labelsize=6)
        plt.tick_params(axis="y", labelsize=6)
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        plt.close()

        latest = daily.iloc[-1, 0] * 100
        await interaction.followup.send(
            f"Company: {company_name}, {latest:.2f}% total shorted above with smallest individual position > 0.1%",
            file=discord.File(buf, filename="plot.png"),
        )

    return bot
