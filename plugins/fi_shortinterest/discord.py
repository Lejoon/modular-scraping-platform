from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, List

import discord
from discord import app_commands
from discord.ext import commands

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib import rcParams
import io
import asyncio

from core.interfaces import DiscordCommands
from core.infra.db import Database
from core.infra.http import HttpClient # Added HttpClient import
import os

logger = logging.getLogger(__name__) # Moved logger to module level

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    # from core.infra.discord_bot import ScraperBot # If you have a custom bot class
    # class ScraperBot(Bot):
    #     fi_short_db: Database
    #     http_client: HttpClient # Changed from http_session

# Avanza API Market Cap Fetching
AVANZA_SEARCH_URL = "https://www.avanza.se/_api/search/filtered-search"
AVANZA_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (market-cap lookup script)", # Consider making this more generic or configurable
}

async def _fetch_avanza_data(http_client: HttpClient, url: str, params: dict) -> dict:
    # HttpClient handles retries, status checks, and JSON parsing.
    # Pass AVANZA_HEADERS per request as they are specific to this API.
    try:
        return await http_client.get_json(url, params=params, headers=AVANZA_HEADERS)
    except Exception as e:
        logger.error(f"Error fetching Avanza data from {url} with params {params}: {e}", exc_info=True)
        raise # Re-raise to be handled by the caller, or return None/empty dict

async def _get_orderbook_id(http_client: HttpClient, isin: str) -> Optional[str]:
    params = {"query": isin, "limit": 1, "marketPlace": "SE"}
    try:
        data = await _fetch_avanza_data(http_client, AVANZA_SEARCH_URL, params)
        if data and data.get("totalMatches", 0) > 0 and data.get("hits"):
            for hit in data["hits"]:
                if hit.get("instrumentType") == "STOCK":
                    return hit.get("id")
    except Exception as e:
        # Error already logged in _fetch_avanza_data if it originated there
        logger.error(f"Failed to get orderbook ID for ISIN {isin} after fetch attempt: {e}")
    return None

async def get_market_cap(http_client: HttpClient, isin: str) -> Optional[int]:
    orderbook_id = await _get_orderbook_id(http_client, isin)
    if not orderbook_id:
        return None
    
    avanza_stock_url = f"https://www.avanza.se/_api/market-guide/stock/{orderbook_id}"
    try:
        data = await _fetch_avanza_data(http_client, avanza_stock_url, params={})
        if data and "marketCapital" in data:
            return int(data["marketCapital"])
    except Exception as e:
        # Error already logged in _fetch_avanza_data if it originated there
        logger.error(f"Failed to get market cap for ISIN {isin} (Orderbook ID: {orderbook_id}) after fetch attempt: {e}")
    return None


class FiShortInterestDiscordCommands(DiscordCommands):
    def __init__(self):
        pass

    async def setup(self, bot: Bot) -> None:
        if not hasattr(bot, 'fi_short_db'):
            fi_db_path = os.path.join(os.getcwd(), "db", "fi_shortinterest.db")
            db_instance = Database(fi_db_path)
            try:
                await db_instance.connect()
                setattr(bot, 'fi_short_db', db_instance) 
                logger.info(f"Successfully connected and attached fi_shortinterest DB from plugin: {fi_db_path}")
            except Exception as e:
                logger.error(f"Failed to connect fi_shortinterest DB from plugin: {e}", exc_info=True)
        else:
            logger.info("fi_short_db already exists on bot instance. Skipping setup in fi_shortinterest plugin.")

    def register(self, bot: Bot) -> None:
        # Ensure bot has fi_short_db and http_client attributes
        # These should be set up in your main bot class (ScraperBot)

        async def company_autocomplete(
            interaction: discord.Interaction,
            current: str,
        ) -> List[app_commands.Choice[str]]:
            if not hasattr(bot, 'fi_short_db'):
                logger.warning("fi_short_db attribute not found on bot for autocomplete.")
                return []

            db_conn = bot.fi_short_db
            
            try:
                if db_conn._connection is None: 
                    await db_conn.connect()
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
                # Ensure the bot.fi_short_db.fetch_all method is available and correct
                rows = await db_conn.fetch_all(query, (f"{current.lower()}%",))
                
                if rows:
                    choices = [
                        app_commands.Choice(name=str(row["company_name"]), value=str(row["company_name"]))
                        for row in rows
                    ]
            except Exception as e:
                logger.error(f"Error during company autocomplete database query: {e}")
            return choices

        @bot.tree.command(name="short", description="Show short interest plot for a company (last 3 months)")
        @app_commands.autocomplete(company=company_autocomplete)
        async def short_command(interaction: discord.Interaction, company: str):
            await interaction.response.defer(thinking=True)

            if not hasattr(bot, 'fi_short_db'):
                await interaction.followup.send("Database connection not available.")
                return

            db = bot.fi_short_db
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

        @bot.tree.command(name="hedgeshort", description="Show market-cap weighted aggregated short interest (last 3 months)")
        @app_commands.default_permissions(administrator=True)
        async def hedgeshort_command(interaction: discord.Interaction):
            await interaction.response.defer(thinking=True)

            if not hasattr(bot, 'fi_short_db') or not hasattr(bot, 'http_client'): # Changed to http_client
                await interaction.followup.send("Database or HTTP client not available.") # Changed message
                return

            db = bot.fi_short_db
            http_client_instance = bot.http_client # Changed from http_session

            now = pd.Timestamp.now()
            plot_ago = now - pd.DateOffset(months=3)
            plot_start_date = plot_ago.normalize()
            plot_end_date = now.normalize()

            sql_data = """
                SELECT event_timestamp, entity_name, isin, position_percent
                  FROM position_holders_history
                 WHERE event_timestamp <= ?
                 ORDER BY event_timestamp, entity_name, isin
            """
            rows = await db.fetch_all(
                sql_data,
                (now.strftime("%Y-%m-%d %H:%M:%S"),),
            )

            if not rows:
                # Simplified empty plot logic
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "No historical data available.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                plt.gca().xaxis.set_major_locator(mdates.MonthLocator()); plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
                plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1)); plt.xlim([plot_start_date, plot_end_date])
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "No short position data to aggregate.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return

            df = pd.DataFrame([dict(r) for r in rows])
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
            df["event_date"] = df["event_timestamp"].dt.normalize()
            df["position_percent"] = pd.to_numeric(df["position_percent"], errors='coerce').fillna(0)
            df = df.dropna(subset=["isin"])

            if df.empty:
                # (Similar empty plot logic)
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "No valid data with ISINs.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                # ... (rest of empty plot styling)
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "No valid data with ISINs to process.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return

            unique_isins = df["isin"].unique()
            isin_market_caps = {}
            logger.info(f"Fetching market caps for {len(unique_isins)} unique ISINs for hedgeshort...")
            for isin_code in unique_isins:
                if pd.isna(isin_code): continue
                mcap = await get_market_cap(http_client_instance, isin_code) # Pass http_client_instance
                if mcap is not None:
                    isin_market_caps[isin_code] = mcap
                else:
                    logger.warning(f"Hedgeshort: Market cap not found for ISIN: {isin_code}. It will be excluded.")
            
            if not isin_market_caps:
                # (Similar empty plot logic)
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "Could not fetch market caps.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                # ... (rest of empty plot styling)
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "Could not fetch any market cap data for hedgeshort. Cannot generate weighted plot.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return

            df = df[df['isin'].isin(isin_market_caps.keys())]
            if df.empty:
                # (Similar empty plot logic)
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "No data for ISINs with market caps.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                # ... (rest of empty plot styling)
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "No data for ISINs with available market caps for hedgeshort.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return
            
            min_data_date_in_df = df["event_date"].min()
            processing_index_start = min_data_date_in_df
            processing_index_end = plot_end_date
            
            if pd.isna(processing_index_start) or processing_index_start > processing_index_end:
                # (Similar empty plot logic)
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "No valid date range for processing.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                # ... (rest of empty plot styling)
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "No valid date range for processing hedgeshort data.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return

            full_history_processing_index = pd.date_range(start=processing_index_start, end=processing_index_end, freq='D')
            all_shorted_values_series = []
            daily_active_isins = {day: set() for day in full_history_processing_index}

            for (entity_name, current_isin), group in df.groupby(['entity_name', 'isin']):
                mcap = isin_market_caps[current_isin]
                entity_isin_daily_pos = group.sort_values('event_timestamp').drop_duplicates(subset=['event_date'], keep='last')
                entity_isin_daily_pos = entity_isin_daily_pos.set_index('event_date')['position_percent']
                filled_positions = entity_isin_daily_pos.reindex(full_history_processing_index).ffill().fillna(0)
                shorted_value_for_holding = (filled_positions / 100.0) * mcap
                all_shorted_values_series.append(shorted_value_for_holding)
                for day, val in shorted_value_for_holding.items():
                    if val > 0:
                        daily_active_isins[day].add(current_isin)
            
            if not all_shorted_values_series:
                # (Similar empty plot logic)
                plt.figure(figsize=(4, 2)); rcParams.update({"font.size": 7})
                plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(), fontsize=6, weight="bold", loc="left")
                plt.text(0.5, 0.5, "No holdings to aggregate.", ha='center', va='center', transform=plt.gca().transAxes, fontsize=7)
                # ... (rest of empty plot styling)
                plt.grid(True, which="both", ls="-", lw=0.5, c="gray", alpha=0.3); [s.set_visible(False) for s in plt.gca().spines.values()]
                plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6); plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format="png"); buf.seek(0); plt.close()
                await interaction.followup.send(
                    "No holdings data to aggregate for hedgeshort after processing.",
                    file=discord.File(buf, filename="hedgeshort_plot.png")
                )
                return

            combined_short_values_df = pd.concat(all_shorted_values_series, axis=1)
            daily_total_shorted_mcap = combined_short_values_df.sum(axis=1)
            daily_total_market_cap_base = pd.Series(0.0, index=full_history_processing_index)
            for day, active_isins_on_day in daily_active_isins.items():
                current_day_mcap_sum = sum(isin_market_caps.get(active_isin, 0) for active_isin in active_isins_on_day)
                daily_total_market_cap_base[day] = current_day_mcap_sum
            
            weighted_short_percentage_full = (daily_total_shorted_mcap / daily_total_market_cap_base.replace(0, pd.NA)).fillna(0)
            final_plot_display_index = pd.date_range(start=plot_start_date, end=plot_end_date, freq='D')
            plot_data_for_formatter = weighted_short_percentage_full.reindex(final_plot_display_index).fillna(0)

            plt.figure(figsize=(4, 2))
            rcParams.update({"font.size": 7})
            # ... (rest of plot styling as before) ...
            plt.plot(plot_data_for_formatter.index, plot_data_for_formatter,
                     marker="o", linestyle="-", color="#7289DA", markersize=3)
            plt.title("MCAP Weighted Aggregated Shorts % Last 3m".upper(),
                      fontsize=6, weight="bold", loc="left")
            plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
            plt.xlim([final_plot_display_index.min(), final_plot_display_index.max()])
            plt.grid(True, which="both", linestyle="-", linewidth=0.5, color="gray", alpha=0.3)
            for s in plt.gca().spines.values(): s.set_visible(False)
            plt.tick_params(axis="x", labelsize=6); plt.tick_params(axis="y", labelsize=6)
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format="png")
            buf.seek(0)
            plt.close()

            latest_weighted_percentage = 0
            if not plot_data_for_formatter.empty:
                latest_weighted_percentage = plot_data_for_formatter.iloc[-1] * 100

            await interaction.followup.send(
                f"Total market-cap weighted aggregated short position: {latest_weighted_percentage:.2f}%",
                file=discord.File(buf, filename="hedgeshort_plot.png"),
            )
        logger.info("Registered Discord commands for fi_shortinterest plugin.")

        @bot.tree.command(name="fi_marketcap", description="Get market capitalization for a Swedish ISIN.")
        @app_commands.describe(isin="The ISIN code of the stock (e.g., SE0000115446)")
        async def fi_marketcap(interaction: discord.Interaction, isin: str):
            await interaction.response.defer(thinking=True)
            if not hasattr(bot, 'http_client'): # Changed to http_client
                await interaction.followup.send("Error: HTTP client not available on the bot.") # Changed message
                return

            market_cap = await get_market_cap(bot.http_client, isin) # Pass bot.http_client
            if market_cap is not None:
                await interaction.followup.send(f"Market capitalization of ISIN {isin}: {market_cap} SEK.")
            else:
                await interaction.followup.send(f"Could not retrieve market capitalization for ISIN {isin}.")
