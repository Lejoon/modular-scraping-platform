"""
Orchestrator for managing the Fetch→Parse→Sink pipeline.
"""

import asyncio
import logging
import traceback
import yaml
from pathlib import Path
from typing import Dict, Any, List

try:
    import importlib.metadata as imeta
except ImportError:
    import importlib_metadata as imeta

from .infra.scheduler import Scheduler


logger = logging.getLogger(__name__)


def load_entrypoints(group: str) -> Dict[str, Any]:
    """Load entry points for a given group."""
    classes = {}
    try:
        for ep in imeta.entry_points(group=group):
            classes[ep.name] = ep.load()
    except Exception as e:
        logger.warning(f"Failed to load entry points for {group}: {e}")
    return classes


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


class Orchestrator:
    """Main orchestrator for managing services via configuration."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.cfg = load_config(config_path)
        
        # Discover classes via entry points
        self.fetchers = {}
        self.parsers = {}
        self.sinks = {}
        
        # Load entry points
        self._load_entry_points()
        
        self.scheduler = Scheduler()
        self._running = False

    def _load_entry_points(self):
        """Load all entry points for fetchers, parsers, and sinks."""
        self.fetchers = load_entrypoints("scraper_platform.fetchers")
        self.parsers = load_entrypoints("scraper_platform.parsers")
        self.sinks = load_entrypoints("scraper_platform.sinks")
        
        logger.info(f"Loaded {len(self.fetchers)} fetchers, {len(self.parsers)} parsers, {len(self.sinks)} sinks")

    async def _wire_service(self, svc_cfg: Dict[str, Any]):
        """Wire up and run a single service once."""
        service_name = svc_cfg["name"]
        
        try:
            # Create fetcher
            fetcher_name = svc_cfg["fetcher"]
            if fetcher_name not in self.fetchers:
                logger.error(f"Unknown fetcher: {fetcher_name}")
                return
            
            fetcher_defaults = self.cfg.get("fetcher_defaults", {})
            fetcher = self.fetchers[fetcher_name](**fetcher_defaults)
            
            # Create parsers
            parsers = []
            for parser_name in svc_cfg["parsers"]:
                if parser_name not in self.parsers:
                    logger.error(f"Unknown parser: {parser_name}")
                    continue
                parser = self.parsers[parser_name]()
                parsers.append(parser)
            
            # Create sinks
            sinks = []
            sink_defaults = self.cfg.get("sink_defaults") or {}
            for sink_def in svc_cfg["sinks"]:
                # Support both old format (string) and new format (dict with type/config)
                if isinstance(sink_def, str):
                    # Old format: just the sink name
                    sink_name = sink_def
                    sink_config = {}
                else:
                    # New format: {"type": "SinkName", "config": {...}}
                    sink_name = sink_def["type"]
                    sink_config = sink_def.get("config", {})
                
                if sink_name not in self.sinks:
                    logger.error(f"Unknown sink: {sink_name}")
                    continue
                
                # Merge global sink_defaults with this service's overrides
                merged_config = {**sink_defaults, **sink_config}
                sink = self.sinks[sink_name](**merged_config)
                sinks.append(sink)
            
            logger.info(f"Running service: {service_name}")
            
            # Execute the pipeline: fetch -> parse -> sink
            async for raw_item in fetcher.fetch():
                for parser in parsers:
                    try:
                        parsed_items = await parser.parse(raw_item)
                        for parsed_item in parsed_items:
                            # Handle diff parsing if available
                            items_to_sink = [parsed_item]
                            if hasattr(parser, 'diff'):
                                diff_items = await parser.diff(parsed_item)
                                if diff_items:  # Only use diff if it returns items
                                    items_to_sink = diff_items
                            
                            # Send to all sinks
                            for item in items_to_sink:
                                for sink in sinks:
                                    try:
                                        await sink.handle(item)
                                    except Exception as e:
                                        logger.error(f"Sink {sink.name} failed: {e}")
                    except Exception as e:
                        logger.error(f"Parser {parser.name} failed: {e}")
                        logger.debug(traceback.format_exc())
                        
        except Exception as e:
            logger.error(f"Service {service_name} failed: {e}")
            logger.debug(traceback.format_exc())

    async def start(self):
        """Start the orchestrator and schedule services."""
        if self._running:
            return
            
        self._running = True
        await self.scheduler.start()
        
        # Schedule each service
        for svc in self.cfg["services"]:
            try:
                # Create a lambda that captures the service config
                job_func = lambda svc=svc: asyncio.create_task(self._wire_service(svc))
                
                self.scheduler.add_cron_job(
                    func=job_func,
                    cron_expression=svc["schedule"],
                    job_id=svc["name"],
                    name=svc["name"],
                )
                logger.info(f"Scheduled service '{svc['name']}' with schedule '{svc['schedule']}'")
                # Kick off service immediately on startup
                asyncio.create_task(self._wire_service(svc))
            except Exception as e:
                logger.error(f"Failed to schedule service {svc['name']}: {e}")

    async def stop(self):
        """Stop the orchestrator."""
        if not self._running:
            return
            
        self._running = False
        await self.scheduler.stop()
        logger.info("Orchestrator stopped")

    def run(self):
        """Run the orchestrator in the current event loop."""
        async def _run():
            await self.start()
            try:
                while self._running:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
            finally:
                await self.stop()
        
        asyncio.run(_run())
