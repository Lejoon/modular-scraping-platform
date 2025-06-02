#!/usr/bin/env python3
"""
Simple script to run a specific TCGPlayer pipeline.
"""

import asyncio
import logging
import os
import sys

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.pipeline_orchestrator import run_pipeline, load_pipelines_config
from core.plugin_loader import refresh_registry

# Make sure we run in the project root for relative paths
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

async def run_specific_pipeline(config_file: str, pipeline_name: str):
    """Run a specific pipeline by name."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Discover and register all plugins
    logger.info("Discovering plugins...")
    refresh_registry()
    
    # Load pipeline configuration
    pipelines_cfg = load_pipelines_config(config_file)
    if not pipelines_cfg:
        logger.error(f"No pipelines found in {config_file}")
        return
    
    # Find the specific pipeline
    target_pipeline = None
    for pipeline in pipelines_cfg:
        if pipeline.get("name") == pipeline_name:
            target_pipeline = pipeline
            break
    
    if not target_pipeline:
        logger.error(f"Pipeline '{pipeline_name}' not found in {config_file}")
        available = [p.get("name", "unnamed") for p in pipelines_cfg]
        logger.error(f"Available pipelines: {available}")
        return
    
    # Run the specific pipeline
    logger.info(f"Running pipeline: {pipeline_name}")
    await run_pipeline(target_pipeline)
    logger.info("Pipeline execution complete")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_pipeline.py <config_file> <pipeline_name>")
        print("Example: python run_pipeline.py tcg_test.yml tcgplayer_pokemon_sets")
        sys.exit(1)
    
    config_file = sys.argv[1]
    pipeline_name = sys.argv[2]
    
    asyncio.run(run_specific_pipeline(config_file, pipeline_name))
