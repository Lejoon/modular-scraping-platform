"""
Main entry point for the scraper platform.
"""

import logging
import sys
import os

# Add project root to PYTHONPATH so imports work when running this script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.orchestrator import Orchestrator

# Make sure we run in the project root for relative paths (e.g. scraper.db)
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)


def main():
    """Main entry point."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting scraper platform...")
    
    # Create and run orchestrator with config-driven approach
    orchestrator = Orchestrator("config.yaml")
    orchestrator.run()


if __name__ == "__main__":
    main()