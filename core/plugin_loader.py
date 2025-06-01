"""
Plugin loader for automatic discovery and registration of transform classes.
"""

import importlib.util
import inspect
import logging
import pathlib
import sys
from types import ModuleType
from typing import Dict, Type

from .interfaces import Transform

logger = logging.getLogger(__name__)

# Plugin directory relative to this file
PLUGIN_DIR = pathlib.Path(__file__).parent.parent / "plugins"

# Global registry of discovered transform classes
_REGISTRY: Dict[str, Type[Transform]] = {}


def _load_module(path: pathlib.Path) -> ModuleType:
    """Load a Python module from a file path."""
    # Create module name like: plugins.fi_shortinterest.fetcher
    plugin_name = path.parent.name
    module_name = path.stem
    full_name = f"plugins.{plugin_name}.{module_name}"
    
    spec = importlib.util.spec_from_file_location(full_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {path}")
    
    mod = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = mod  # Allow intra-plugin imports
    spec.loader.exec_module(mod)
    
    logger.debug(f"Loaded module: {full_name}")
    return mod


def refresh_registry() -> None:
    """Scan all Python files in plugins/ and register Transform subclasses."""
    _REGISTRY.clear()
    
    if not PLUGIN_DIR.exists():
        logger.warning(f"Plugin directory does not exist: {PLUGIN_DIR}")
        return
    
    plugin_count = 0
    transform_count = 0
    
    # Walk through all Python files in plugins/
    for py_file in PLUGIN_DIR.rglob("*.py"):
        # Skip __init__.py and files starting with _
        if py_file.name.startswith("_"):
            continue
            
        try:
            mod = _load_module(py_file)
            plugin_count += 1
            
            # Find all Transform subclasses in this module
            for name, obj in inspect.getmembers(mod, inspect.isclass):
                if (issubclass(obj, Transform) and 
                    obj is not Transform and 
                    obj.__module__ == mod.__name__):
                    
                    # Register with key: plugin_name.ClassName
                    plugin_name = py_file.parent.name
                    key = f"{plugin_name}.{obj.__name__}"
                    _REGISTRY[key] = obj
                    transform_count += 1
                    
                    logger.debug(f"Registered transform: {key}")
        
        except Exception as e:
            logger.error(f"Failed to load module {py_file}: {e}")
    
    logger.info(f"Plugin discovery complete: {plugin_count} modules, {transform_count} transforms")


def get(class_path: str) -> Type[Transform]:
    """Get a transform class by its plugin path.
    
    Args:
        class_path: Format 'plugin_name.ClassName' (e.g., 'fi_shortinterest.FiFetcher')
    
    Returns:
        The transform class
        
    Raises:
        KeyError: If the class is not found
    """
    if not _REGISTRY:
        refresh_registry()
    
    if class_path not in _REGISTRY:
        available = list(_REGISTRY.keys())
        raise KeyError(f"Transform '{class_path}' not found. Available: {available}")
    
    return _REGISTRY[class_path]


def list_available() -> Dict[str, Type[Transform]]:
    """Get a copy of all registered transforms."""
    if not _REGISTRY:
        refresh_registry()
    return _REGISTRY.copy()
