"""AppMagic plugin package – fetcher, parser, sink.

The public interface that *pipeline_orchestrator* discovers is the
three classes below:

* :class:`AppMagicFetcher` – downloads raw JSON from AppMagic REST API
* :class:`AppMagicParser`  – converts :class:`~core.models.RawItem` -> :class:`~core.models.ParsedItem`
* :class:`AppMagicSink`    – persists parsed items into SQLite via :pyclass:`core.infra.db.Database`

All logic is contained in the three sibling modules; this file only
exposes convenience re‑exports so that YAML can reference a short path
such as:

```yaml
- "core.plugins.appmagic.AppMagicFetcher": { … }
```
"""

from .fetcher import AppMagicFetcher   # noqa: F401
from .parser import AppMagicParser     # noqa: F401
from .sinks import AppMagicSink        # noqa: F401
