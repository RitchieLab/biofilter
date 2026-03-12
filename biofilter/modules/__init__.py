from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from biofilter.biofilter import BiofilterCore


@dataclass
class BaseComponent:
    """
    Base class for all Biofilter components.

    Components should be thin wrappers that:
    - Validate preconditions (e.g., DB connected)
    - Provide a stable public interface (bf.<component>.<method>)
    - Delegate heavy logic to domain managers (ETLManager, ReportManager, etc.)
    """

    core: "BiofilterCore"

    def require_db(self):
        """Return the active Database instance or raise."""
        return self.core.require_db()
