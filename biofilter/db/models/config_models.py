from biofilter.db.base import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
import datetime


class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(String, nullable=False)
    type = Column(String, nullable=False, default="string")
    description = Column(Text, nullable=True)
    editable = Column(Boolean, default=True)
    created_at = Column(
        DateTime, default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        onupdate=lambda: datetime.datetime.now(datetime.timezone.utc),
    )


"""
================================================================================
Developer Note - SystemConfig Model
================================================================================

The `SystemConfig` model is responsible for storing dynamic, global
configuration parameters that influence system behavior without requiring code
changes.

Key Design Considerations:

1. **Key/Value Storage**:
    - Each configuration is stored as a simple key-value pair.
    - Values are stored as strings but interpreted according to the `type`
        field (e.g., string, integer, boolean, float, etc.).

2. **Editable Flag**:
    - Controls whether the config can be updated via UI or external clients.
    - Used to protect critical internal settings from unintended changes.

3. **Description Field**:
    - Optional field for providing human-readable context to each setting.
    - Encouraged for documentation and frontend presentation.

4. **Audit Timestamps**:
    - `created_at` and `updated_at` are stored in UTC using Python
        timezone-aware datetimes.
    - Useful for tracking configuration changes over time.

5. **Uniqueness Constraint**:
    - The `key` field must be unique to avoid collisions and ambiguity.

Limitations & Future Enhancements:

- Current implementation stores all values as strings.
    Future versions may include value validation and type conversion during
        access.

- There is no built-in versioning or change history.
    Consider integrating change logs or audit tables if advanced tracking is
        needed.

- No encryption is applied to values. For sensitive settings like tokens or
    credentials, implement additional security measures in the application
    layer.

================================================================================
    Author: Andre Garon - Biofilter 3R  
    Date: 2025-04
================================================================================
"""
