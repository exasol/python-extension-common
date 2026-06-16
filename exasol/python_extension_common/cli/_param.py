from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Param:
    """
    Only used internally to distinguish between source and destination
    parameters.
    """

    name: str | None
    value: Any | None
