from dataclasses import dataclass, field

from typing import Any, Dict, TYPE_CHECKING, Set, TypeVar, List
from typing_extensions import Protocol

if TYPE_CHECKING:
    from odex.index import Index
    from odex.set import Attributes

T = TypeVar("T")


class Context(Protocol[T]):
    """Interface for filter context, so `IndexedSet` can pass context to optimizers"""

    indexes: "Dict[str, List[Index]]"
    objs: Set[T]
    attrs: "Attributes"

    def getattr(self, obj: T, item: str) -> Any: ...


@dataclass
class SimpleContext(Context[T]):
    """Context as a dataclass. Intended for testing."""

    indexes: "Dict[str, List[Index]]" = field(default_factory=dict)
    objs: Set[T] = field(default_factory=set)
    attrs: "Attributes" = field(default_factory=dict)

    def getattr(self, obj: T, item: str) -> Any:
        return getattr(obj, item)
