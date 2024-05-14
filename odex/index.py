from abc import abstractmethod
from typing import Generic, TypeVar, Set, Any, Optional, Iterable, Dict
from typing_extensions import Protocol

from odex.condition import Condition, Eq, Literal, Attribute, In
from odex.context import Context
from odex.plan import IndexLookup

T = TypeVar("T")


class Index(Protocol[T]):
    @abstractmethod
    def add(self, objs: Set[T], ctx: Context[T]) -> None:
        """Add `objs` to the index"""

    @abstractmethod
    def remove(self, objs: Set[T], ctx: Context[T]) -> None:
        """Remove `objs` from the index"""

    @abstractmethod
    def match(self, condition: Condition) -> "Optional[IndexLookup]":
        """
        Determine if this index can serve the given `condition`.

        Args:
            condition: logical expression
        Returns:
            `None` if this index can't serve the condition.
            `IndexLoop` plan if it can.
        """

    @abstractmethod
    def lookup(self, value: Any) -> Set[T]:
        """
        Get members from the index.

        Args:
            value: Attribute value to lookup
        Returns:
            Result set
        """


_NotFound = object()


class HashIndex(Generic[T], Index[T]):
    """
    Hash table index.

    This maps unique object attribute values to sets of objects.

    This matches equality expressions, e.g. `a = 1`

    Args:
        attr: name of the attribute to index
    """

    def __init__(self, attr: str):
        self.attr = attr
        self.idx: Dict[Any, Set[T]] = {}

    def add(self, objs: Set[T], ctx: Context[T]) -> None:
        for o in objs:
            for val in self._extract_values(o, ctx):
                self.idx.setdefault(val, set()).add(o)

    def remove(self, objs: Set[T], ctx: Context[T]) -> None:
        for o in objs:
            for val in self._extract_values(o, ctx):
                self.idx.setdefault(val, set()).remove(o)

    def lookup(self, value: Any) -> Set[T]:
        return self.idx.get(value) or set()

    def match(self, condition: Condition) -> Optional[IndexLookup]:
        value = self._match(condition)
        if value is _NotFound:
            return None
        objs = self.idx.get(value) or set()
        return IndexLookup(index=self, cost=len(objs), value=value)

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        yield ctx.getattr(obj, self.attr)

    def _match(self, condition: Condition) -> Any:
        if isinstance(condition, Eq):
            l, r = condition.left, condition.right
            if isinstance(l, Attribute) and l.name == self.attr and isinstance(r, Literal):
                return r.value
            elif isinstance(r, Attribute) and r.name == self.attr and isinstance(l, Literal):
                return l.value
        return _NotFound

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.attr})"


class MultiHashIndex(Generic[T], HashIndex[T]):
    """
    Same as a `HashIndex`, except this assumes the attribute is a collection of values.

    This matches IN expressions, e.g. `1 in a`
    """

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        for val in ctx.getattr(obj, self.attr):
            yield val

    def _match(self, condition: Condition) -> Any:
        if isinstance(condition, In):
            member, container = condition.left, condition.right
            if isinstance(member, Literal) and isinstance(container, Attribute):
                return member.value
        return _NotFound
