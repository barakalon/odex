from abc import abstractmethod
from typing import Generic, TypeVar, Set, Any, Optional, Iterable, Dict, List
from typing_extensions import Protocol

from odex.condition import Condition, Eq, Literal, In, BinOp
from odex.context import Context
from odex.plan import IndexLookup

T = TypeVar("T")


class Index(Protocol[T]):
    attributes: List[str]

    @abstractmethod
    def add(self, objs: Set[T], ctx: Context[T]) -> None:
        """Add `objs` to the index"""

    @abstractmethod
    def remove(self, objs: Set[T], ctx: Context[T]) -> None:
        """Remove `objs` from the index"""

    @abstractmethod
    def match(self, condition: BinOp, operand: Condition) -> "Optional[IndexLookup]":
        """
        Determine if this index can serve the given `condition`.

        This assumes the optimizer has already found which side of the condition is the attribute.

        Args:
            condition: the entire binary operator
            operand: the side of the binary operator opposite the attribute
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
        self.attributes = [attr]
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

    def match(self, condition: BinOp, operand: Condition) -> "Optional[IndexLookup]":
        value = self._match(condition, operand)
        if value is _NotFound:
            return None
        return IndexLookup(index=self, value=value)

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        yield ctx.getattr(obj, self.attr)

    def _match(self, condition: BinOp, operand: Condition) -> Any:
        if isinstance(condition, Eq) and isinstance(operand, Literal):
            return operand.value
        return _NotFound

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.attr})"


class InvertedIndex(Generic[T], HashIndex[T]):
    """
    Same as a `HashIndex`, except this assumes the attribute is a collection of values.

    This matches IN expressions, e.g. `1 in a`
    """

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        for val in ctx.getattr(obj, self.attr):
            yield val

    def _match(self, condition: BinOp, operand: Condition) -> Any:
        if isinstance(condition, In) and operand is condition.left and isinstance(operand, Literal):
            return operand.value
        return _NotFound
