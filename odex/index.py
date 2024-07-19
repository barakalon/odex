from abc import abstractmethod
from typing import Generic, TypeVar, Set, Any, Optional, Iterable, List, cast, Dict, Type, Callable
from typing_extensions import Protocol

from sortedcontainers import SortedDict  # type: ignore

from odex.condition import Condition, Eq, Literal, In, BinOp, Array, Le, Lt, Ge, Gt
from odex.context import Context
from odex.plan import Plan, IndexLookup, Union, Range, IndexRange, Bound, Unset

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
    def match(self, condition: BinOp, operand: Condition) -> Optional[Plan]:
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

    @abstractmethod
    def range(self, rng: Range) -> Set[T]:
        """
        Get members from the index base on a range of values.

        Args:
            rng: Range
        Returns:
            Result set
        """


class HashIndex(Generic[T], Index[T]):
    """
    Hash table index.

    This maps unique object attribute values to sets of objects.

    This matches equality expressions, e.g. `a = 1`

    Args:
        attr: name of the attribute to index
    """

    idx: dict

    def __init__(self, attr: str):
        self.attr = attr
        self.attributes = [attr]
        self.idx = self._create_index()

    def _create_index(self) -> dict:
        return {}

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

    def range(self, rng: Range) -> Set[T]:
        raise ValueError(f"{self.__class__.__name__} does not support range queries")

    def match(self, condition: BinOp, operand: Condition) -> Optional[Plan]:
        if isinstance(condition, Eq) and isinstance(operand, Literal):
            return IndexLookup(index=self, value=operand.value)
        if (
            isinstance(condition, In)
            and operand is condition.right
            and isinstance(operand, Array)
            and all(isinstance(i, Literal) for i in operand.items)
        ):
            return Union(
                inputs=[
                    IndexLookup(index=self, value=cast(Literal, i).value) for i in operand.items
                ]
            )
        return None

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        yield ctx.getattr(obj, self.attr)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.attr})"


class SortedDictIndex(Generic[T], HashIndex[T]):
    """
    Same as `HashIndex`, except this uses a `sortedcontainers.SortedDict` as the index
    and supports range queries.
    """

    idx: SortedDict

    INVERSE_COMPARISONS: Dict[Type[BinOp], Type[BinOp]] = {
        Lt: Gt,
        Gt: Lt,
        Le: Ge,
        Ge: Le,
    }
    COMPARISONS = tuple(INVERSE_COMPARISONS)
    COMPARISON_RANGES: Dict[Type[BinOp], Callable[[Any], Range]] = {
        Lt: lambda val: Range(right=Bound(val, False)),
        Gt: lambda val: Range(left=Bound(val, False)),
        Le: lambda val: Range(right=Bound(val, True)),
        Ge: lambda val: Range(left=Bound(val, True)),
    }

    def _create_index(self) -> SortedDict:
        return SortedDict()

    def range(self, rng: Range[Any]) -> Set[T]:
        left = 0
        right = None
        if not isinstance(rng.left, Unset):
            if rng.left.inclusive:
                left = self.idx.bisect_left(rng.left.value)
            else:
                left = self.idx.bisect_right(rng.left.value)
        if not isinstance(rng.right, Unset):
            if rng.right.inclusive:
                right = self.idx.bisect_right(rng.right.value)
            else:
                right = self.idx.bisect_left(rng.right.value)

        groups = self.idx.values()[left:right]
        return set.union(*groups) if groups else set()

    def match(self, condition: BinOp, operand: Condition) -> Optional[Plan]:
        if isinstance(condition, self.COMPARISONS) and isinstance(operand, Literal):
            comparison: Type[BinOp] = type(condition)
            if operand is condition.left:
                comparison = self.INVERSE_COMPARISONS.get(comparison, comparison)
            return IndexRange(index=self, range=self.COMPARISON_RANGES[comparison](operand.value))
        return super().match(condition, operand)


class InvertedIndex(Generic[T], HashIndex[T]):
    """
    Same as a `HashIndex`, except this assumes the attribute is a collection of values.

    This matches IN expressions, e.g. `1 in a`
    """

    def _extract_values(self, obj: T, ctx: Context[T]) -> Iterable[Any]:
        for val in ctx.getattr(obj, self.attr):
            yield val

    def match(self, condition: BinOp, operand: Condition) -> Optional[Plan]:
        if isinstance(condition, In) and operand is condition.left and isinstance(operand, Literal):
            return IndexLookup(index=self, value=operand.value)
        return None
