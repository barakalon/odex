"""
Query plans. These make up the nodes of a query plan.
"""

from abc import abstractmethod
from dataclasses import dataclass
from copy import deepcopy
from typing import List, Any, Callable, TYPE_CHECKING, Union as UnionType, Tuple, Generic, TypeVar
from typing_extensions import Protocol

from odex.condition import Condition, And, Or

if TYPE_CHECKING:
    from odex.index import Index


Transformer = Callable[["Plan"], "Plan"]
T = TypeVar("T")


class Unset:
    def __repr__(self):
        return "UNSET"


UNSET = Unset()


class Comparable(Protocol):
    @abstractmethod
    def __lt__(self: "C", other: "C") -> bool:
        pass

    @abstractmethod
    def __gt__(self: "C", other: "C") -> bool:
        pass


C = TypeVar("C", bound=Comparable)


class Plan(Protocol):
    """Base class for all query plan nodes"""

    def __str__(self) -> str:
        return self.to_s()

    @abstractmethod
    def to_s(self, depth: int = 0) -> str:
        """Render this plan as a string"""

    def transform_inputs(self, transformer: Transformer) -> None:
        """
        Recursively transform any child plans

        Args:
            transformer: function that transforms plan nodes and returns a new node
        Returns:
            nothing - this should happen in-place
        """
        return None

    def transform(self, transformer: Transformer) -> "Plan":
        """
        Recursively transform this plan using the `transformer` function.

        Args:
            transformer: function that transforms this plan node and returns a new node
        Returns:
            new plan
        """
        self.transform_inputs(transformer)
        return transformer(self)


@dataclass
class ScanFilter(Plan):
    """Return all objects in the collection, filtering with `condition`"""

    condition: Condition

    def to_s(self, depth=0):
        return f"ScanFilter: {self.condition}"


@dataclass
class Filter(Plan):
    """Return all objects return by `input`, filtering with `condition`"""

    condition: Condition
    input: Plan

    def to_s(self, depth=0):
        indent = "  " * depth
        return f"Filter: {self.condition}\n{indent}  - {self.input.to_s(depth + 1)}"

    def transform_inputs(self, transformer: Transformer) -> None:
        self.input = self.input.transform(transformer)


@dataclass
class SetOp(Plan):
    """Base class for set operations"""

    inputs: List[Plan]

    def to_s(self, depth=0):
        indent = "  " * depth
        inputs = "\n".join([f"{indent}  - {i.to_s(depth+1)}" for i in self.inputs])
        return f"{self.__class__.__name__}\n{inputs}"

    def transform_inputs(self, transformer: Transformer) -> None:
        self.inputs = [i.transform(transformer) for i in self.inputs]


@dataclass
class Intersect(SetOp):
    """Return the intersection of all `inputs`"""


@dataclass
class Union(SetOp):
    """Return the union of all `inputs`"""


@dataclass(frozen=True)
class Range(Generic[C]):
    left: UnionType[C, Unset] = UNSET
    left_inclusive: bool = True
    right: UnionType[C, Unset] = UNSET
    right_inclusive: bool = True

    def combine(self, other: "Range[C]") -> "Range[C]":
        left, left_inclusive = self._combine_comparison(
            self.left, self.left_inclusive, other.left, other.left_inclusive, lambda a, b: a > b
        )
        right, right_inclusive = self._combine_comparison(
            self.right, self.right_inclusive, other.right, other.right_inclusive, lambda a, b: a < b
        )

        return Range(
            left=left,
            left_inclusive=left_inclusive,
            right=right,
            right_inclusive=right_inclusive,
        )

    def _combine_comparison(
        self,
        a: UnionType[C, Unset],
        a_inclusive: bool,
        b: UnionType[C, Unset],
        b_inclusive: bool,
        compare: Callable[[C, C], bool],
    ) -> Tuple[UnionType[C, Unset], bool]:
        if isinstance(a, Unset):
            return b, b_inclusive
        elif isinstance(b, Unset):
            return a, a_inclusive
        elif a == b:
            return a, a_inclusive and b_inclusive
        elif compare(a, b):
            return a, a_inclusive
        else:
            return b, b_inclusive


@dataclass
class IndexRange(Plan):
    index: "Index"
    range: Range[Any]

    def to_s(self, depth=0):
        left_symbol = "<=" if self.range.left_inclusive else "<"
        right_symbol = "<=" if self.range.right_inclusive else "<"
        if self.range.left is UNSET:
            return f"IndexRange: {self.index} {right_symbol} {self.range.right}"
        if self.range.right is UNSET:
            return f"IndexRange: {self.range.left} {left_symbol} {self.index}"
        return f"IndexRange: {self.range.left} {left_symbol} {self.index} {right_symbol} {self.range.right}"

    def __deepcopy__(self, memodict):
        return IndexRange(
            index=self.index,
            range=self.range,
        )


@dataclass
class IndexLookup(Plan):
    """Return objects by looking up `value` in `index`"""

    index: "Index"
    value: Any

    def to_s(self, depth=0):
        return f"IndexLookup: {self.index} = {self.value}"

    def __deepcopy__(self, memodict):
        return IndexLookup(index=self.index, value=deepcopy(self.value))


class Planner:
    def plan(self, condition: Condition) -> Plan:
        """
        Convert a syntax tree into a query plan.

        Args:
            condition: logical expression syntax tree
        Returns:
            query plan
        """
        if isinstance(condition, And):
            return Intersect(inputs=[self.plan(condition.left), self.plan(condition.right)])
        if isinstance(condition, Or):
            return Union(inputs=[self.plan(condition.left), self.plan(condition.right)])
        return ScanFilter(condition=condition)
