"""
Query plans. These make up the nodes of a query plan.
"""

from abc import abstractmethod
from dataclasses import dataclass
from copy import deepcopy
from typing import List, Any, Callable, TYPE_CHECKING
from typing_extensions import Protocol

from odex.condition import Condition, And, Or

if TYPE_CHECKING:
    from odex.index import Index


Transformer = Callable[["Plan"], "Plan"]


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
