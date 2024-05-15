import operator

from typing import (
    TypeVar,
    Set,
    Any,
    Callable,
    Type,
    Dict,
    List,
    Union as UnionType,
    Optional,
    Iterable,
    Iterator,
    MutableSet,
    Sequence,
)

from sqlglot import exp

from odex.index import Index
from odex.optimize import Chain, Rule
from odex.parse import Parser
from odex.plan import Plan, Union, Intersect, ScanFilter, Filter, Planner, IndexLookup
from odex import condition as cond
from odex.condition import BinOp, UnaryOp, Attribute, Literal, Condition
from odex.utils import intersect

T = TypeVar("T")

Attributes = Dict[str, Callable[[T], Any]]


class IndexedSet(MutableSet[T]):
    """
    Unordered, indexed collection of distinct, hashable objects.

    This is intended for efficient filtering of large sets by member attributes.

    Example:
        >>> from collections import namedtuple
        >>> from odex import HashIndex
        >>> X = namedtuple("X", ["a"])
        >>> iset = IndexedSet({X(a=1), X(a=2), X(a=3)}, indexes=[HashIndex("a")])
        >>> iset.filter("a = 2")
        {X(a=2)}

    Args:
        objs: Objects to initialize the set with
            Example: `{X(a=1), X(a=2)}`
        indexes: Attribute indexes for this set.
            Example: `[HashIndex("a")]`
        attrs: Extend members with extra attribute getters.
            Example: `{"a": lambda obj: obj.get_a()}`
        parser: Override the query parser
        planner: Override the query planner
        optimizer: Override the query optimizer
    """

    BINOPS = {
        cond.Add: operator.add,
        cond.Div: operator.truediv,
        cond.FloorDiv: operator.floordiv,
        cond.BitwiseAnd: operator.and_,
        cond.Xor: operator.xor,
        cond.BitwiseOr: operator.or_,
        cond.Pow: operator.pow,
        cond.Is: operator.is_,
        cond.Lshift: operator.lshift,
        cond.Mod: operator.mod,
        cond.Mul: operator.mul,
        cond.Rshift: operator.rshift,
        cond.Sub: operator.sub,
        cond.Lt: operator.lt,
        cond.Le: operator.le,
        cond.Gt: operator.gt,
        cond.Ge: operator.ge,
        cond.Eq: operator.eq,
        cond.Ne: operator.ne,
        cond.And: lambda l, r: l and r,
        cond.Or: lambda l, r: l or r,
        cond.In: lambda l, r: l in r,
    }
    UNARY_OPS = {
        cond.Not: operator.not_,
        cond.Invert: operator.invert,
    }

    def __init__(
        self,
        objs: Optional[Iterable[T]] = None,
        indexes: Optional[Sequence[Index[T]]] = None,
        attrs: Optional[Attributes] = None,
        parser: Optional[Parser] = None,
        planner: Optional[Planner] = None,
        optimizer: Optional[Rule] = None,
    ):
        self.objs = objs if isinstance(objs, set) else set(objs) if objs else set()
        self.planner = planner or Planner()
        self.optimizer = optimizer or Chain()
        self.parser = parser or Parser()
        self.attrs = attrs or {}
        self.indexes: Dict[str, List[Index]] = {}
        for index in indexes or []:
            for attr in index.attributes:
                self.indexes.setdefault(attr, []).append(index)

        self.update(self.objs)

        self.executors: Dict[Type[Plan], Callable[[Plan], Set[T]]] = {
            ScanFilter: lambda plan: {o for o in self.objs if self.match(plan.condition, o)},  # type: ignore
            Filter: lambda plan: {
                o
                for o in self.execute(plan.input)  # type: ignore
                if self.match(plan.condition, o)  # type: ignore
            },
            Union: lambda plan: set.union(*(self.execute(i) for i in plan.inputs)),  # type: ignore
            Intersect: lambda plan: intersect(*(self.execute(i) for i in plan.inputs)),  # type: ignore
            IndexLookup: lambda plan: plan.index.lookup(plan.value),  # type: ignore
        }

        def match_binop(op: Callable[[Any, Any], Any]) -> Callable[[BinOp, T], Any]:
            def matcher(condition: BinOp, obj: T) -> Any:
                return op(self.match(condition.left, obj), self.match(condition.right, obj))

            return matcher

        def match_unaryop(op: Callable[[Any], Any]) -> Callable[[UnaryOp, T], Any]:
            def matcher(condition: UnaryOp, obj: T) -> Any:
                return op(self.match(condition.operand, obj))

            return matcher

        self.matchers: Dict[Type[Condition], Callable[[Condition, T], Any]] = {
            Literal: lambda condition, obj: condition.value,  # type: ignore
            Attribute: lambda condition, obj: self.getattr(obj, condition.name),  # type: ignore
            cond.Array: lambda condition, obj: {self.match(i, obj) for i in condition.items},  # type: ignore
            **{klass: match_binop(op) for klass, op in self.BINOPS.items()},  # type: ignore
            **{klass: match_unaryop(op) for klass, op in self.UNARY_OPS.items()},  # type: ignore
        }

    def filter(self, condition: UnionType[Condition, str, exp.Expression]) -> Set[T]:
        """
        Apply a logical expression to this set, returning a set of the matching members.

        Example:
            >>> iset = IndexedSet({X(a=1), X(a=2), X(a=3)}, indexes=[HashIndex("a")])
            >>> iset.filter("a = 2")
            {X(a=2)}
            >>> iset.filter(attr("a").eq(2))
            {X(a=2)}

        Args:
            condition: logical expression
        Returns:
            Set of matching members
        """
        plan = self.plan(condition)
        plan = self.optimize(plan)
        return self.execute(plan)

    def plan(self, condition: UnionType[Condition, str, exp.Expression]) -> Plan:
        """
        Build a query plan from a condition.

        Args:
            condition: logical expression
        Returns:
            Query plan
        """
        if isinstance(condition, (str, exp.Expression)):
            condition = self.parser.parse(condition)
        return self.planner.plan(condition)

    def optimize(self, plan: Plan) -> Plan:
        """
        Optimize a query plan.

        Args:
            plan: query plan
        Returns:
            Optimized query plan
        """
        return self.optimizer(plan, self)

    def execute(self, plan: Plan) -> Set[T]:
        """
        Execute a query plan.

        Args:
            plan: query plan
        Returns:
            Set of matching members
        """
        executor = self.executors.get(plan.__class__)
        if executor:
            return executor(plan)
        raise ValueError(f"Unsupported plan: {plan}")

    def match(self, condition: Condition, obj: T) -> Any:
        """
        Determine if `obj` matches the given `condition`.

        Args:
            condition: logical expression
            obj: object
        Returns:
            Usually a boolean, but this can return other types based on the condition
        """
        matcher = self.matchers.get(condition.__class__)
        if matcher:
            return matcher(condition, obj)

        raise ValueError(f"Unsupported condition: {condition}")

    def getattr(self, obj: T, item: str) -> Any:
        """Get the attribute `item` from `obj`"""
        attr = self.attrs.get(item)
        if attr:
            return attr(obj)
        return getattr(obj, item)

    def add(self, obj: T) -> None:
        self.objs.add(obj)
        for index in self._iter_indexes():
            index.add({obj}, self)

    def discard(self, obj: T) -> None:
        self.objs.discard(obj)
        for index in self._iter_indexes():
            index.remove({obj}, self)

    def __contains__(self, x: Any) -> bool:
        return x in self.objs

    def __len__(self) -> int:
        return len(self.objs)

    def __iter__(self) -> Iterator[T]:
        for i in self.objs:
            yield i

    def update(self, objs: Set[T]) -> None:
        self.objs.update(objs)
        for index in self._iter_indexes():
            index.add(objs, self)

    def difference_update(self, objs: Set[T]) -> None:
        self.objs.difference_update(objs)
        for index in self._iter_indexes():
            index.remove(objs, self)

    def _iter_indexes(self) -> Iterator[Index]:
        for indexes in self.indexes.values():
            for index in indexes:
                yield index
