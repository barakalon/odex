from collections import defaultdict
from typing import Callable, Sequence, Dict, TYPE_CHECKING, List, Union as UnionType
from typing_extensions import Protocol

from odex.condition import and_, BinOp, Attribute
from odex.context import Context
from odex.plan import (
    Plan,
    SetOp,
    Intersect,
    Filter,
    ScanFilter,
    Range,
    IndexRange,
    IndexLookup,
    Bound,
    Empty,
)

if TYPE_CHECKING:
    from odex.index import Index


class Rule(Protocol):
    def __call__(self, plan: Plan, ctx: Context) -> Plan: ...


class TransformerWithContext:
    """
    Implementation of the Plan Transformer protocol, including Context
    """

    def __init__(self, ctx: Context, func: Callable[[Plan, Context], Plan]):
        self.ctx = ctx
        self.func = func

    def __call__(self, plan: Plan) -> Plan:
        return self.func(plan, self.ctx)


class TransformerRule(Rule):
    """Abstract class for optimizer rules that call `Plan.transform`"""

    def __call__(self, plan: Plan, ctx: Context) -> Plan:
        return plan.transform(TransformerWithContext(ctx, self.transform))

    def transform(self, plan: Plan, ctx: Context) -> Plan:
        return plan


class MergeSetOps(TransformerRule):
    """Merge nested set operations into a single operation"""

    def transform(self, plan: Plan, ctx: Context) -> Plan:
        if isinstance(plan, SetOp):
            new_inputs = []
            for input in plan.inputs:
                if isinstance(input, plan.__class__):
                    new_inputs.extend(input.inputs)
                else:
                    new_inputs.append(input)
            plan.inputs = new_inputs
        return plan


class UseIndex(TransformerRule):
    """Replace scans with index lookups"""

    def transform(self, plan: Plan, ctx: Context) -> Plan:
        if isinstance(plan, ScanFilter):
            condition = plan.condition
            if isinstance(condition, BinOp):
                l, r = condition.left, condition.right

                if isinstance(l, Attribute) and not isinstance(r, Attribute):
                    name, value = l.name, r
                elif isinstance(r, Attribute) and not isinstance(l, Attribute):
                    name, value = r.name, l
                else:
                    return plan

                for idx in ctx.indexes.get(name) or []:
                    match = idx.match(condition, value)
                    if match:
                        return match

        return plan


class CombineRanges(TransformerRule):
    """Combine multiple ranges into one"""

    def transform(self, plan: Plan, ctx: Context) -> Plan:
        if isinstance(plan, Intersect):
            # Group the plans by ones that support ranges and by index
            plans_by_index: Dict[Index, List[UnionType[IndexLookup, IndexRange]]] = defaultdict(
                list
            )
            others = []
            for i in plan.inputs:
                if isinstance(i, (IndexLookup, IndexRange)):
                    plans_by_index[i.index].append(i)
                else:
                    others.append(i)

            inputs: List[Plan] = []

            for index, plans in plans_by_index.items():
                if len(plans) == 1:
                    inputs.append(plans[0])
                    continue

                ranges = [
                    # Treat a lookup as a range
                    Range(
                        left=Bound(i.value, True),
                        right=Bound(i.value, True),
                    )
                    if isinstance(i, IndexLookup)
                    else i.range
                    for i in plans
                ]

                new_range = ranges[0]
                for rng in ranges[1:]:
                    combined = new_range.combine(rng)

                    # None means there is a range that always evaluates to False
                    if combined is None:
                        return Empty()
                    else:
                        new_range = combined

                inputs.append(
                    IndexRange(
                        index=index,
                        range=new_range,
                    )
                )

            inputs.extend(others)

            if len(inputs) == 1:
                return inputs[0]
            return Intersect(inputs=inputs)

        return plan


class CombineFilters(TransformerRule):
    """Combine multiple filters into one"""

    def transform(self, plan: Plan, ctx: Context) -> Plan:
        if isinstance(plan, Intersect):
            others = []
            filters = []
            for i in plan.inputs:
                if isinstance(i, ScanFilter):
                    filters.append(i)
                else:
                    others.append(i)

            if filters:
                combined = and_(*(f.condition for f in filters))
                if others:
                    base = others[0] if len(others) == 1 else Intersect(inputs=others)
                    return Filter(
                        condition=combined,
                        input=base,
                    )
                else:
                    return ScanFilter(
                        condition=combined,
                    )
        return plan


class Chain(Rule):
    """Chain multiple rules together"""

    DEFAULT_RULES = (MergeSetOps(), UseIndex(), CombineRanges(), CombineFilters())

    def __init__(self, rules: Sequence[Rule] = DEFAULT_RULES):
        self.rules = list(rules)

    def __call__(self, plan: Plan, ctx: Context) -> Plan:
        for rule in self.rules:
            plan = rule(plan, ctx)
        return plan
