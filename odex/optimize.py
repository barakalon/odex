from typing import Callable, Sequence, Dict, TYPE_CHECKING, Any, List
from typing_extensions import Protocol

from odex.condition import and_, BinOp, Attribute
from odex.context import Context
from odex.plan import Plan, SetOp, Intersect, Filter, ScanFilter, Range, IndexRange, IndexLookup

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
            ranges: Dict[Index, Range] = {}
            others = []

            for i in plan.inputs:
                if isinstance(i, IndexLookup):
                    rng: Range[Any] = Range(
                        left=i.value,
                        right=i.value,
                        left_inclusive=True,
                        right_inclusive=True,
                    )
                    existing = ranges.get(i.index)
                    ranges[i.index] = existing.combine(rng) if existing else rng
                elif isinstance(i, IndexRange):
                    existing = ranges.get(i.index)
                    ranges[i.index] = existing.combine(i.range) if existing else i.range
                else:
                    others.append(i)

            inputs: List[Plan] = []
            for index, rng in ranges.items():
                if rng.left == rng.right and rng.left_inclusive and rng.right_inclusive:
                    inputs.append(IndexLookup(index=index, value=rng.left))
                else:
                    inputs.append(
                        IndexRange(
                            index=index,
                            range=rng,
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
