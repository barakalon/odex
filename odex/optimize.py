from typing import Dict, Type, Callable, Sequence, cast
from typing_extensions import Protocol

from odex.condition import and_
from odex.context import Context
from odex.plan import Plan, SetOp, Intersect, Filter, ScanFilter, Union, IndexLookup


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
            best_plan: Plan = plan
            best_cost = len(ctx.objs)
            for idx in ctx.indexes:
                match = idx.match(plan.condition)
                if match:
                    if best_cost is None or match.cost <= best_cost:
                        best_plan, best_cost = match, match.cost
            return best_plan

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


class OrderIntersects(Rule):
    """Reorder intersections so that the plans with the smallest cost are first"""

    def __init__(self) -> None:
        self.estimators: Dict[Type[Plan], Callable[[Plan, Context], int]] = {
            ScanFilter: lambda plan, ctx: len(ctx.objs),
            Filter: lambda plan, ctx: self._estimate(cast(Filter, plan).input, ctx),
            IndexLookup: lambda plan, ctx: cast(IndexLookup, plan).cost,
            Union: lambda plan, ctx: max(self._estimate(i, ctx) for i in cast(Union, plan).inputs),
            Intersect: self._estimate_intersect,
        }

    def __call__(self, plan: Plan, ctx: Context) -> Plan:
        self._estimate(plan, ctx)
        return plan

    def _estimate(self, plan: Plan, ctx: Context) -> int:
        estimator = self.estimators.get(plan.__class__)
        return estimator(plan, ctx) if estimator else len(ctx.objs)

    def _estimate_intersect(self, plan: Plan, ctx: Context) -> int:
        plan = cast(Intersect, plan)
        costs = sorted(((self._estimate(i, ctx), i) for i in plan.inputs), key=lambda t: t[0])
        plan.inputs = [i[1] for i in costs]
        return costs[0][0]


class Chain(Rule):
    """Chain multiple rules together"""

    DEFAULT_RULES = (MergeSetOps(), UseIndex(), CombineFilters(), OrderIntersects())

    def __init__(self, rules: Sequence[Rule] = DEFAULT_RULES):
        self.rules = list(rules)

    def __call__(self, plan: Plan, ctx: Context) -> Plan:
        for rule in self.rules:
            plan = rule(plan, ctx)
            # print(rule)
            # print(plan)
        return plan
