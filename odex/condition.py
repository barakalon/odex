"""
Logical conditions. These make up the nodes of a syntax tree.
"""

from dataclasses import dataclass
from functools import reduce

from typing import Any, ClassVar, List


class Condition:
    """Base class for all conditions"""

    def and_(self, *others: Any) -> "Condition":
        return and_(self, *others)

    def or_(self, *others: Any) -> "Condition":
        return or_(self, *others)

    def add(self, other: Any) -> "Condition":
        return add(self, other)

    def div(self, other: Any) -> "Condition":
        return div(self, other)

    def floordiv(self, other: Any) -> "Condition":
        return floordiv(self, other)

    def bitwise_and(self, other: Any) -> "Condition":
        return bitwise_and(self, other)

    def xor(self, other: Any) -> "Condition":
        return xor(self, other)

    def bitwise_or(self, other: Any) -> "Condition":
        return bitwise_or(self, other)

    def pow(self, other: Any) -> "Condition":
        return pow(self, other)

    def is_(self, other: Any) -> "Condition":
        return is_(self, other)

    def lshift(self, other: Any) -> "Condition":
        return lshift(self, other)

    def mod(self, other: Any) -> "Condition":
        return mod(self, other)

    def mul(self, other: Any) -> "Condition":
        return mul(self, other)

    def rshift(self, other: Any) -> "Condition":
        return rshift(self, other)

    def sub(self, other: Any) -> "Condition":
        return sub(self, other)

    def lt(self, other: Any) -> "Condition":
        return lt(self, other)

    def le(self, other: Any) -> "Condition":
        return le(self, other)

    def gt(self, other: Any) -> "Condition":
        return gt(self, other)

    def ge(self, other: Any) -> "Condition":
        return ge(self, other)

    def eq(self, other: Any) -> "Condition":
        return eq(self, other)

    def ne(self, other: Any) -> "Condition":
        return ne(self, other)

    def in_(self, other: Any) -> "Condition":
        return in_(self, other)

    def not_(self) -> "Condition":
        return not_(self)

    def invert(self) -> "Condition":
        return invert(self)


@dataclass
class Attribute(Condition):
    """Name of an object attribute"""

    name: str

    def __str__(self) -> str:
        return self.name


@dataclass
class Literal(Condition):
    """Any Python value"""

    value: Any

    def __str__(self) -> str:
        return str(self.value)


@dataclass
class Array(Condition):
    items: List[Condition]

    def __str__(self) -> str:
        items = ", ".join(str(i) for i in self.items)
        return f"({items})"


@dataclass
class BinOp(Condition):
    """Abstract class for binary operators"""

    left: Condition
    right: Condition
    SYMBOL: ClassVar[str] = ""

    def __str__(self) -> str:
        return f"{self.left} {self.SYMBOL} {self.right}"


@dataclass
class Add(BinOp):
    SYMBOL = "+"


@dataclass
class Div(BinOp):
    SYMBOL = "/"


@dataclass
class FloorDiv(BinOp):
    SYMBOL = "//"


@dataclass
class BitwiseAnd(BinOp):
    SYMBOL = "&"


@dataclass
class Xor(BinOp):
    SYMBOL = "^"


@dataclass
class BitwiseOr(BinOp):
    SYMBOL = "|"


@dataclass
class Pow(BinOp):
    SYMBOL = "**"


@dataclass
class Is(BinOp):
    SYMBOL = "is"


@dataclass
class Lshift(BinOp):
    SYMBOL = "<<"


@dataclass
class Mod(BinOp):
    SYMBOL = "%"


@dataclass
class Mul(BinOp):
    SYMBOL = "*"


@dataclass
class Rshift(BinOp):
    SYMBOL = ">>"


@dataclass
class Sub(BinOp):
    SYMBOL = "-"


@dataclass
class Lt(BinOp):
    SYMBOL = "<"


@dataclass
class Le(BinOp):
    SYMBOL = "<="


@dataclass
class Gt(BinOp):
    SYMBOL = ">"


@dataclass
class Ge(BinOp):
    SYMBOL = ">="


@dataclass
class Eq(BinOp):
    SYMBOL = "="


@dataclass
class Ne(BinOp):
    SYMBOL = "!="


@dataclass
class Or(BinOp):
    SYMBOL = "OR"


@dataclass
class And(BinOp):
    SYMBOL = "AND"


@dataclass
class In(BinOp):
    SYMBOL = "IN"


@dataclass
class UnaryOp(Condition):
    """Abstract class for unary operators"""

    operand: Condition
    SYMBOL: ClassVar[str] = ""

    def __str__(self) -> str:
        return f"{self.SYMBOL} {self.operand}"


@dataclass
class Not(UnaryOp):
    SYMBOL = "NOT"


@dataclass
class Invert(UnaryOp):
    SYMBOL = "~"


# Just some sugar for the fluent interface
literal = Literal
attr = Attribute


def ensure_condition(val: Any) -> Condition:
    if isinstance(val, Condition):
        return val
    return Literal(val)


def and_(*conditions: Any) -> Condition:
    """Logically AND multiple conditions"""
    return reduce(lambda l, r: And(l, r), [ensure_condition(c) for c in conditions])


def or_(*conditions: Any) -> Condition:
    """Logically OR multiple conditions"""
    return reduce(lambda l, r: Or(l, r), [ensure_condition(c) for c in conditions])


def add(left: Any, right: Any) -> Condition:
    return Add(ensure_condition(left), ensure_condition(right))


def div(left: Any, right: Any) -> Condition:
    return Div(ensure_condition(left), ensure_condition(right))


def floordiv(left: Any, right: Any) -> Condition:
    return FloorDiv(ensure_condition(left), ensure_condition(right))


def bitwise_and(left: Any, right: Any) -> Condition:
    return BitwiseAnd(ensure_condition(left), ensure_condition(right))


def xor(left: Any, right: Any) -> Condition:
    return Xor(ensure_condition(left), ensure_condition(right))


def bitwise_or(left: Any, right: Any) -> Condition:
    return BitwiseOr(ensure_condition(left), ensure_condition(right))


def pow(left: Any, right: Any) -> Condition:
    return Pow(ensure_condition(left), ensure_condition(right))


def is_(left: Any, right: Any) -> Condition:
    return Is(ensure_condition(left), ensure_condition(right))


def lshift(left: Any, right: Any) -> Condition:
    return Lshift(ensure_condition(left), ensure_condition(right))


def mod(left: Any, right: Any) -> Condition:
    return Mod(ensure_condition(left), ensure_condition(right))


def mul(left: Any, right: Any) -> Condition:
    return Mul(ensure_condition(left), ensure_condition(right))


def rshift(left: Any, right: Any) -> Condition:
    return Rshift(ensure_condition(left), ensure_condition(right))


def sub(left: Any, right: Any) -> Condition:
    return Sub(ensure_condition(left), ensure_condition(right))


def lt(left: Any, right: Any) -> Condition:
    return Lt(ensure_condition(left), ensure_condition(right))


def le(left: Any, right: Any) -> Condition:
    return Le(ensure_condition(left), ensure_condition(right))


def gt(left: Any, right: Any) -> Condition:
    return Gt(ensure_condition(left), ensure_condition(right))


def ge(left: Any, right: Any) -> Condition:
    return Ge(ensure_condition(left), ensure_condition(right))


def eq(left: Any, right: Any) -> Condition:
    return Eq(ensure_condition(left), ensure_condition(right))


def ne(left: Any, right: Any) -> Condition:
    return Ne(ensure_condition(left), ensure_condition(right))


def in_(left: Any, right: Any) -> Condition:
    return In(ensure_condition(left), ensure_condition(right))


def not_(operand: Any) -> Condition:
    return Not(ensure_condition(operand))


def invert(operand: Any) -> Condition:
    return Invert(ensure_condition(operand))
