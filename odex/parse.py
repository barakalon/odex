from typing import Callable, TypeVar, Dict, Type, cast, Optional

from odex import condition as cond
from odex.condition import Condition, Literal, Attribute, BinOp, UnaryOp
from sqlglot import Dialect, exp

B = TypeVar("B", bound=BinOp)
U = TypeVar("U", bound=UnaryOp)


class Converter:
    """Convert sqlglot Expressions to odex Conditions"""

    def __init__(self) -> None:
        self.converters: Dict[Type[exp.Expression], Callable[[exp.Expression], Condition]] = {
            exp.Add: lambda e: self._convert_binary(cond.Add, e),
            exp.Div: lambda e: self._convert_binary(cond.Div, e),
            exp.IntDiv: lambda e: self._convert_binary(cond.FloorDiv, e),
            exp.BitwiseAnd: lambda e: self._convert_binary(cond.BitwiseAnd, e),
            exp.BitwiseXor: lambda e: self._convert_binary(cond.Xor, e),
            exp.BitwiseOr: lambda e: self._convert_binary(cond.BitwiseOr, e),
            exp.Pow: lambda e: self._convert_binary(cond.Pow, e),
            exp.Is: lambda e: self._convert_binary(cond.Is, e),
            exp.BitwiseLeftShift: lambda e: self._convert_binary(cond.Lshift, e),
            exp.Mod: lambda e: self._convert_binary(cond.Mod, e),
            exp.Mul: lambda e: self._convert_binary(cond.Mul, e),
            exp.BitwiseRightShift: lambda e: self._convert_binary(cond.Rshift, e),
            exp.Sub: lambda e: self._convert_binary(cond.Sub, e),
            exp.LT: lambda e: self._convert_binary(cond.Lt, e),
            exp.LTE: lambda e: self._convert_binary(cond.Le, e),
            exp.GT: lambda e: self._convert_binary(cond.Gt, e),
            exp.GTE: lambda e: self._convert_binary(cond.Ge, e),
            exp.EQ: lambda e: self._convert_binary(cond.Eq, e),
            exp.NEQ: lambda e: self._convert_binary(cond.Ne, e),
            exp.And: lambda e: self._convert_binary(cond.And, e),
            exp.Or: lambda e: self._convert_binary(cond.Or, e),
            exp.Not: lambda e: self._convert_unary(cond.Not, e),
            exp.BitwiseNot: lambda e: self._convert_unary(cond.Invert, e),
            exp.Literal: self._convert_literal,
            exp.Column: self._convert_column,
            exp.In: self._convert_in,
            exp.Null: lambda e: Literal(None),
            exp.Boolean: lambda e: Literal(e.this),
            exp.Paren: lambda e: self.convert(e.this),
        }

    def convert(self, expression: exp.Expression) -> Condition:
        converter = self.converters.get(expression.__class__)
        if not converter:
            raise ValueError(f"Unsupported sqlglot Expression: {expression.__class__}")
        return converter(expression)

    def _convert_binary(self, condition_type: Type[B], expression: exp.Expression) -> B:
        expression = cast(exp.Binary, expression)
        return condition_type(
            self.convert(expression.left),
            self.convert(expression.right),
        )

    def _convert_unary(self, condition_type: Type[U], expression: exp.Expression) -> U:
        return condition_type(
            self.convert(expression.this),
        )

    def _convert_literal(self, expression: exp.Expression) -> Literal:
        expression = cast(exp.Literal, expression)
        if expression.is_string:
            return Literal(value=expression.this)
        if expression.is_int:
            return Literal(value=int(expression.this))
        if expression.is_number:
            return Literal(value=float(expression.this))
        raise ValueError(f"Unsupported sqlglot Literal: {expression}")

    def _convert_column(self, expression: exp.Expression) -> Attribute:
        expression = cast(exp.Column, expression)
        return Attribute(name=expression.name)

    def _convert_in(self, expression: exp.Expression) -> cond.In:
        expression = cast(exp.In, expression)
        field = expression.args.get("field")
        if field:
            return cond.In(left=self.convert(expression.this), right=Attribute(name=field.name))
        raise ValueError(f"Unsupported sqlglot In: {expression}")


class Parser:
    """Parse SQL-like logical expressions into odex Conditions"""

    def __init__(self, dialect: Optional[Dialect] = None, converter: Optional[Converter] = None):
        self.dialect = dialect or Dialect()
        self.converter = converter or Converter()

    def parse(self, expression: str) -> Condition:
        ast = self.dialect.parse_into(exp.Condition, expression)[0]
        if not ast:
            raise ValueError(f"Failed to parse expression: {expression}")
        return self.converter.convert(ast)
