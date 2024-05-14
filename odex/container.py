from typing import Generic, TypeVar

T = TypeVar("T")


class Container(Generic[T]):
    """
    Proxy object that wraps another object.

    This is intended for a convenience to wrap objects that aren't hashable, e.g. `dict`s.
    Container objects are hashed by `id`.

    Example:
        >>> from odex.container import Container
        >>> container = Container({"a": 1})
        >>> container.a
        1
        >>> hash(Container({"a": 1})) != hash(container)
        True
    """

    def __init__(self, obj: T):
        self.obj = obj

    def __getattr__(self, item):
        try:
            return getattr(self.obj, item)
        except AttributeError:
            return self.obj[item]
