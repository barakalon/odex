from typing import TypeVar, Set

T = TypeVar("T")


def intersect(*sets: Set[T]) -> Set[T]:
    """
    Find the intersection of all `sets`.

    Set intersection is O(smaller size), so this orders by size.
    """
    return set.intersection(*sorted(sets, key=lambda s: len(s)))
