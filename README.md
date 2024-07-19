# odex

Python object index for fast, declarative retrieval

## Install

```
pip install odex
```

## Usage

Odex provides a set-like collection called `IndexedSet`:

```python
from odex import IndexedSet, attr, and_

class X:
    def __init__(self, a, b):
        self.a = a
        self.b = b

iset = IndexedSet(
    [
        X(a=1, b=4),
        X(a=2, b=5),
        X(a=2, b=6),
        X(a=3, b=7),
    ], 
    indexes=["a"]
)

# Filter objects with SQL-like expressions:
iset.filter("a = 2 AND b = 5") == {X(a=2, b=5)}

# Or, using the fluent interface:
iset.filter(
    and_(
        attr("a").eq(2),
        attr("b").eq(5)
    )
) == {X(a=2, b=5)}
```

`IndexedSet` maintains indexes on the given attributes. There are three index types:
- `HashIndex` - based on `dict`. Only supports exact value queries (e.g. `a = 1`).
- `SortedDictIndex` - based on [Sorted Containers](https://github.com/grantjenks/python-sortedcontainers). Supports exact value _and_ range queries (e.g. `a > 1`), but has slower updates.
- `InvertedIndex` - based on `dict`, but supports collection attributes and supports queries like `'foo' IN tags`

When attribute names are given as indexes, the index type will be inferred from the given objects. Otherwise, explicit indexes can be given.

## Related projects

- [sqlglot](https://github.com/tobymao/sqlglot) - odex uses sqlglot for expression parsing
- [ducks](https://github.com/manimino/ducks) - similar project with different tradeoffs

## Benchmarks

See [comparisons.ipynb](benchmarks/comparisons.ipynb) for more.
