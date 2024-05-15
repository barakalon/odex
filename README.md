# odex

Python object index for fast, declarative retrieval

## Install

```
pip install odex
```

## Usage

Odex provides a set-like collection called `IndexedSet`:

```python
from collections import namedtuple
from odex import IndexedSet, HashIndex, attr, and_

X = namedtuple("X", ["a", "b"])

iset = IndexedSet(
    [
        X(a=1, b=4),
        X(a=2, b=5),
        X(a=2, b=6),
        X(a=3, b=7),
    ], 
    indexes=[HashIndex("a")]
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

## Related projects

- [sqlglot](https://github.com/tobymao/sqlglot) - odex uses sqlglot for expression parsing
- [ducks](https://github.com/manimino/ducks) - similar project with different tradeoffs

## Benchmarks

See [comparisons.ipynb](benchmarks/comparisons.ipynb) for more.
