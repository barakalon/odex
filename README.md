# odex

Python object index for fast, declarative retrieval.

## Install

```
pip install odex
```

## Usage

Odex provides a set-like collection called `IndexedSet`:

```python
from odex import IndexedSet, HashIndex, attr, and_

class X:
    def __init__(self, a, b):
        self.a = a
        self.b = b

objs = [
    X(a=1, b=4),
    X(a=2, b=5),
    X(a=2, b=6),
    X(a=3, b=7),
]
        
iset = IndexedSet(objs, indexes=[HashIndex("a")])

# Filter objects with SQL-like expressions:
assert iset.filter("a = 2 AND b = 5") == {objs[1]}

# Or, using the fluent interface:
assert iset.filter(
    and_(
        attr("a").eq(2),
        attr("b").eq(5)
    )
) == {objs[1]}
```

## Related projects

- [sqlglot](https://github.com/tobymao/sqlglot) - odex uses sqlglot for expression parsing
- [ducks](https://github.com/manimino/ducks) - similar project with different tradeoffs

## Benchmarks

See [comparisons.ipynb](benchmarks/comparisons.ipynb) for more.
