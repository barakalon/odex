import doctest
import inspect
import unittest
from collections import namedtuple

import odex
from odex import IndexedSet, HashIndex, literal, attr
from odex.context import SimpleContext


def load_tests(loader, tests, ignore):
    """
    This finds and runs all the doctests
    """

    modules = {mod for module in [odex] for _, mod in inspect.getmembers(module, inspect.ismodule)}

    assert len(modules) >= 1

    X = namedtuple("X", ["a"])

    for module in modules:
        tests.addTests(
            doctest.DocTestSuite(
                module,
                globs={
                    "X": X,
                    "IndexedSet": IndexedSet,
                    "HashIndex": HashIndex,
                    "literal": literal,
                    "attr": attr,
                    "SimpleContext": SimpleContext,
                },
            )
        )

    return tests


if __name__ == "__main__":
    unittest.main()
