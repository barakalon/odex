import os
from copy import deepcopy
from unittest import TestCase

from ruamel.yaml import YAML

from odex.set import IndexedSet
from odex.container import Container
from odex.condition import attr, or_


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

ONLY = ""
OVERWRITE = False


class TestIndexedSet(TestCase):
    def test_e2e(self):
        yaml = YAML()
        fixture_path = os.path.join(FIXTURE_DIR, "e2e.yaml")
        with open(fixture_path) as fp:
            fixture = yaml.load(fp)

        for setup in fixture["setups"]:
            objects = {i: Container(o) for i, o in enumerate(setup["objects"])}
            objects_to_id = {v: k for k, v in objects.items()}

            iset = IndexedSet(set(objects.values()), indexes=setup["indexes"])

            for test in setup["tests"]:
                title = test["title"]
                if ONLY and ONLY != title:
                    continue
                with self.subTest(title):
                    plan = iset.plan(test["condition"])
                    optimized_plan = iset.optimize(deepcopy(plan))
                    result = iset.execute(deepcopy(optimized_plan))

                    if OVERWRITE:
                        test["plan"] = str(plan)
                        test["optimized_plan"] = str(optimized_plan)
                        test["result"] = sorted(objects_to_id[i] for i in result)

                    self.assertEqual(test["plan"].strip(), str(plan).strip())
                    self.assertEqual(test["optimized_plan"].strip(), str(optimized_plan).strip())
                    self.assertEqual({objects[i] for i in test["result"]}, result)

            if OVERWRITE:
                with open(fixture_path, "w", encoding="utf-8") as fp:
                    yaml.dump(fixture, fp)

    def test_set_abc(self):
        a = IndexedSet([1, 2, 3])
        b = IndexedSet([2, 3, 4])
        self.assertEqual(IndexedSet([2, 3]), a & b)
        self.assertEqual(IndexedSet([1, 2, 3, 4]), a | b)
        self.assertIn(2, a)
        self.assertNotIn(4, a)

    def test_fluent_interface(self):
        objs = [
            Container({"x": 1, "y": 1}),
            Container({"x": 2, "y": 2}),
            Container({"x": 3, "y": 3}),
        ]
        iset = IndexedSet(objs)

        self.assertEqual(
            {objs[0], objs[1]},
            iset.filter(
                or_(
                    attr("x").eq(1).and_(attr("y").in_([1, 2])),
                    attr("x").eq(2),
                )
            ),
        )
        self.assertEqual(
            {objs[0], objs[1]},
            iset.filter(attr("x").eq(3).not_()),
        )
