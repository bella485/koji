
from __future__ import absolute_import
from __future__ import print_function
import unittest

from koji.policy import BaseSimpleTest, AllTest, SimpleRuleSet, RuleChecker


ruleset = """

mammal && goes moo :: cow
mammal && goes meow :: cat
mammal && flies :: {
    goes moo :: flying cow
    goes moo !! bat
}

mammal !! {
    flies :: {
        goes squish :: bug
        goes squawk chirp :: bird
    }
    flies !! {
        goes splish splash :: fish
        goes squish :: creepy crawly bug
        all :: some other thing
    }
}

"""


class PolicyTestMammal(BaseSimpleTest):

    name = "mammal"

    def run(self, data):
        critter = data['critter']
        return critter.mammal


class PolicyTestFlies(BaseSimpleTest):

    name = "flies"

    def run(self, data):
        critter = data['critter']
        return critter.flies


class PolicyTestGoes(BaseSimpleTest):

    name = "goes"

    def __init__(self, args):
        BaseSimpleTest.__init__(self, args)
        self.noises = args.strip().split()[1:]

    def run(self, data):
        critter = data['critter']
        return critter.goes in self.noises


class critter(object):
    def __init__(self, mammal=True, goes="moo", flies=False):
        self.mammal = mammal
        self.goes = goes
        self.flies = flies


class TestBasicTests(unittest.TestCase):

    def setUp(self):
        from koji.policy import findSimpleTests
        tests = findSimpleTests(globals())
        self.rules = SimpleRuleSet(ruleset.splitlines(), tests)

    test_params = [
        # [result, critter kwds]
        [['cow'],
         {}],  # default critter
        [['cat'],
         {'mammal': True, 'goes': 'meow', 'flies': False}],
        [['cow', 'flying cow'],
         {'mammal': True, 'goes': 'moo', 'flies': True}],
        [['fish', 'some other thing'],
         {'mammal': False, 'goes': 'splash', 'flies': False}],
        [['bug'],
         {'mammal': False, 'goes': 'squish', 'flies': True}],
        [['bird'],
         {'mammal': False, 'goes': "chirp", 'flies': True}],
        [['bat'],
         {'mammal': True, 'goes': "squish", 'flies': True}],
        [['some other thing'],
         {'mammal': False, 'goes': "thud", 'flies': False}],
        [[],  # no matching rules for dog
         {'mammal': True, 'goes': 'woof', 'flies': False}],
    ]

    def test_basic_multipolicy(self):
        for expected, kwds in self.test_params:
            data = {'critter': critter(**kwds)}
            checker = RuleChecker(self.rules, data)

            first = checker.apply(multi=False)
            self.assertEqual(checker.lastrun['multi'], False)

            results = checker.apply(multi=True)

            print("all matches:")
            for result in checker.lastrun['results']:
                print("  ", checker.pretty_trace(result))

            # check that single result matches first result of multi run
            self.assertEqual(checker.lastrun['multi'], True)
            if first:
                self.assertEqual(first, results[0])
            else:
                self.assertEqual([], results)

            self.assertEqual(results, expected)


#
# The end.
