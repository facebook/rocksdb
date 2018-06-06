# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import os
import unittest
from advisor.rule_parser import RulesSpec, DatabaseLogs, DatabaseOptions
from advisor.rule_parser import get_triggered_rules, trigger_conditions

RuleToSuggestions = {
    "stall-too-many-memtables": [
        'inc-bg-flush',
        'inc-write-buffer'
    ],
    "stall-too-many-L0": [
        'inc-max-subcompactions',
        'inc-max-bg-compactions',
        'inc-write-buffer-size',
        'dec-max-bytes-for-level-base',
        'inc-l0-slowdown-writes-trigger'
    ],
    "stop-too-many-L0": [
        'inc-max-bg-compactions',
        'inc-write-buffer-size',
        'inc-l0-stop-writes-trigger'
    ],
    "stall-too-many-compaction-bytes": [
        'inc-max-bg-compactions',
        'inc-write-buffer-size',
        'inc-hard-pending-compaction-bytes-limit',
        'inc-soft-pending-compaction-bytes-limit'
    ],
    "level0-level1-ratio": [
        'l0-l1-ratio-health-check'
    ]
}


class TestAllRulesTriggered(unittest.TestCase):
    def setUp(self):
        # load the Rules
        this_path = os.path.abspath(os.path.dirname(__file__))
        ini_path = os.path.join(this_path, '../advisor/rules.ini')
        self.db_rules = RulesSpec(ini_path)
        self.db_rules.load_rules_from_spec()
        self.db_rules.perform_section_checks()
        # load the data sources: LOG and OPTIONS
        log_path = os.path.join(this_path, 'input_files/LOG-0')
        options_path = os.path.join(this_path, 'input_files/OPTIONS-000005')
        self.data_sources = []
        self.data_sources.append(DatabaseOptions(options_path))
        self.data_sources.append(DatabaseLogs(log_path))

    def test_triggered_conditions(self):
        conditions_dict = self.db_rules.get_conditions_dict()
        rules_dict = self.db_rules.get_rules_dict()
        # Make sure none of the conditions is triggered beforehand
        for cond in conditions_dict.values():
            self.assertFalse(cond.is_triggered(), repr(cond))
        for rule in rules_dict.values():
            self.assertFalse(rule.is_triggered(conditions_dict), repr(rule))

        # Trigger the conditions as per the data sources.
        trigger_conditions(self.data_sources, conditions_dict)

        # Make sure each condition and rule is triggered
        for cond in conditions_dict.values():
            self.assertTrue(cond.is_triggered(), repr(cond))

        # Get the set of rules that have been triggered
        triggered_rules = get_triggered_rules(rules_dict, conditions_dict)

        for rule in rules_dict.values():
            self.assertIn(rule, triggered_rules)
            # Check the suggestions made by the triggered rules
            for sugg in rule.get_suggestions():
                self.assertIn(sugg, RuleToSuggestions[rule.name])

        for rule in triggered_rules:
            self.assertIn(rule, rules_dict.values())
            for sugg in RuleToSuggestions[rule.name]:
                self.assertIn(sugg, rule.get_suggestions())


class TestConditionsConjunctions(unittest.TestCase):
    def setUp(self):
        # load the Rules
        this_path = os.path.abspath(os.path.dirname(__file__))
        ini_path = os.path.join(this_path, 'input_files/test_rules.ini')
        self.db_rules = RulesSpec(ini_path)
        self.db_rules.load_rules_from_spec()
        self.db_rules.perform_section_checks()
        # load the data sources: LOG and OPTIONS
        log_path = os.path.join(this_path, 'input_files/LOG-1')
        options_path = os.path.join(this_path, 'input_files/OPTIONS-000005')
        self.data_sources = []
        self.data_sources.append(DatabaseOptions(options_path))
        self.data_sources.append(DatabaseLogs(log_path))

    def test_condition_conjunctions(self):
        conditions_dict = self.db_rules.get_conditions_dict()
        rules_dict = self.db_rules.get_rules_dict()
        # Make sure none of the conditions is triggered beforehand
        for cond in conditions_dict.values():
            self.assertFalse(cond.is_triggered(), repr(cond))
        for rule in rules_dict.values():
            self.assertFalse(rule.is_triggered(conditions_dict), repr(rule))

        # Trigger the conditions as per the data sources.
        trigger_conditions(self.data_sources, conditions_dict)

        # Check for the conditions
        conds_triggered = ['log-1-true', 'log-2-true', 'log-3-true']
        conds_not_triggered = ['log-4-false', 'options-1-false']
        for cond in conds_triggered:
            self.assertTrue(conditions_dict[cond].is_triggered(), repr(cond))
        for cond in conds_not_triggered:
            self.assertFalse(conditions_dict[cond].is_triggered(), repr(cond))

        # Check for the rules
        rules_triggered = ['multiple-conds-true']
        rules_not_triggered = [
            'single-condition-false',
            'multiple-conds-one-false',
            'multiple-conds-all-false'
        ]
        for rule in rules_triggered:
            self.assertTrue(
                rules_dict[rule].is_triggered(conditions_dict),
                repr(rule)
            )
        for rule in rules_not_triggered:
            self.assertFalse(
                rules_dict[rule].is_triggered(conditions_dict),
                repr(rule)
            )


class TestSanityChecker(unittest.TestCase):
    def setUp(self):
        this_path = os.path.abspath(os.path.dirname(__file__))
        ini_path = os.path.join(this_path, 'input_files/rules_err1.ini')
        db_rules = RulesSpec(ini_path)
        db_rules.load_rules_from_spec()
        self.rules_dict = db_rules.get_rules_dict()
        self.conditions_dict = db_rules.get_conditions_dict()
        self.suggestions_dict = db_rules.get_suggestions_dict()

    def test_rule_missing_suggestions(self):
        regex = '.*rule must have at least one suggestion.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.rules_dict['missing-suggestions'].perform_checks()

    def test_rule_missing_conditions(self):
        regex = '.*rule must have at least one condition.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.rules_dict['missing-conditions'].perform_checks()

    def test_condition_missing_regex(self):
        regex = '.*provide regex for log condition.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.conditions_dict['missing-regex'].perform_checks()

    def test_condition_missing_options(self):
        regex = '.*options missing in condition.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.conditions_dict['missing-options'].perform_checks()

    def test_condition_missing_expression(self):
        regex = '.*expression missing in condition.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.conditions_dict['missing-expression'].perform_checks()

    def test_suggestion_missing_option(self):
        regex = '.*provide option or description.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.suggestions_dict['missing-option'].perform_checks()

    def test_suggestion_missing_description(self):
        regex = '.*provide option or description.*'
        with self.assertRaisesRegex(ValueError, regex):
            self.suggestions_dict['missing-description'].perform_checks()


class TestParsingErrors(unittest.TestCase):
    def setUp(self):
        self.this_path = os.path.abspath(os.path.dirname(__file__))

    def test_condition_missing_source(self):
        ini_path = os.path.join(self.this_path, 'input_files/rules_err2.ini')
        db_rules = RulesSpec(ini_path)
        regex = '.*provide source for condition.*'
        with self.assertRaisesRegex(ValueError, regex):
            db_rules.load_rules_from_spec()

    def test_suggestion_missing_action(self):
        ini_path = os.path.join(self.this_path, 'input_files/rules_err3.ini')
        db_rules = RulesSpec(ini_path)
        regex = '.*provide action for option.*'
        with self.assertRaisesRegex(ValueError, regex):
            db_rules.load_rules_from_spec()

    def test_section_no_name(self):
        ini_path = os.path.join(self.this_path, 'input_files/rules_err4.ini')
        db_rules = RulesSpec(ini_path)
        regex = 'Parsing error: section header be like:.*'
        with self.assertRaisesRegex(ValueError, regex):
            db_rules.load_rules_from_spec()


if __name__ == '__main__':
    unittest.main()
