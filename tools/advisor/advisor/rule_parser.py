# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import ABC, abstractmethod
import argparse
from advisor.db_log_parser import DatabaseLogs, DataSource
from advisor.db_options_parser import DatabaseOptions
from enum import Enum
from advisor.ini_parser import IniParser


class Section(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def set_parameter(self, key, value):
        pass

    @abstractmethod
    def perform_checks(self):
        pass


class Rule(Section):
    def __init__(self, name):
        super().__init__(name)
        self.conditions = None
        self.suggestions = None

    def set_parameter(self, key, value):
        # If the Rule is associated with a single suggestion/condition, then
        # value will be a string and not a list. Hence, convert it to a single
        # element list before storing it in self.suggestions or
        # self.conditions.
        if key == 'conditions':
            if isinstance(value, str):
                self.conditions = [value]
            else:
                self.conditions = value
        elif key == 'suggestions':
            if isinstance(value, str):
                self.suggestions = [value]
            else:
                self.suggestions = value

    def get_suggestions(self):
        return self.suggestions

    def perform_checks(self):
        if not self.conditions or len(self.conditions) < 1:
            raise ValueError(
                self.name + ': rule must have at least one condition'
            )
        if not self.suggestions or len(self.suggestions) < 1:
            raise ValueError(
                self.name + ': rule must have at least one suggestion'
            )

    def is_triggered(self, conditions_dict):
        condition_triggers = []
        for cond in self.conditions:
            condition_triggers.append(conditions_dict[cond].is_triggered())
        return all(condition_triggers)

    def __repr__(self):
        # Append conditions
        rule_string = "Rule: " + self.name + " has conditions:: "
        is_first = True
        for cond in self.conditions:
            if is_first:
                rule_string += cond
                is_first = False
            else:
                rule_string += (" AND " + cond)
        # Append suggestions
        rule_string += "\nsuggestions:: "
        is_first = True
        for sugg in self.suggestions:
            if is_first:
                rule_string += sugg
                is_first = False
            else:
                rule_string += (", " + sugg)
        # Return constructed string
        return rule_string


class Suggestion(Section):
    class Action(Enum):
        set = 1
        increase = 2
        decrease = 3

    def __init__(self, name):
        super().__init__(name)
        self.option = None
        self.action = None
        self.suggested_value = None
        self.description = None

    def set_parameter(self, key, value):
        if key == 'option':
            self.option = value
        elif key == 'action':
            if self.option and not value:
                raise ValueError(self.name + ': provide action for option')
            self.action = self.Action[value]
        elif key == 'suggested_value':
            self.suggested_value = value
        elif key == 'description':
            self.description = value

    def perform_checks(self):
        if not self.description:
            if not self.option:
                raise ValueError(self.name + ': provide option or description')
            if not self.action:
                raise ValueError(self.name + ': provide action for option')
            if self.action is self.Action.set and not self.suggested_value:
                raise ValueError(
                    self.name + ': provide suggested value for option'
                )

    def __repr__(self):
        if self.description:
            return self.description
        sugg_string = ""
        if self.action is self.Action.set:
            sugg_string = (
                self.name + ' suggests setting ' + self.option +
                ' to ' + self.suggested_value
            )
        else:
            sugg_string = self.name + ' suggests ' + self.action.name + ' in '
            sugg_string += (self.option + '.')
            if self.suggested_value:
                sugg_string += (
                    ' The suggested value is ' + self.suggested_value
                )
        return sugg_string


class Condition(Section):
    def __init__(self, name):
        # a rule is identified by its name, so there should be no duplicates
        super().__init__(name)
        self.data_source = None
        self.trigger = None

    def perform_checks(self):
        if not self.data_source:
            raise ValueError(self.name + ': condition not tied to data source')

    def set_data_source(self, data_source):
        self.data_source = data_source

    def get_data_source(self):
        return self.data_source

    def reset_trigger(self):
        self.trigger = None

    def set_trigger(self, condition_trigger):
        self.trigger = condition_trigger

    def is_triggered(self):
        if self.trigger:
            return True
        return False

    def set_parameter(self, key, value):
        # must be defined by the subclass
        raise ValueError(self.name + ': provide source for condition')


class LogCondition(Condition):
    @classmethod
    def create(cls, base_condition):
        base_condition.set_data_source(DataSource.Type['LOG'])
        base_condition.__class__ = cls
        return base_condition

    class Scope(Enum):
        database = 1
        column_family = 2

    def set_parameter(self, key, value):
        if key == 'regex':
            self.regex = value
        elif key == 'scope':
            self.scope = self.Scope[value]

    def perform_checks(self):
        super().perform_checks()
        if not self.regex:
            raise ValueError(self.name + ': provide regex for log condition')

    def __repr__(self):
        log_cond_str = (
            self.name + ' checks if the regex ' + self.regex + ' is found ' +
            ' in the LOG file in the scope of ' + self.scope.name
        )
        return log_cond_str


class OptionCondition(Condition):
    @classmethod
    def create(cls, base_condition):
        base_condition.set_data_source(DataSource.Type['DB_OPTIONS'])
        base_condition.__class__ = cls
        return base_condition

    def set_parameter(self, key, value):
        if key == 'options':
            self.options = value
        if key == 'evaluate':
            self.eval_expr = value

    def perform_checks(self):
        super().perform_checks()
        if not self.options:
            raise ValueError(self.name + ': options missing in condition')
        if not self.eval_expr:
            raise ValueError(self.name + ': expression missing in condition')

    def __repr__(self):
        log_cond_str = (
            self.name + ' checks if the given expression evaluates to true'
        )
        return log_cond_str


class RulesSpec:
    def __init__(self, rules_path):
        self.file_path = rules_path
        self.rules_dict = {}
        self.conditions_dict = {}
        self.suggestions_dict = {}

    def perform_section_checks(self):
        for rule in self.rules_dict.values():
            rule.perform_checks()
        for cond in self.conditions_dict.values():
            cond.perform_checks()
        for sugg in self.suggestions_dict.values():
            sugg.perform_checks()

    def load_rules_from_spec(self):
        with open(self.file_path, 'r') as db_rules:
            curr_section = None
            for line in db_rules:
                element = IniParser.get_element(line)
                if element is IniParser.Element.comment:
                    continue
                elif element is not IniParser.Element.key_val:
                    curr_section = element  # it's a new IniParser header
                    section_name = IniParser.get_section_name(line)
                    if element is IniParser.Element.rule:
                        new_rule = Rule(section_name)
                        self.rules_dict[section_name] = new_rule
                    elif element is IniParser.Element.cond:
                        new_cond = Condition(section_name)
                        self.conditions_dict[section_name] = new_cond
                    elif element is IniParser.Element.sugg:
                        new_suggestion = Suggestion(section_name)
                        self.suggestions_dict[section_name] = new_suggestion
                elif element is IniParser.Element.key_val:
                    key, value = IniParser.get_key_value_pair(line)
                    if curr_section is IniParser.Element.rule:
                        new_rule.set_parameter(key, value)
                    elif curr_section is IniParser.Element.cond:
                        if key == 'source':
                            if value == 'LOG':
                                new_cond = LogCondition.create(new_cond)
                            elif value == 'OPTIONS':
                                new_cond = OptionCondition.create(new_cond)
                        else:
                            new_cond.set_parameter(key, value)
                    elif curr_section is IniParser.Element.sugg:
                        new_suggestion.set_parameter(key, value)

    def get_rules_dict(self):
        return self.rules_dict

    def get_conditions_dict(self):
        return self.conditions_dict

    def get_suggestions_dict(self):
        return self.suggestions_dict


def trigger_conditions(data_sources, conditions_dict):
    for source in data_sources:
        cond_subset = [
            cond
            for cond in conditions_dict.values()
            if cond.get_data_source() is source.type
        ]
        if not cond_subset:
            continue
        source.check_and_trigger_conditions(cond_subset)


def get_triggered_rules(rules_dict, conditions_dict):
    triggered_rules = []
    for rule in rules_dict.values():
        if rule.is_triggered(conditions_dict):
            triggered_rules.append(rule)
    return triggered_rules


def main(args):
    # Load the rules with their conditions and suggestions.
    db_rules = RulesSpec(args.rules_spec)
    db_rules.load_rules_from_spec()
    # Perform some basic sanity checks for each section.
    db_rules.perform_section_checks()
    rules_dict = db_rules.get_rules_dict()
    conditions_dict = db_rules.get_conditions_dict()
    suggestions_dict = db_rules.get_suggestions_dict()
    print()
    print('RULES')
    for rule in rules_dict.values():
        print(repr(rule))
    print()
    print('CONDITIONS')
    for cond in conditions_dict.values():
        print(repr(cond))
    print()
    print('SUGGESTIONS')
    for sugg in suggestions_dict.values():
        print(repr(sugg))

    # Initialise the data sources.
    data_sources = []
    data_sources.append(DatabaseOptions(args.rocksdb_options))
    data_sources.append(DatabaseLogs(args.rocksdb_log_prefix))

    # Initialise the ConditionChecker with the provided data sources.
    trigger_conditions(data_sources, conditions_dict)

    # Check for the conditions read in from the Rules spec, if triggered.
    print()
    triggered_rules = get_triggered_rules(rules_dict, conditions_dict)
    for rule in triggered_rules:
        print('Rule: ' + rule.name + ' has been triggered and:')
        rule_suggestions = rule.get_suggestions()
        for sugg_name in rule_suggestions:
            print(suggestions_dict[sugg_name])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script is used for\
        gauging rocksdb performance using as input: Rocksdb LOG, OPTIONS,\
        performance context, command-line statistics and statistics published\
        on ODS and providing as output: suggestions to improve Rocksdb\
        performance')
    parser.add_argument('--rules_spec', required=True, type=str)
    parser.add_argument('--rocksdb_options', required=True, type=str)
    parser.add_argument('--rocksdb_log_prefix', required=True, type=str)
    args = parser.parse_args()
    main(args)
