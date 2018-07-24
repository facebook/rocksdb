# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.rule_parser import Suggestion
import copy
import random
import shutil
import subprocess


class ConfigOptimizer:
    SCOPE = 'scope'
    SUGG_VAL = 'suggested values'
    DB_CHECKPOINT = '/dev/shm/dbbench.base'

    @staticmethod
    def get_guideline_boiler_plate():
        guideline = {}
        for action in Suggestion.Action:
            guideline[action] = {
                ConfigOptimizer.SCOPE: set(),
                ConfigOptimizer.SUGG_VAL: set()
            }
        return guideline

    @staticmethod
    def apply_action_on_value(old_value, action, suggested_values):
        chosen_sugg_val = None
        if suggested_values:
            chosen_sugg_val = random.choice(list(suggested_values))
        if action is Suggestion.Action.increase:
            if old_value:
                old_value = float(old_value)
            if (not old_value or old_value <= 0) and chosen_sugg_val:
                new_value = chosen_sugg_val
            elif not old_value:
                new_value = None
            elif old_value < 10:
                new_value = old_value + 2
            else:
                new_value = 1.3 * old_value
            new_value = int(new_value)
        elif action is Suggestion.Action.decrease:
            if old_value:
                old_value = float(old_value)
            if (not old_value or old_value <= 0) and chosen_sugg_val:
                new_value = chosen_sugg_val
            elif not old_value:
                new_value = None
            else:
                new_value = 0.7 * old_value
            new_value = int(new_value)
        elif action is Suggestion.Action.set:
            # don't care about old value of option
            new_value = chosen_sugg_val
        return new_value

    # TODO(poojam23): try other algorithms for improving the database
    # configuration currently, this picks an option at random
    @staticmethod
    def improve_db_config(current_config, guidelines):
        option = random.choice(list(guidelines.keys()))
        better_config = {option: {}}
        for action in guidelines[option]:
            suggested_values = (
                guidelines[option][action][ConfigOptimizer.SUGG_VAL]
            )
            for col_fam in guidelines[option][action][ConfigOptimizer.SCOPE]:
                old_value = None
                if (
                    option in current_config and
                    col_fam in current_config[option]
                ):
                    old_value = float(current_config[option][col_fam])
                new_value = ConfigOptimizer.apply_action_on_value(
                    old_value, action, suggested_values
                )
                if new_value:
                    better_config[option][col_fam] = new_value
        return better_config

    @staticmethod
    def improve_db_config_v2(options, rule, suggestions_dict):
        # get the current configuration
        required_options = []
        rule_suggestions = []
        for sugg_name in rule.get_suggestions():
            option = suggestions_dict[sugg_name].option
            action = suggestions_dict[sugg_name].action
            if not (option and action):  # suggestion not in required format
                continue
            required_options.append(option)
            rule_suggestions.append(suggestions_dict[sugg_name])
        current_config = options.get_options(required_options)
        # Create the updated configuration from the rule's suggestions
        updated_config = {}
        for sugg in rule_suggestions:
            # case: when the option is not present in the current configuration
            if sugg.option not in current_config:
                new_value = ConfigOptimizer.apply_action_on_value(
                    None, sugg.action, sugg.suggested_values
                )
                if new_value:
                    if sugg.option not in updated_config:
                        updated_config[sugg.option] = {}
                    if '.' not in sugg.option:
                        updated_config[sugg.option][NO_FAM] = new_value
                    else:
                        for col_fam in rule.get_trigger_column_families():
                            updated_config[sugg.option][col_fam] = new_value
                continue
            # case: when the option is present in the current configuration
            if NO_FAM in current_config[sugg.option]:
                old_value = current_config[sugg.option][NO_FAM]
                new_value = ConfigOptimizer.apply_action_on_value(
                    old_value, sugg.action, sugg.suggested_values
                )
                if new_value:
                    if sugg.option not in updated_config:
                        updated_config[sugg.option] = {}
                    updated_config[sugg.option][NO_FAM] = new_value
            else:
                for col_fam in rule.get_trigger_column_families():
                    old_value = None
                    if col_fam in current_config[sugg.option]:
                        old_value = current_config[sugg.option][col_fam]
                    new_value = ConfigOptimizer.apply_action_on_value(
                        old_value, sugg.action, sugg.suggested_values
                    )
                    if new_value:
                        if sugg.option not in updated_config:
                            updated_config[sugg.option] = {}
                        updated_config[sugg.option][col_fam] = new_value
        return current_config, updated_config

    @staticmethod
    def pick_rule_to_apply(rules, current_rule_name, rules_tried, backtrack):
        if not rules:
            print('\nNo more rules triggered!')
            return None
        if current_rule_name and not backtrack:
            for rule in rules:
                if rule.name == current_rule_name:
                    return rule
        for rule in rules:
            if rule.name not in rules_tried:
                return rule
        print('\nAll rules have been exhausted')
        return None

    @staticmethod
    def apply_suggestions(
        triggered_rules,
        current_rule_name,
        rules_tried,
        backtrack,
        curr_options,
        suggestions_dict
    ):
        curr_rule = ConfigOptimizer.pick_rule_to_apply(
            triggered_rules, current_rule_name, rules_tried, backtrack
        )
        if not curr_rule:
            return tuple([None]*4)
        # if a rule has been picked for improving db_config, update rules_tried
        rules_tried.add(curr_rule.name)
        # get updated config based on the picked rule
        curr_conf, updated_conf = ConfigOptimizer.improve_db_config_v2(
            curr_options, curr_rule, suggestions_dict
        )
        conf_diff = DatabaseOptions.get_options_diff(curr_conf, updated_conf)
        if not conf_diff:  # the current and updated configs are the same
            curr_rule, rules_tried, curr_conf, updated_conf = (
                ConfigOptimizer.apply_suggestions(
                    triggered_rules,
                    None,
                    rules_tried,
                    backtrack,
                    curr_options,
                    suggestions_dict
                )
            )
        return (curr_rule, rules_tried, curr_conf, updated_conf)

    @staticmethod
    def get_backtrack_config(curr_config, updated_config):
        diff = DatabaseOptions.get_options_diff(curr_config, updated_config)
        bt_config = {}
        for option in diff:
            bt_config[option] = {}
            for col_fam in diff[option]:
                bt_config[option][col_fam] = diff[option][col_fam][0]
        print(bt_config)
        return bt_config

    def __init__(self, bench_runner, db_options, rule_parser, base_db):
        self.bench_runner = bench_runner
        self.db_options = db_options
        self.rule_parser = rule_parser
        self.base_db_path = base_db

    def disambiguate_guidelines(self, guidelines):
        final_guidelines = copy.deepcopy(guidelines)
        options_to_remove = []
        acl = [action for action in Suggestion.Action]
        # for any option, if there is any intersection of scopes between the
        # different suggested actions, then remove that option from the
        # guidelines for now; resolving conflicting suggestions
        for option in guidelines:
            for ix in range(len(acl)):
                sc1 = guidelines[option][acl[ix]][self.SCOPE]
                for cw in range(ix+1, len(acl), 1):
                    sc2 = guidelines[option][acl[cw]][self.SCOPE]
                    if sc1.intersection(sc2):
                        options_to_remove.append(option)
                        break
        # if it's a database-wide option, only one action is possible on it,
        # so remove this option if >1 actions have been suggested for it
        current_config = self.db_options.get_options(list(guidelines.keys()))
        for option in guidelines:
            misc_option = False
            if option not in current_config:
                if '.' not in option:
                    misc_option = True
                else:
                    # note: no way to disambiguate guidelines for these options
                    # also, update will not be proper in improve_db_config
                    # if the option is really 'DB_WIDE'
                    continue
            if misc_option or NO_FAM in current_config[option]:
                num_actions_suggested = 0
                db_wide_action = None
                for action in Suggestion.Action:
                    if guidelines[option][action][self.SCOPE]:
                        db_wide_action = action
                        num_actions_suggested += 1
                if num_actions_suggested > 1:
                    options_to_remove.append(option)
                elif num_actions_suggested == 1:
                    final_guidelines[option][db_wide_action][self.SCOPE] = (
                        {NO_FAM}
                    )
        # remove the ambiguous guidelines
        for option in options_to_remove:
            final_guidelines.pop(option, None)
        return final_guidelines

    def get_guidelines(self, rules, suggestions_dict):
        guidelines = {}
        for rule in rules:
            new_scope = rule.get_trigger_column_families()
            for sugg_name in rule.get_suggestions():
                option = suggestions_dict[sugg_name].option
                action = suggestions_dict[sugg_name].action
                if not (option or action):  # suggestion not in required format
                    continue
                sugg_values = suggestions_dict[sugg_name].suggested_values
                if not sugg_values:
                    sugg_values = []
                if option not in guidelines:
                    guidelines[option] = self.get_guideline_boiler_plate()
                guidelines[option][action][self.SCOPE] = (
                    guidelines[option][action][self.SCOPE].union(new_scope)
                )
                guidelines[option][action][self.SUGG_VAL] = (
                    guidelines[option][action][self.SUGG_VAL].union(
                        sugg_values
                    )
                )
        guidelines = self.disambiguate_guidelines(guidelines)
        return guidelines

    def run(self, num_iterations):
        new_options = copy.deepcopy(self.db_options)
        for _ in range(num_iterations):
            # run the benchmarking tool for the given database configuration
            print('\nbenchrunner experiment: ')
            data_sources, _ = self.bench_runner.run_experiment(new_options)
            # load and run the rule parser with the obtained data sources
            self.rule_parser.load_rules_from_spec()
            self.rule_parser.perform_section_checks()
            triggered_rules = self.rule_parser.get_triggered_rules(
                data_sources, new_options.get_column_families()
            )
            if not triggered_rules:
                print('No rules triggered, exiting optimizer!')
                break
            print('\nTriggered:')
            self.rule_parser.print_rules(triggered_rules)
            # convert triggered rules to optimizer understandable advisor
            # guidelines
            guidelines = self.get_guidelines(
                triggered_rules, self.rule_parser.get_suggestions_dict()
            )
            # use the guidelines to improve the database configuration
            working_config = new_options.get_options(list(guidelines.keys()))
            updated_config = ConfigOptimizer.improve_db_config(
                working_config, guidelines
            )
            print('current config:\n' + repr(working_config))
            print('db config changes:\n' + repr(updated_config))
            new_options.update_options(updated_config)
        return new_options

    def run_v2(self):
        # bootstrapping the optimizer
        print('Bootstrapping optimizer:')
        options = copy.deepcopy(self.db_options)
        old_data_sources, old_metric = (
            self.bench_runner.run_experiment(options, self.base_db_path)
        )
        print('Initial metric: ' + str(old_metric))
        self.rule_parser.load_rules_from_spec()
        self.rule_parser.perform_section_checks()
        triggered_rules = self.rule_parser.get_triggered_rules(
            old_data_sources, options.get_column_families()
        )
        print('\nTriggered:')
        self.rule_parser.print_rules(triggered_rules)
        backtrack = False
        rules_tried = set()
        curr_rule, rules_tried, curr_conf, updated_conf = (
            ConfigOptimizer.apply_suggestions(
                triggered_rules,
                None,
                rules_tried,
                backtrack,
                options,
                self.rule_parser.get_suggestions_dict()
            )
        )
        # the optimizer loop
        while curr_rule:
            print('\nRule picked for next iteration:')
            print(curr_rule.name)
            print('\ncurrent config:')
            print(curr_conf)
            print('updated config:')
            print(updated_conf)
            options.update_options(updated_conf)
            # run bench_runner with updated config
            new_data_sources, new_metric = (
                self.bench_runner.run_experiment(options, self.base_db_path)
            )
            print('\nnew metric: ' + str(new_metric))
            backtrack = not self.bench_runner.is_metric_better(
                new_metric, old_metric
            )
            # update triggered_rules, metric, data_sources, if required
            if backtrack:
                # revert changes to options config
                print('\nBacktracking to previous configuration')
                backtrack_conf = ConfigOptimizer.get_backtrack_config(
                    curr_conf, updated_conf
                )
                options.update_options(backtrack_conf)
            else:
                # run advisor on new data sources
                self.rule_parser.load_rules_from_spec()  # reboot the advisor
                self.rule_parser.perform_section_checks()
                triggered_rules = self.rule_parser.get_triggered_rules(
                    new_data_sources, options.get_column_families()
                )
                print('\nTriggered:')
                self.rule_parser.print_rules(triggered_rules)
                old_metric = new_metric
                old_data_sources = new_data_sources
                rules_tried = set()
            # pick rule to work on and set curr_rule to that
            curr_rule, rules_tried, curr_conf, updated_conf = (
                ConfigOptimizer.apply_suggestions(
                    triggered_rules,
                    curr_rule.name,
                    rules_tried,
                    backtrack,
                    options,
                    self.rule_parser.get_suggestions_dict()
                )
            )
        # return the final database options configuration
        return options
