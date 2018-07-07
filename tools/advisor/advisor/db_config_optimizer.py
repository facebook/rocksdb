# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import NO_FAM
from advisor.rule_parser import Suggestion
import copy
import random


class ConfigOptimizer:
    SCOPE = 'scope'
    SUGG_VAL = 'suggested values'

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
    def get_action(guidelines, option, col_fam):
        for action in Suggestion.Action:
            action_scope = guidelines[option][action][ConfigOptimizer.SCOPE]
            if action_scope and col_fam in action_scope:
                return action
        return None

    @staticmethod
    def apply_action_on_value(old_value, action, suggested_values):
        chosen_sugg_val = None
        if suggested_values:
            chosen_sugg_val = random.choice(list(suggested_values))
        if action is Suggestion.Action.increase:
            # need an old value to work with; increase by 10% for now
            if (not old_value or old_value <= 0) and chosen_sugg_val:
                new_value = chosen_sugg_val
            elif old_value < 10:
                new_value = old_value + 2
            else:
                new_value = 1.3 * old_value
        elif action is Suggestion.Action.decrease:
            # need an old value to work with; decrease by 10% for now
            new_value = 0.7 * old_value
        elif action is Suggestion.Action.set:
            # don't care about old value of option
            new_value = chosen_sugg_val
        return new_value

    # TODO: try other algorithms for improving the database configuration
    # currently, this picks an option at random
    @staticmethod
    def improve_db_config(current_config, guidelines):
        # note: if an option is not in the current config, it's not chosen
        # for update
        option = random.choice(list(current_config.keys()))
        better_config = {option: copy.deepcopy(current_config[option])}
        suggested_values = (
            guidelines[option][Suggestion.Action.set][ConfigOptimizer.SUGG_VAL]
        )
        for col_fam in current_config[option]:
            sugg_action = ConfigOptimizer.get_action(
                guidelines, option, col_fam
            )
            if not sugg_action:
                continue
            old_value = float(current_config[option][col_fam])
            new_value = ConfigOptimizer.apply_action_on_value(
                old_value, sugg_action, suggested_values
            )
            better_config[option][col_fam] = new_value
        return better_config

    @staticmethod
    def improve_db_config_v2(options, rule, suggestions_dict):
        required_options = []
        rule_suggestions = []
        for sugg_name in rule.get_suggestions():
            option = suggestions_dict[sugg_name].option
            action = suggestions_dict[sugg_name].action
            if not (option or action):  # suggestion not in required format
                continue
            required_options.append(option)
            rule_suggestions.append(suggestions_dict[sugg_name])
        # get the current configuration
        current_config = options.get_options(required_options)
        updated_config = copy.deepcopy(current_config)
        # apply the suggestions to generated updated_config
        for sugg in rule_suggestions:
            # note: if an option is not in the current config, it's not updated
            if sugg.option not in updated_config:
                continue
            for col_fam in updated_config[sugg.option]:
                if (
                    col_fam == NO_FAM or
                    col_fam in rule.get_trigger_column_families()
                ):
                    new_value = ConfigOptimizer.apply_action_on_value(
                        float(current_config[sugg.option][col_fam]),
                        sugg.action,
                        sugg.suggested_values
                    )
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

    def __init__(self, bench_runner, db_options, rule_parser):
        self.bench_runner = bench_runner
        self.db_options = db_options
        self.rule_parser = rule_parser

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
        for option in current_config:
            if NO_FAM in current_config[option]:
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
        old_data_sources, old_throughput = (
            self.bench_runner.run_experiment(options)
        )
        print('Initial throughput: ' + str(old_throughput))
        self.rule_parser.load_rules_from_spec()
        self.rule_parser.perform_section_checks()
        triggered_rules = self.rule_parser.get_triggered_rules(
            old_data_sources, options.get_column_families()
        )
        print('\nTriggered:')
        self.rule_parser.print_rules(triggered_rules)
        backtrack = False
        rules_tried = set()
        curr_rule = self.pick_rule_to_apply(
            triggered_rules, None, rules_tried, backtrack
        )
        # the optimizer loop
        while curr_rule:
            print('\nRule picked for next iteration:')
            print(curr_rule.name)
            # update rules_tried
            rules_tried.add(curr_rule.name)
            # get updated config based on the picked rule
            curr_conf, updated_conf = ConfigOptimizer.improve_db_config_v2(
                options, curr_rule, self.rule_parser.get_suggestions_dict()
            )
            print('\ncurrent config:')
            print(curr_conf)
            print('updated config:')
            print(updated_conf)
            options.update_options(updated_conf)
            # run bench_runner with updated config: data_sources, throughput
            new_data_sources, new_throughput = (
                self.bench_runner.run_experiment(options)
            )
            print('\nnew throughput: ' + str(new_throughput))
            backtrack = (new_throughput < old_throughput)
            # update triggered_rules, throughput, data_sources, if required
            if backtrack:
                # revert changes to options config
                print('\nBacktracking to previous configuration')
                options.update_options(curr_conf)
            else:
                # run advisor on new data sources
                self.rule_parser.load_rules_from_spec()  # reboot the advisor
                self.rule_parser.perform_section_checks()
                triggered_rules = self.rule_parser.get_triggered_rules(
                    new_data_sources, options.get_column_families()
                )
                print('\nTriggered:')
                self.rule_parser.print_rules(triggered_rules)
                old_throughput = new_throughput
                old_data_sources = new_data_sources
            # pick rule to work on and set curr_rule to that
            curr_rule = self.pick_rule_to_apply(
                triggered_rules, curr_rule.name, rules_tried, backtrack)
        # generate the final rocksdb options file
        options_file = options.generate_options_config('final')
        return options_file
