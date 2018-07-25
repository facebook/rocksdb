# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.rule_parser import Suggestion
import copy
import random


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
        new_value = None
        if action is Suggestion.Action.set or not old_value:
            assert(chosen_sugg_val)
            new_value = chosen_sugg_val
        else:
            # For increase/decrease actions, currently the code tries to make
            # a 30% change in the option's value per iteration. An addend is
            # also present (+1 or -1) to handle the cases when the option's
            # old value was 0 or the final int() conversion suppressed the 30%
            # change made to the option
            old_value = float(old_value)
            mul = 0
            add = 0
            if action is Suggestion.Action.increase:
                if old_value < 0:
                    mul = 0.7
                    add = 2
                else:
                    mul = 1.3
                    add = 2
            elif action is Suggestion.Action.decrease:
                if old_value < 0:
                    mul = 1.3
                    add = -2
                else:
                    mul = 0.7
                    add = -2
            new_value = int(old_value * mul + add)
        return new_value

    # TODO(poojam23): try other algorithms for improving the database
    # configuration
    # currently, this method picks ONE option at random, and makes
    # appropriate changes to that option for ALL the column families.
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
                try:
                    new_value = ConfigOptimizer.apply_action_on_value(
                        old_value, action, suggested_values
                    )
                    better_config[option][col_fam] = new_value
                except AssertionError:
                    print(
                        'WARNING(ConfigOptimizer): provide suggested_values ' +
                        'for ' + option
                    )
        return better_config

    @staticmethod
    def improve_db_config_v2(options, rule, suggestions_dict):
        '''
        this method takes ONE 'rule' and applies all its suggestions on the
        appropriate options whereas
        'improve_db_config' picks ONE option to update and applies all
        suggestions that were triggered for that option
        '''
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
                try:
                    new_value = ConfigOptimizer.apply_action_on_value(
                        None, sugg.action, sugg.suggested_values
                    )
                    if sugg.option not in updated_config:
                        updated_config[sugg.option] = {}
                    if '.' not in sugg.option:
                        updated_config[sugg.option][NO_FAM] = new_value
                    else:
                        for col_fam in rule.get_trigger_column_families():
                            updated_config[sugg.option][col_fam] = new_value
                except AssertionError:
                    print(
                        'WARNING(ConfigOptimizer): provide suggested_values ' +
                        'for ' + sugg.option
                    )
                continue
            # case: when the option is present in the current configuration
            if NO_FAM in current_config[sugg.option]:
                old_value = current_config[sugg.option][NO_FAM]
                try:
                    new_value = ConfigOptimizer.apply_action_on_value(
                        old_value, sugg.action, sugg.suggested_values
                    )
                    if sugg.option not in updated_config:
                        updated_config[sugg.option] = {}
                    updated_config[sugg.option][NO_FAM] = new_value
                except AssertionError:
                    print(
                        'WARNING(ConfigOptimizer): provide suggested_values ' +
                        'for ' + sugg.option
                    )
            else:
                for col_fam in rule.get_trigger_column_families():
                    old_value = None
                    if col_fam in current_config[sugg.option]:
                        old_value = current_config[sugg.option][col_fam]
                    try:
                        new_value = ConfigOptimizer.apply_action_on_value(
                            old_value, sugg.action, sugg.suggested_values
                        )
                        if sugg.option not in updated_config:
                            updated_config[sugg.option] = {}
                        updated_config[sugg.option][col_fam] = new_value
                    except AssertionError:
                        print(
                            'WARNING(ConfigOptimizer): provide ' +
                            'suggested_values for ' + sugg.option
                        )
        return current_config, updated_config

    @staticmethod
    def pick_rule_to_apply(rules, last_rule_name, rules_tried, backtrack):
        if not rules:
            print('\nNo more rules triggered!')
            return None
        # if the last rule provided an improvement in the database performance,
        # and it was triggered again (i.e. it is present in 'rules'), then pick
        # the same rule for this iteration too.
        if last_rule_name and not backtrack:
            for rule in rules:
                if rule.name == last_rule_name:
                    return rule
        # there was no previous rule OR the previous rule did not improve db
        # performance OR it was not triggered for this iteration,
        # then pick another rule that has not been tried yet
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

    # TODO(poojam23): check if this method is required or can we directly set
    # the config equal to the curr_config
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

    def get_guidelines(self, rules, suggestions_dict):
        '''
        This method takes in a list of Rule objects whose suggestions need to
        be applied to the Rocksdb options. From these suggestions it creates
        and returns a dictionary:
        Dict[option_name, Dict[action, {'scope': set(), 'sugg_vals': set()}]]
        '''
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
        # resolve the scope of database wide options, example (to explain):
        # Rule_A triggered for col_fam_A may have a suggestion to increase the
        # DATABASE-WIDE option_XYZ; to be able to update this option in
        # the DatabaseOptions object, need to reset the scope for option_XYZ
        # in guidelines to NO_FAM instead of col_fam_A
        guidelines_options = list(guidelines.keys())
        config = self.db_options.get_options(guidelines_options)
        for option in guidelines_options:
            misc_option = False
            if option not in config:
                if '.' not in option:
                    # it's an option that is not supported by the Rocksdb
                    # OPTIONS file
                    misc_option = True
                else:
                    # note: no way to find out if this option is database wide
                    continue
            if misc_option or NO_FAM in config[option]:
                for action in Suggestion.Action:
                    if guidelines[option][action][self.SCOPE]:
                        guidelines[option][action][self.SCOPE] = {NO_FAM}
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
            # fetch the subset of Rocksdb options for which suggestions were
            # triggered in this iteration
            working_config = new_options.get_options(list(guidelines.keys()))
            updated_config = ConfigOptimizer.improve_db_config(
                working_config, guidelines
            )
            print('current config:\n' + repr(working_config))
            print('db config changes:\n' + repr(updated_config))
            new_options.update_options(updated_config)
        return new_options

    def run_v2(self):
        '''
        In every iteration of this method's optimization loop we pick ONE RULE
        from all the triggered rules and apply all its suggestions to the
        appropriate options. This is because this method calls
        'apply_suggestions' which calls 'improve_db_config_v2'.
        On the other hand, in method run(), ONE OPTION is picked in every
        iteration and the all the suggestions for that option are applied
        to the appropriate column families. This method calls
        'improve_db_config'.
        '''
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
