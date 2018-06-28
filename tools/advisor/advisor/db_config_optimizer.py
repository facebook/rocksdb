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

    # TODO: remove this comment
    '''
    'DB_WIDE' will be encountered here because of using get_options and
    update_options; and ideally, the guideline involving a db_wide option
    should have only one action suggested for it and the scope of that action
    should be 'DB_WIDE'
    '''
    # TODO: try other algorithms for improving the database configuration
    @staticmethod
    def improve_db_config(current_config, guidelines):
        # note: if the guideline option did not exist in the current_config,
        # no change made to it
        # pick an option to change, at random
        option = random.choice(list(current_config.keys()))
        better_config = {option: copy.deepcopy(current_config[option])}
        chosen_sugg_val = None
        suggested_values = (
            guidelines[option][Suggestion.Action.set][ConfigOptimizer.SUGG_VAL]
        )
        if suggested_values:
            chosen_sugg_val = random.choice(list(suggested_values))
        for col_fam in current_config[option]:
            sugg_action = ConfigOptimizer.get_action(
                guidelines, option, col_fam
            )
            if not sugg_action:
                continue
            if sugg_action is Suggestion.Action.increase:
                # need an old value to work with; increase by 10% for now
                new_value = 1.1 * float(current_config[option][col_fam])
            elif sugg_action is Suggestion.Action.decrease:
                # need an old value to work with; decrease by 10% for now
                new_value = 0.9 * float(current_config[option][col_fam])
            elif sugg_action is Suggestion.Action.set:
                # don't care about old value of option
                new_value = chosen_sugg_val
            better_config[option][col_fam] = new_value
        return better_config

    def __init__(self, bench_runner, db_options, rule_parser, benchmarks):
        self.bench_runner = bench_runner
        self.db_options = db_options
        self.rule_parser = rule_parser
        self.benchmarks = benchmarks

    def disambiguate_guidelines(self, guidelines):
        final_guidelines = copy.deepcopy(guidelines)
        options_to_remove = []
        acl = [action for action in Suggestion.Action]
        # for any option, if there is any intersection of scopes between the
        # different suggested actions, then remove that option from the
        # guidelines for now; resolving possible conflicts
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
                for action in Suggestion.Action:
                    if guidelines[option][action][self.SCOPE]:
                        db_wide_action = action
                        num_actions_suggested += 1
                if num_actions_suggested > 1:
                    options_to_remove.append(option)
                elif num_actions_suggested == 1:
                    guidelines[option][db_wide_action][self.SCOPE] = {NO_FAM}
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
                sugg_values = suggestions_dict[sugg_name].suggested_value
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

    # TODO: try other stopping conditions for the loop in this method
    def run(self, num_iterations):
        new_options = copy.deepcopy(self.db_options)
        suggestions_dict = self.rule_parser.get_suggestions_dict()
        for _ in range(num_iterations):
            # run the benchmarking tool for the given database configuration
            data_sources = self.bench_runner.run_experiment(
                self.benchmarks, new_options
            )
            # check the obtained data sources to for triggered rules
            triggered_rules = self.rule_parser.get_triggered_rules(
                data_sources, new_options.get_column_families()
            )
            if not triggered_rules:
                print('No rules triggered, exiting optimizer!')
                break
            # convert triggered rules to optimizer understandable advisor
            # guidelines
            guidelines = self.get_guidelines(triggered_rules, suggestions_dict)
            # use the guidelines to improve the database configuration
            working_config = new_options.get_options(list(guidelines.keys()))
            updated_config = ConfigOptimizer.improve_db_config(
                working_config, guidelines
            )
            new_options.update_options(updated_config)
        return new_options
