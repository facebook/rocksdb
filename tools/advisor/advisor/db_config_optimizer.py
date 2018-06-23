from advisor.db_log_parser import NO_FAM
from advisor.rule_parser import Suggestion
import copy
from enum import Enum
import math
import pickle


'''
Note: This assumes that the options that need to optimized, are all numeric-
valued and they need to be either increased or decreased.
'''
# TODO: maybe run multiple instances of db_bench parallely but then only when
# all the runs are over for a particular option, then only we can compare their
# outputs and choose the best one.
# TODO: Remove unnecessary 'print' statements


class ConfigOptimizer:
    # class constants
    ACTION = 'action'
    SCOPE = 'scope'
    SUGG_VALUE = 'suggested_value'
    VAL_TRIED = 'values_experimented_with'
    STATISTIC = 'corresponding_statistic_value'
    BEST = 'best_value'

    @staticmethod
    def update_options(working_config, update_config, option_to_update):
        updated_config = copy.deepcopy(working_config)
        for col_fam in updated_config[option_to_update]:
            if update_config[option_to_update][col_fam] == 0:
                continue
            updated_config[option_to_update][col_fam] = math.floor(
                updated_config[option_to_update][col_fam] +
                update_config[option_to_update][col_fam]
            )
            if updated_config[option_to_update][col_fam] < 0:
                updated_config[option_to_update][col_fam] = 0
        return updated_config

    @staticmethod
    def is_better_than(compare_this, with_this, pref_value):
        if pref_value is ConfigOptimizer.StatPreferredValue.high:
            return compare_this > with_this
        elif pref_value is ConfigOptimizer.StatPreferredValue.low:
            return compare_this < with_this

    class StatPreferredValue(Enum):
        high = 1
        low = 2

    def __init__(self, benchmark_client, db_options, advisor_output):
        with open(advisor_output, 'rb') as fp:
            self.conditions_dict = pickle.load(fp)
            self.suggestions_dict = pickle.load(fp)
            self.triggered_rules = pickle.load(fp)
        self.db_options = db_options
        self.benchmark_client = benchmark_client

        # initialize the guidelines
        self.guidelines = self.get_guidelines_from_rules()
        print('GUIDELINES')
        print(self.guidelines)

        # initialize the output
        self.experiments = {}
        for option in self.guidelines:

            self.experiments[option] = {
                self.VAL_TRIED: [],
                self.STATISTIC: [],
                self.BEST: None,
                self.SCOPE: self.guidelines[option][self.SCOPE]
            }

    def get_guidelines_from_rules(self):
        # type: () -> Dict[str, Dict[str, Any]]
        guidelines = {}
        options_to_remove = []
        for rule in self.triggered_rules:
            for sugg_name in rule.get_suggestions():
                sugg = self.suggestions_dict[sugg_name]
                # ignore the suggestions that contain description / action=set
                if sugg.option and sugg.action:
                    if sugg.option in guidelines:
                        # if scopes are intersecting and actions are not same
                        # then ignore suggestions to this option
                        if (
                            sugg.action is not
                            guidelines[sugg.option][self.ACTION]
                        ):
                            # Ignore possibly contradicting suggestions for now
                            # TODO: If the scopes are mutually exclusive,
                            # then there is no contradiction.
                            options_to_remove.append(sugg.option)
                        else:
                            guidelines[sugg.option][self.SCOPE] = (
                                guidelines[sugg.option][self.SCOPE].union(
                                    rule.get_trigger_column_families()
                                )
                            )
                            sugg_val = guidelines[sugg.option][self.SUGG_VALUE]
                            if isinstance(sugg.suggested_value, str):
                                sugg_val = sugg_val.add(sugg.suggested_value)
                            else:
                                sugg_val = sugg_val.union(sugg.suggested_value)
                            guidelines[sugg.option][self.SUGG_VALUE] = sugg_val
                    else:
                        suggested_values = set()
                        if sugg.suggested_value:
                            if isinstance(sugg.suggested_value, str):
                                suggested_values = {sugg.suggested_value}
                            else:
                                suggested_values = set(sugg.suggested_value)
                        guidelines[sugg.option] = {
                            self.ACTION: sugg.action,
                            self.SCOPE: rule.get_trigger_column_families(),
                            self.SUGG_VALUE: suggested_values
                        }
        for option in options_to_remove:
            guidelines.pop(option)
        return guidelines

    def get_options_from_config(self, reqd_options):
        options_config = self.db_options.get_options(reqd_options)
        # convert the options' values to float type
        for option in options_config:
            for col_fam in options_config[option]:
                options_config[option][col_fam] = float(
                    options_config[option][col_fam]
                )
        return options_config

    def run_dbbench_and_get_stat(
        self, benchmark, options_file, reqd_stat, metric
    ):
        db_bench_output = self.benchmark_client.run_experiment(
            [benchmark], options_file
        )
        stats_dict = self.benchmark_client.fetch_statistics(
            db_bench_output, [reqd_stat]
        )
        print(
            reqd_stat + ':' + metric + '-' +
            str(stats_dict[reqd_stat][metric])
        )
        return stats_dict[reqd_stat][metric]

    def get_updates_for_options(
        self, original_config, factor_change, num_iterations
    ):
        # TODO: add heuristics here for options with small values
        multiplier = {
            Suggestion.Action.increase: ((factor_change - 1) / num_iterations),
            Suggestion.Action.decrease: (
                -((factor_change - 1) / (factor_change * num_iterations))
            ),
            Suggestion.Action.set: 0
        }
        update_config = {}
        for option in original_config:
            update_config[option] = {}
            for col_fam in original_config[option]:
                if (
                    col_fam == NO_FAM or  # this is a database-wide option
                    col_fam in self.guidelines[option][ConfigOptimizer.SCOPE]
                ):
                    update_config[option][col_fam] = (
                        multiplier[
                            self.guidelines[option][ConfigOptimizer.ACTION]
                        ] * original_config[option][col_fam]
                    )
                else:
                    update_config[option][col_fam] = 0
        print('update_config:')
        print(update_config)
        return update_config

    def run(self, benchmark, stat_to_improve, factor_change, num_iterations):
        # type: (str, str, str, Dict[str, PreferredValue]) ->
        # Dict[str, Dict[str, Any]]
        reqd_options = list(self.guidelines.keys())
        original_config = self.get_options_from_config(reqd_options)

        # Parse the stats_to_improve (3-tuple)
        reqd_stat = stat_to_improve[0]
        metric = stat_to_improve[1]
        pref_value = self.StatPreferredValue[stat_to_improve[2]]

        # Run db_bench with original_config,
        # initialise best_stat and best_config
        best_stat = self.run_dbbench_and_get_stat(
            benchmark, self.db_options.get_original_file(), reqd_stat, metric
        )
        best_config = copy.deepcopy(original_config)

        # handle the suggestions that require setting a particular value

        # calculate the updates reqd in the options
        update_config = self.get_updates_for_options(
            original_config, factor_change, num_iterations
        )

        for option in reqd_options:
            print('optimizing for option ' + option)
            print(best_config)
            working_config = copy.deepcopy(best_config)
            for _ in range(num_iterations):
                working_config = self.update_options(
                    working_config, update_config, option
                )
                new_options_file = 'temp/updated_options.tmp'
                self.db_options.generate_options_config(new_options_file)
                print('working config')
                print(working_config)
                working_stat = self.run_dbbench_and_get_stat(
                    benchmark, new_options_file, reqd_stat, metric
                )
                if ConfigOptimizer.is_better_than(
                    working_stat, best_stat, pref_value
                ):
                    best_stat = working_stat
                    best_config = working_config
        return best_config
