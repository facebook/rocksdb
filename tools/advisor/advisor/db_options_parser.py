# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import copy
from advisor.db_log_parser import DataSource, NO_FAM
from advisor.ini_parser import IniParser


class OptionsSpecParser(IniParser):
    @staticmethod
    def is_new_option(line):
        return '=' in line

    @staticmethod
    def get_section_type(line):
        '''
        Example section header: [TableOptions/BlockBasedTable "default"]
        Here ConfigurationOptimizer returned would be
        'TableOptions.BlockBasedTable'
        '''
        section_path = line.strip()[1:-1].split()[0]
        ConfigurationOptimizer = '.'.join(section_path.split('/'))
        return ConfigurationOptimizer

    @staticmethod
    def get_section_name(line):
        token_list = line.strip()[1:-1].split('"')
        if len(token_list) < 3:
            return None
        return token_list[1]

    @staticmethod
    def get_section_str(section_type, section_name):
        section_type = '/'.join(section_type.strip().split('.'))
        section_str = '[' + section_type
        if section_name == NO_FAM:
            return (section_str + ']')
        else:
            return section_str + ' "' + section_name + '"]'

    @staticmethod
    def get_option_str(key, values):
        option_str = key + '='
        if values:
            if isinstance(values, list):
                for value in values:
                    option_str += (str(value) + ':')
                option_str = option_str[:-1]
            else:
                option_str += str(values)
        return option_str


class DatabaseOptions(DataSource):
    def __init__(self, rocksdb_options):
        super().__init__(DataSource.Type.DB_OPTIONS)
        self.options_path = rocksdb_options
        # The options are stored in the following data structure:
        # Dict[str, Dict[str, Dict[str, Any]]].
        # The above strings are:
        # ConfigurationOptimizer, column_family, option, value(s).
        self.options_dict = None
        self.column_families = None
        # Load the options from the given file to a dictionary.
        self.load_from_source()

    def get_original_file(self):
        return self.options_path

    def load_from_source(self):
        self.options_dict = {}
        with open(self.options_path, 'r') as db_options:
            for line in db_options:
                line = OptionsSpecParser.remove_trailing_comment(line)
                if not line:
                    continue
                if OptionsSpecParser.is_section_header(line):
                    curr_sec_type = (
                        OptionsSpecParser.get_section_type(line)
                    )
                    curr_sec_name = OptionsSpecParser.get_section_name(line)
                    if curr_sec_type not in self.options_dict:
                        self.options_dict[curr_sec_type] = {}
                    if not curr_sec_name:
                        curr_sec_name = NO_FAM
                    self.options_dict[curr_sec_type][curr_sec_name] = {}
                    if curr_sec_type == 'CFOptions':
                        if not self.column_families:
                            self.column_families = []
                        self.column_families.append(curr_sec_name)
                elif OptionsSpecParser.is_new_option(line):
                    key, value = OptionsSpecParser.get_key_value_pair(line)
                    self.options_dict[curr_sec_type][curr_sec_name][key] = (
                        value
                    )
                else:
                    error = 'Not able to parse line in Options file.'
                    OptionsSpecParser.exit_with_parse_error(line, error)

    def get_column_families(self):
        return self.column_families

    def get_options(self, reqd_options):
        # type: List[str] -> Dict[str, Dict[str, Any]]
        # List[option] -> Dict[option, Dict[col_fam, value]]
        reqd_options_dict = {}
        for option in reqd_options:
            sec_name = '.'.join(option.split('.')[:-1])
            opt_name = option.split('.')[-1]
            if sec_name not in self.options_dict:
                continue
            if (
                NO_FAM in self.options_dict[sec_name] and
                opt_name in self.options_dict[sec_name][NO_FAM]
            ):
                if option not in reqd_options_dict:
                    reqd_options_dict[option] = {}
                reqd_options_dict[option][NO_FAM] = (
                    self.options_dict[sec_name][NO_FAM][opt_name]
                )
            for col_fam in self.options_dict[sec_name]:
                if opt_name in self.options_dict[sec_name][col_fam]:
                    if option not in reqd_options_dict:
                        reqd_options_dict[option] = {}
                    reqd_options_dict[option][col_fam] = (
                        self.options_dict[sec_name][col_fam][opt_name]
                    )
        return reqd_options_dict

    def update_options(self, options):
        # type: Dict[str, Dict[str, Any]] -> None
        # Dict[option, Dict[col_fam, value]] -> None where option is in the
        # form: ('.' delimited section type) + '.' + option
        for option in options:
            sec_name = '.'.join(option.split('.')[:-1])
            opt_name = option.split('.')[-1]
            if sec_name not in self.options_dict:
                self.options_dict[sec_name] = {}
            for col_fam in options[option]:
                # if the option is not already present in the dictionary,
                # it will be inserted, else it will be updated to the new
                # value
                if col_fam not in self.options_dict[sec_name]:
                    self.options_dict[sec_name][col_fam] = {}
                self.options_dict[sec_name][col_fam][opt_name] = (
                    copy.deepcopy(options[option][col_fam])
                )

    def generate_options_config(self, file_name):
        # type: str -> str
        with open(file_name, 'w') as fp:
            for section in self.options_dict:
                for col_fam in self.options_dict[section]:
                    fp.write(
                        OptionsSpecParser.get_section_str(section, col_fam) +
                        '\n'
                    )
                    for option in self.options_dict[section][col_fam]:
                        values = self.options_dict[section][col_fam][option]
                        fp.write(
                            OptionsSpecParser.get_option_str(option, values) +
                            '\n'
                        )
        return file_name

    def check_and_trigger_conditions(self, conditions):
        for cond in conditions:
            reqd_options_dict = self.get_options(cond.options)
            incomplete_option_ix = []
            options = []
            missing_reqd_option = False
            for ix, option in enumerate(cond.options):
                if option not in reqd_options_dict:
                    missing_reqd_option = True
                    break  # required option absent
                if NO_FAM in reqd_options_dict[option]:
                    options.append(reqd_options_dict[option][NO_FAM])
                else:
                    options.append(None)
                    incomplete_option_ix.append(ix)

            if missing_reqd_option:
                continue

            # if all the options are database-wide options
            if not incomplete_option_ix:
                try:
                    if eval(cond.eval_expr):
                        cond.set_trigger({NO_FAM: options})
                except Exception as e:
                    print('DatabaseOptions check_and_trigger: ' + str(e))
                continue

            # for all the options that are not database-wide, we look for their
            # values specific to column families
            col_fam_options_dict = {}
            for col_fam in self.column_families:
                present = True
                for ix in incomplete_option_ix:
                    option = cond.options[ix]
                    if col_fam not in reqd_options_dict[option]:
                        present = False
                        break
                    options[ix] = reqd_options_dict[option][col_fam]
                if present:
                    try:
                        if eval(cond.eval_expr):
                            col_fam_options_dict[col_fam] = (
                                copy.deepcopy(options)
                            )
                    except Exception as e:
                        print('DatabaseOptions check_and_trigger: ' + str(e))
            if col_fam_options_dict:
                cond.set_trigger(col_fam_options_dict)
