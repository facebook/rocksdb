from advisor.db_log_parser import DataSource
from advisor.ini_parser import IniParser


class OptionsSpecParser(IniParser):
    @staticmethod
    def is_new_option(line):
        if '=' in line:
            return True
        return False

    @staticmethod
    def get_section_type(line):
        section_path = line.strip()[1:-1].split()[0]
        section_type = '.'.join(section_path.split('/'))
        return section_type

    @staticmethod
    def get_section_name(line):
        token_list = line.strip()[1:-1].split('"')
        if len(token_list) < 3:
            return None
        return token_list[1]


class DatabaseOptions(DataSource):
    def __init__(self, rocksdb_options):
        super().__init__(DataSource.Type.DB_OPTIONS)
        self.options_path = rocksdb_options
        self.options_dict = None
        self.column_families = None

    def load_from_source(self):
        self.options_dict = {}
        with open(self.options_path, 'r') as db_options:
            for line in db_options:
                line = OptionsSpecParser.remove_trailing_comment(line)
                if not line:
                    continue
                if OptionsSpecParser.is_section_header(line):
                    curr_sec_type = OptionsSpecParser.get_section_type(line)
                    curr_sec_name = OptionsSpecParser.get_section_name(line)
                    if curr_sec_name:
                        option_prefix = curr_sec_name + '.' + curr_sec_type
                        if curr_sec_type == 'CFOptions':
                            if not self.column_families:
                                self.column_families = []
                            self.column_families.append(curr_sec_name)
                    else:
                        option_prefix = curr_sec_type
                elif OptionsSpecParser.is_new_option(line):
                    key, value = OptionsSpecParser.get_key_value_pair(line)
                    if not self.options_dict:
                        self.options_dict = {}
                    self.options_dict[option_prefix + '.' + key] = value
                else:
                    error = 'Not able to parse line in Options file.'
                    OptionsSpecParser.exit_with_parse_error(line, error)

    def check_and_trigger_conditions(self, conditions):
        # for every condition, if the fields are not present set_trigger will
        # not be called for it. Or if all the fields are present, then the
        # trigger will be set to whatever the expression evaluates to.
        self.load_from_source()
        for cond in conditions:
            prefix_ix = []
            ix = 0
            options = []
            for option in cond.options:
                if option in self.options_dict.keys():
                    options.append(self.options_dict[option])
                else:
                    prefix_ix.append(ix)
                    options.append(0)
                ix += 1

            # if all the options were present as is:
            if not prefix_ix:
                if not eval(cond.eval_expr):
                    cond.set_trigger(cond.eval_expr)
                continue

            # for all the options that were not present as is:
            for col_fam in self.column_families:
                present = True
                for ix in prefix_ix:
                    full_option = col_fam + '.' + cond.options[ix]
                    if full_option not in self.options_dict.keys():
                        present = False
                        break
                    options[ix] = self.options_dict[full_option]
                if present and not eval(cond.eval_expr):
                    cond.set_trigger(cond.eval_expr)
