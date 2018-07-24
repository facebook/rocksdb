from advisor.db_log_parser import NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.ini_parser import IniParser


class MyCnfParser:
    MYSQLD = "mysqld"
    DEFAULT_CF = "loose-rocksdb_default_cf_options"
    OTHER_CF = "loose-rocksdb_override_cf_options"
    # list of ignored sections from Rocksdb OPTIONS file
    OPTIONS_SEC_IGNORED = ['Version']
    # list of ignored options from OPTIONS file
    OPTIONS_IGNORED = ['TableOptions.BlockBasedTable.filter_policy']
    OPT_SEC_KEY_HIERARCHY = {
        'CFOptions': DEFAULT_CF,
        'TableOptions.BlockBasedTable': (
            DEFAULT_CF + '.block_based_table_factory'
        )
    }

    @staticmethod
    def get_section(line):
        return line.strip()[1:-1].strip()

    @staticmethod
    def is_rocksdb_option(option):
        return (
            option.strip().startswith('rocksdb_') or
            option.strip().startswith('loose-rocksdb_')
        )

    @staticmethod
    def is_value_dict(opt_value):
        opt_value = opt_value.strip()
        return opt_value.startswith('{') and opt_value.endswith('}')

    @staticmethod
    def get_str_from_dict(value_dict):
        dict_str = ''
        for key in value_dict:
            value = value_dict[key]
            if isinstance(value_dict[key], dict):
                value = MyCnfParser.get_str_from_dict(value_dict[key])
                value = '{' + value + '}'
            dict_str += (key + '=' + value + ';')
        return dict_str

    @staticmethod
    def get_key_value_pair(line):
        key = line.split('=')[0].strip()
        value = '='.join(line.split('=')[1:])
        return key, value

    @staticmethod
    def get_last_val_from_key_hier(init_dict, key_hier):
        final_dict = init_dict
        for key in key_hier:
            if key not in final_dict:
                final_dict[key] = {}
            final_dict = final_dict[key]
        return final_dict

    @staticmethod
    def get_dict_from_str(string, ix_pair, dict_of_dict):
        # parses 'string' from [ix_pair[0]+1, ix_pair[1]-1], both indices
        # included: because it expects the string to be flanked by '{' and '}'
        # the method expects that there should be no spaces in 'string'
        key = ''
        value = ''
        val_begin = False
        final_dict = {}
        ix = ix_pair[0] + 1  # start parsing from this index
        while ix < ix_pair[1]:
            if string[ix] == '=':
                val_begin = True
            elif val_begin and string[ix] == ';':
                final_dict[key] = value
                key = ''
                value = ''
                val_begin = False
            elif val_begin and string[ix] == '{':
                final_dict[key] = dict_of_dict[ix][0]
                ix = dict_of_dict[ix][1] + 1  # go to ';' char at end of value
                key = ''
                value = ''
                val_begin = False
            elif val_begin:
                value += string[ix]
            else:
                key += string[ix]
            ix += 1  # go to the next key for parsing
        if value:
            final_dict[key] = value
        return final_dict

    @staticmethod
    def find_all(string, character):
        return [ix for ix, char in enumerate(string) if char == character]

    @staticmethod
    def get_all_dictionaries(string):
        start_list = MyCnfParser.find_all(string, '{')
        end_list = MyCnfParser.find_all(string, '}')
        dict_of_dict = {}  # start_ix: (dict, end_ix)
        for e_ix in end_list:
            ix = 0
            for s_ix in start_list:
                if s_ix > e_ix:
                    break
                ix += 1
            pair = (start_list[ix - 1], e_ix)
            dict = MyCnfParser.get_dict_from_str(string, pair, dict_of_dict)
            dict_of_dict[start_list[ix-1]] = (dict, e_ix)
            start_list.pop(ix - 1)
        return dict_of_dict

    @staticmethod
    def options_to_cnf_mapper(option, option_value):
        # maps some options from the Rocksdb OPTIONS file to their format in
        # the MySQL config file
        name = None
        value = None
        if option == 'cache_size':
            name = 'loose-rocksdb_block_cache_size'
            value = option_value
        elif option == 'bloom_bits':
            name = (
                MyCnfParser.DEFAULT_CF +
                '.block_based_table_factory.filter_policy'
            )
            value = 'bloomfilter:%s:false' % str(option_value)
        elif option == 'wal_recovery_mode':
            wal_recovery_mode_val_dict = {
                'kTolerateCorruptedTailRecords': '0',
                'kAbsoluteConsistency': '1',
                'kPointInTimeRecovery': '2',
                'kSkipAnyCorruptedRecords': '3'
            }
            name = 'loose-rocksdb_wal_recovery_mode'
            value = wal_recovery_mode_val_dict[option_value]
        elif option == 'access_hint_on_compaction_start':
            access_hint_val_dict = {
                'NONE': '0',
                'NORMAL': '1',
                'SEQUENTIAL': '2',
                'WILLNEED': '3'
            }
            name = 'loose-rocksdb_access_hint_on_compaction_start'
            value = access_hint_val_dict[option_value]
        return name, value

    @staticmethod
    def try_map_boolean_value(option_value):
        if isinstance(option_value, str):
            temp_value = option_value.strip().lower()
            if temp_value == 'false':
                return '0'
            if temp_value == 'true':
                return '1'
        return option_value

    def __init__(self, cnf):
        self.cnf_dict = None
        self.col_fam = None
        self.load_from_file(cnf)

    def load_from_file(self, cnf):
        self.cnf_dict = {}
        self.col_fam = []
        fp = open(cnf, 'r')
        curr_sec = None
        for line in fp:
            line = line.strip()  # remove the ending newline chars, if any
            line = line.replace(' ', '')  # remove unnecessary spaces
            if not line:  # if the line is now empty, continue
                continue
            if IniParser.is_section_header(line):
                # initialize new section
                curr_sec = MyCnfParser.get_section(line)
                self.cnf_dict[curr_sec] = {}
            else:  # it's a key-value pair
                key, value = MyCnfParser.get_key_value_pair(line)
                if MyCnfParser.is_rocksdb_option(key):
                    if not key.startswith('loose-'):
                        key = 'loose-' + key
                if key == self.DEFAULT_CF:
                    value = '{' + value + '}'
                    dict_of_dict = MyCnfParser.get_all_dictionaries(value)
                    value = MyCnfParser.get_dict_from_str(
                        value, (0, len(value)-1), dict_of_dict
                    )
                    self.col_fam.append('default')
                elif key == self.OTHER_CF:
                    value = '{' + value + '}'
                    dict_of_dict = MyCnfParser.get_all_dictionaries(value)
                    value = MyCnfParser.get_dict_from_str(
                        value, (0, len(value)-1), dict_of_dict
                    )
                    self.col_fam.extend(list(value.keys()))
                self.cnf_dict[curr_sec][key] = value
        fp.close()

    def generate_config_file(self, port):
        file_path = 'temp/my-' + str(port) + '.cnf'
        fp = open(file_path, 'w')
        for section in self.cnf_dict:
            section_str = '[' + section + ']\n'
            fp.write(section_str)
            for option in self.cnf_dict[section]:
                value = self.cnf_dict[section][option]
                if isinstance(value, dict):
                    value = MyCnfParser.get_str_from_dict(value)
                option_str = option + ' = ' + value + '\n'
                fp.write(option_str)
            fp.write('\n')
        fp.close()
        return file_path

    def update_rocksdb_options(self, db_options):
        # NOTE: this only supports updating default column-family for now
        rocks_opt = db_options.get_all_options()
        for option in rocks_opt:
            if option.endswith('dir') or option in self.OPTIONS_IGNORED:
                continue
            sec_type = '.'.join(option.split('.')[:-1])
            opt_name = option.split('.')[-1]
            if sec_type in self.OPTIONS_SEC_IGNORED:
                continue  # these options are to be ignored
            # get the value of the option from the OPTIONS file
            if NO_FAM in rocks_opt[option]:
                opt_value = rocks_opt[option][NO_FAM]
            elif 'default' in rocks_opt[option]:
                opt_value = rocks_opt[option]['default']
            else:
                continue  # these column families are not yet supported
            if not opt_value:
                continue
            # if it's a boolean convert it to '0' for false or '1' for true
            opt_value = MyCnfParser.try_map_boolean_value(opt_value)
            # update the value of this option in cnf, if present
            temp_name, temp_value = MyCnfParser.options_to_cnf_mapper(
                opt_name, opt_value
            )
            if temp_name and temp_value:
                key_hier = temp_name.split('.')
                final_dict = MyCnfParser.get_last_val_from_key_hier(
                    self.cnf_dict[self.MYSQLD], key_hier[:-1]
                )
                final_dict[key_hier[-1]] = temp_value
                continue  # this option has been handled
            # create the MySQL config supported name for this option
            loose_opt_name = 'loose-rocksdb_' + opt_name
            if (
                loose_opt_name in self.cnf_dict[self.MYSQLD] or
                NO_FAM in rocks_opt[option]
            ):
                # update its value
                self.cnf_dict[self.MYSQLD][loose_opt_name] = str(opt_value)
                continue  # this option has been updated
            if sec_type in self.OPT_SEC_KEY_HIERARCHY:
                key_hier = self.OPT_SEC_KEY_HIERARCHY[sec_type].split('.')
                final_dict = MyCnfParser.get_last_val_from_key_hier(
                    self.cnf_dict[self.MYSQLD], key_hier
                )
                final_dict[opt_name] = str(opt_value)
                continue  # this option has been handled
            # this option is not handled by this parser, raise error
            raise ValueError('MyCnfParser-option not handled: ' + option)


# TODO: move these methods to the unit tests of SysbenchRunner and MyCnfParser
def test_get_dict_from_str():
    string = (
        'write_buffer_size=128m;target_file_size_base=32m;' +
        'compression_per_level=kLZ4Compression;' +
        'bottommost_compression=kZSTD;compression_opts=-14:6:0;' +
        'block_based_table_factory={cache_index_and_filter_blocks=1;' +
        'filter_policy={bloomfilter=10;another=false};whole_key_filtering=0' +
        '};prefix_extractor=capped:12;level_compaction_dynamic_level_bytes' +
        '=true;soft_pending_compaction_bytes_limit=20480000000;ttl=1728000'
    )
    string = string.replace(' ', '')
    if not string.startswith('{'):
        string = '{' + string + '}'
    dict_of_dict = MyCnfParser.get_all_dictionaries(string)
    print(string)
    final_dict = MyCnfParser.get_dict_from_str(
        string, (0, len(string)-1), dict_of_dict
    )
    print(final_dict)


def main():
    test_get_dict_from_str()
    cnf_path = 'temp/my-3306.cnf'
    opt_path = 'temp/OPTIONS_boot.tmp'
    db_options = DatabaseOptions(opt_path)
    cnf = MyCnfParser(cnf_path)
    print(cnf.cnf_dict)
    cnf.update_rocksdb_options(db_options)
    print(cnf.generate_config_file(port=3308))
    print(cnf.col_fam)


if __name__ == "__main__":
    main()
