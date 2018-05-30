from enum import Enum


class IniParser:
    class Element(Enum):
        rule = 1
        cond = 2
        sugg = 3
        key_val = 4
        comment = 5

    @staticmethod
    def remove_trailing_comment(line):
        line = line.strip()
        comment_start = line.find('#')
        if comment_start > -1:
            return line[:comment_start]
        return line

    @staticmethod
    def is_section_header(line):
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            return True
        return False

    @staticmethod
    def get_section_name(line):
        token_list = line.strip()[1:-1].split('"')
        if len(token_list) < 3:
            error = 'section header be like: [<section_type> "<section_name>"]'
            raise ValueError('Parsing error: ' + error + '\n' + line)
        return token_list[1]

    @staticmethod
    def get_element(line):
        line = IniParser.remove_trailing_comment(line)
        if not line:
            return IniParser.Element.comment
        if IniParser.is_section_header(line):
            if line.strip()[1:-1].startswith('Suggestion'):
                return IniParser.Element.sugg
            if line.strip()[1:-1].startswith('Rule'):
                return IniParser.Element.rule
            if line.strip()[1:-1].startswith('Condition'):
                return IniParser.Element.cond
        if '=' in line:
            return IniParser.Element.key_val
        error = 'not a recognizable RulesSpec element'
        raise ValueError('Parsing error: ' + error + '\n' + line)

    @staticmethod
    def get_key_value_pair(line):
        line = line.strip()
        key = line.split('=')[0].strip()
        value = line.split('=')[1].strip()
        if not value:
            return (key, None)
        values = IniParser.get_list_from_value(value)
        if len(values) == 1:
            return (key, value)
        return (key, values)

    @staticmethod
    def get_list_from_value(value):
        values = value.strip().split(':')
        return values
