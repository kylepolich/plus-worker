import feaas.objects as objs
from feaas.abstract import AbstractAction
import markdown2

def levenshtein_distance(s1, s2):
    """Compute the Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        s1, s2 = s2, s1  # Ensure s1 is the longer string

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


# This makes for some re-use of code, making the overall script smaller.

s_param = objs.Parameter(
    var_name='s',
    label='A String',
    ptype=objs.ParameterType.STRING)

i_param = objs.Parameter(
    var_name='val',
    label='An Integer',
    ptype=objs.ParameterType.INTEGER)

b_param = objs.Parameter(
    var_name='b',
    label='A Boolean',
    ptype=objs.ParameterType.BOOLEAN)

class StringUcase(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        TEXT = objs.Parameter(
            var_name='TEXT',
            label='TEXT',
            ptype=objs.ParameterType.STRING)
        outputs = [TEXT]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = s.upper()
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='TEXT', outputs={ 'TEXT': output })


class StringLcase(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        text = objs.Parameter(
            var_name='text',
            label='text',
            ptype=objs.ParameterType.STRING)
        outputs = [text]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = s.lower()
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='text', outputs={ 'text': output })


class Markdown(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        html = objs.Parameter(
            var_name='html',
            label='HTML',
            ptype=objs.ParameterType.STRING)
        outputs = [html]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = markdown2.markdown(s)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='html', outputs={ 'html': output })


class StringReplace(AbstractAction):

    def __init__(self, dao):
        needle = objs.Parameter(
            var_name='needle',
            label='needle',
            ptype=objs.ParameterType.STRING)        
        haystack = objs.Parameter(
            var_name='haystack',
            label='haystack',
            ptype=objs.ParameterType.STRING)        
        new_value = objs.Parameter(
            var_name='new_value',
            label='new_value',
            ptype=objs.ParameterType.STRING)        
        params = [needle, haystack, new_value]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, needle, haystack, new_value) -> objs.Receipt:
        out = haystack.replace(needle, new_value)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=out)
        return objs.Receipt(success=True, primary_output='s', outputs={ "s": output })


class StringLen(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        val = objs.Parameter(
            var_name='val',
            label='val',
            ptype=objs.ParameterType.INTEGER)
        outputs = [val]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = len(s)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=val)
        return objs.Receipt(success=True, primary_output='val', outputs={ 'val': output })


class StringRev(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        text = objs.Parameter(
            var_name='text',
            label='text',
            ptype=objs.ParameterType.STRING)
        outputs = [text]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = s[::-1]
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='text', outputs={ 'text': output })


class StringConcat(AbstractAction):

    def __init__(self, dao):
        str_1 = objs.Parameter(
            var_name='str_1',
            label='str_1',
            ptype=objs.ParameterType.STRING)        
        str_2 = objs.Parameter(
            var_name='str_2',
            label='str_2',
            ptype=objs.ParameterType.STRING)              
        params = [str_1, str_2]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, str_1, str_2) -> objs.Receipt:
        out = str_1 + str_2
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=out)
        return objs.Receipt(success=True, primary_output='s', outputs={ "s": output })


class StringSubstring(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        start = objs.Parameter(
            var_name='start',
            label='start',
            ptype=objs.ParameterType.INTEGER)
        end = objs.Parameter(
            var_name='end',
            label='end',
            ptype=objs.ParameterType.INTEGER)              
        params = [str, start, end]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, str, start, end) -> objs.Receipt:
        out = str[start:end]
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=out)
        return objs.Receipt(success=True, primary_output='s', outputs={ "s": output })


class StringTrim(AbstractAction):

    def __init__(self, dao):
        params = [s_param]
        text = objs.Parameter(
            var_name='text',
            label='text',
            ptype=objs.ParameterType.STRING)
        outputs = [text]
        super().__init__(params, outputs)


    def execute_action(self, s) -> objs.Receipt:
        val = s.strip()
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='text', outputs={ 'text': output })


class StringPadStart(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        n_pad = objs.Parameter(
            var_name='n_pad',
            label='n_pad',
            ptype=objs.ParameterType.INTEGER)
        char_pad = objs.Parameter(
            var_name='char_pad',
            label='char_pad',
            ptype=objs.ParameterType.STRING)              
        params = [str, n_pad, char_pad]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, str, n_pad, char_pad) -> objs.Receipt:
        val = str.rjust(n_pad, char_pad)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='text', outputs={ 'text': output })


class StringPadEnd(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        n_pad = objs.Parameter(
            var_name='n_pad',
            label='n_pad',
            ptype=objs.ParameterType.INTEGER)
        char_pad = objs.Parameter(
            var_name='char_pad',
            label='char_pad',
            ptype=objs.ParameterType.STRING)              
        params = [str, n_pad, char_pad]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, str, n_pad, char_pad) -> objs.Receipt:
        val = str.ljust(n_pad, char_pad)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=val)
        return objs.Receipt(success=True, primary_output='text', outputs={ 'text': output })


class StringContains(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        if substring in str:
            out = True
        else:
            out = False
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringFirstIndex(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [i_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        out = str.find(substring)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=out)
        return objs.Receipt(success=True, primary_output='val', outputs={ "val": output })


class StringLastIndex(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [i_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        out = str.rfind(substring)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=out)
        return objs.Receipt(success=True, primary_output='val', outputs={ "val": output })


class StringStartsWith(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        out = str.startswith(substring)
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringEndsWith(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        out = str.endswith(substring)
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringIsEmpty(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)                   
        params = [str]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str) -> objs.Receipt:
        if len(str) == 0:
            out = True
        else:
            out = False
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })

    
class StringIsAlpha(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)                   
        params = [str]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str) -> objs.Receipt:
        out = str.isalpha()
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringIsNumeric(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)                   
        params = [str]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str) -> objs.Receipt:
        out = str.isnumeric()
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringIsAlphaNum(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)                   
        params = [str]
        outputs = [b_param]
        super().__init__(params, outputs)


    def execute_action(self, str) -> objs.Receipt:
        out = str.isalnum()
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=out)
        return objs.Receipt(success=True, primary_output='b', outputs={ "b": output })


class StringCountSubstring(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)        
        substring = objs.Parameter(
            var_name='substring',
            label='substring',
            ptype=objs.ParameterType.STRING)              
        params = [str, substring]
        outputs = [i_param]
        super().__init__(params, outputs)


    def execute_action(self, str, substring) -> objs.Receipt:
        val = str.lower().count(substring.lower()) #not case sensitive
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=val)
        return objs.Receipt(success=True, primary_output='val', outputs={ 'val': output })


class StringTitleCase(AbstractAction):

    def __init__(self, dao):
        str = objs.Parameter(
            var_name='str',
            label='str',
            ptype=objs.ParameterType.STRING)                   
        params = [str]
        outputs = [s_param]
        super().__init__(params, outputs)


    def execute_action(self, str) -> objs.Receipt:
        out = str.title()
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=out)
        return objs.Receipt(success=True, primary_output='s', outputs={ "s": output })


class StringLevenshtein(AbstractAction):

    def __init__(self, dao):
        str_1 = objs.Parameter(
            var_name='str_1',
            label='str_1',
            ptype=objs.ParameterType.STRING)
        str_2 = objs.Parameter(
            var_name='str_2',
            label='str_2',
            ptype=objs.ParameterType.STRING)
        i_param = objs.Parameter(
            var_name='val',
            label='Levenshtein Distance',
            ptype=objs.ParameterType.INTEGER)
        params = [str_1, str_2]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, str_1, str_2) -> objs.Receipt:
        val = levenshtein_distance(str_1, str_2)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=val)
        return objs.Receipt(success=True, primary_output='val', outputs={ 'val': output })
