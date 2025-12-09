import re
from beartype.door import is_bearable
from typing import Callable

from s_expression import Expression
from s_expression.operators import Value, Sum, Avg, Max, Min, Prop, Join
from utils.custom_types import UnitCompatibilityError

Atom   = str
List   = list
Exp    = (Atom, List)
Env    = dict


def standard_env() -> Env:
    """This function defines the set (environment) of operators possible and their procedures when called."""
    env = Env()
    env.update({
        'MSR':      lambda _, x: x,
        'DIM':      lambda _, group, codes: (group, codes),
        # This could be done automatically if the __init__.py would contain the import of the operators,
        #  but it gets confused when deciding what class to call when creating the env as
        #  c.operator(): lambda *x: c(x) for c in Expression.__subclasses__()
        'VALUE':    lambda sexp, *x: Value(sexp, *x),
        'SUM':      lambda sexp, *x: Sum(sexp, *x),
        'AVG':      lambda sexp, *x: Avg(sexp, *x),
        'MAX':      lambda sexp, *x: Max(sexp, *x),
        'MIN':      lambda sexp, *x: Min(sexp, *x),
        'PROP':     lambda sexp, *x: Prop(sexp, *x),
        'JOIN':     lambda sexp, *x: Join(sexp, *x),
    })
    return env

global_env = standard_env()


def parse(program: str) -> Exp:
    """
        Parse a string into a valid S-expression.
        Parser taken from https://rosettacode.org/wiki/S-expressions#Python

        :param program: S-expression in string format
        :returns: parsed S-expression instance following the given input
    """
    TERM_REGEX = rf'''(?mx)
        (?P<brackl>\s?\{Expression.SOS})|
        (?P<brackr>\{Expression.EOS})|
        (?P<sq>"[^"]*")|
        (?P<tok>[^(^)\s]+)|
        (?P<s>\s)
    '''
    # Parse the string into a hierarchy
    stack = []
    out = []
    for match in re.finditer(TERM_REGEX, program):
        term, value = [(t, v) for t, v in match.groupdict().items() if v][0]

        if term == 'brackl':
            stack.append(out)
            out = []
        elif term == 'brackr':
            assert stack, "Trouble with nesting of brackets"
            tmpout, out = out, stack.pop(-1)
            out.append(tmpout)
        elif term == 'tok':
            out.append(value)
        elif term == 'sq':
            out.append(value[1:-1])
        elif term == 's':
            continue
        else:
            raise NotImplementedError(f"Error: {(term, value)}")

    assert not stack, "Trouble with nesting of brackets"
    return out


def eval(x: Exp, env: Env = global_env, **kwargs) -> Exp:
    """
        Evaluate a parsed expression tree and instantiate the relevant expression classes.
        Inspiration taken from https://norvig.com/lispy.html

        :param x: a Lisp expression
        :param env: dictionary containing (function) definitions for keywords and environment variables
        :returns: an evaluated (i.e. instantiated) sub expression
    """
    if isinstance(x, Atom) and x in env:
        # Variable/function reference from the environment
        return env[x]
    elif isinstance(x, Atom):
        # Constant
        return x
    elif is_bearable(x, List[str]) and (len(x) == 0 or x[0] not in env):
        # Flat list with constants
        return x
    else:
        # Procedure call
        proc = eval(x[0], env, **kwargs)
        if not isinstance(proc, Callable):
            raise SyntaxError(f"Unknown function: {proc}")

        args = [eval(arg, env, **kwargs) for arg in x[1:]]

        try:
            if isinstance(proc, Expression):
                return proc(**kwargs)
            return proc(x, *args)
        except UnitCompatibilityError as e:
            raise e
        except TypeError as e:
            raise SyntaxError(f"Invalid expression syntax: {e}")
