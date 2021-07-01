# module pyparsing.py
#
# Copyright (c) 2003-2019  Paul T. McGuire
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# flake8: noqa
from pyparsing import (
    Literal,
    Word,
    Group,
    Forward,
    alphas,
    alphanums,
    Regex,
    CaselessKeyword,
    Suppress,
    delimitedList,
)
import math
import operator

# map operator symbols to corresponding arithmetic operations
epsilon = 1e-12
opn = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "^": operator.pow,
}

fn = {
    "sin": math.sin,
    "cos": math.cos,
    "tan": math.tan,
    "exp": math.exp,
    "abs": abs,
    "trunc": lambda a: int(a),
    "round": round,
    "sgn": lambda a: -1 if a < -epsilon else 1 if a > epsilon else 0,
}

exprStack = []


def push_first(toks):
    exprStack.append(toks[0])


def push_unary_minus(toks):
    for t in toks:
        if t == "-":
            exprStack.append("unary -")
        else:
            break


def BNF():
    """
    expop   :: '^'
    multop  :: '*' | '/'
    addop   :: '+' | '-'
    integer :: ['+' | '-'] '0'..'9'+
    atom    :: PI | E | real | fn '(' expr ')' | '(' expr ')'
    factor  :: atom [ expop factor ]*
    term    :: factor [ multop factor ]*
    expr    :: term [ addop term ]*
    """
    # use CaselessKeyword for e and pi, to avoid accidentally matching
    # functions that start with 'e' or 'pi' (such as 'exp'); Keyword
    # and CaselessKeyword only match whole words
    e = CaselessKeyword("E")
    pi = CaselessKeyword("PI")
    # fnumber = Combine(Word("+-"+nums, nums) +
    #                    Optional("." + Optional(Word(nums))) +
    #                    Optional(e + Word("+-"+nums, nums)))
    # or use provided pyparsing_common.number, but convert back to str:
    # fnumber = ppc.number().addParseAction(lambda t: str(t[0]))
    fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
    ident = Word(alphas, alphanums + "_$")

    plus, minus, mult, div = map(Literal, "+-*/")
    lpar, rpar = map(Suppress, "()")
    addop = plus | minus
    multop = mult | div
    expop = Literal("^")

    expr = Forward()
    expr_list = delimitedList(Group(expr))
    # add parse action that replaces the function identifier with a (name, number of args) tuple
    fn_call = (ident + lpar - Group(expr_list) + rpar).setParseAction(
        lambda t: t.insert(0, (t.pop(0), len(t[0])))
    )
    atom = (
        addop[...]
        + (
            (fn_call | pi | e | fnumber | ident).setParseAction(push_first)
            | Group(lpar + expr + rpar)
        )
    ).setParseAction(push_unary_minus)

    # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left
    # exponents, instead of left-to-right that is, 2^3^2 = 2^(3^2), not (2^3)^2.
    factor = Forward()
    factor <<= atom + (expop + factor).setParseAction(push_first)[...]
    term = factor + (multop + factor).setParseAction(push_first)[...]
    expr <<= term + (addop + term).setParseAction(push_first)[...]
    bnf = expr

    return bnf


def evaluate_stack(s, stats):
    op, num_args = s.pop(), 0
    if isinstance(op, tuple):
        op, num_args = op
    if op == "unary -":
        return -evaluate_stack(s, stats)
    if op in "+-*/^":
        # note: operands are pushed onto the stack in reverse order
        op2 = evaluate_stack(s, stats)
        op1 = evaluate_stack(s, stats)
        return opn[op](op1, op2)
    elif op == "PI":
        return math.pi  # 3.1415926535
    elif op == "E":
        return math.e  # 2.718281828
    elif op == "mean":
        return stats['mean']
    elif op == "min":
        return stats['min']
    elif op == "max":
        return stats['max']
    elif op == "std":
        return stats['std']
    elif op in fn:
        # note: args are pushed onto the stack in reverse order
        args = reversed([evaluate_stack(s, stats) for _ in range(num_args)])
        return fn[op](*args)
    elif op[0].isalpha():
        raise Exception("invalid identifier '%s'" % op)
    else:
        return float(op)


def eval_fx(fx, stats):
    """Given fx and stats ('min', 'max', 'mean', 'std') return the result"""
    _ = BNF().parseString(fx, parseAll=True)
    val = evaluate_stack(exprStack[:], stats)

    return val
