# Utilities to create google sheets queries.

import itertools
import string

def kleene_star(alphabet):
    """Generates all finite strings over an alphabet"""
    for i in itertools.count():
        for x in itertools.product(*([alphabet] * i)):
            yield ''.join(x)

def gen_nth(gen, n):
    """Returns the nth element of a generator."""
    return next(itertools.islice(gen, n, n + 1))

def header(n):
    """Returns the column name of the nth column in a google sheet.

    In a google sheet, the columns go A, B, ..., Z, AA, AB, ....
    """
    return gen_nth(kleene_star(string.ascii_uppercase), n + 1)
