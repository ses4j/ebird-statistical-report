import datetime
import logging
import requests
from bs4 import BeautifulSoup
from .models import EBird
from diskcache import Cache
from django.db import connection

cache = Cache("cache_observer_name")

logger = logging.getLogger(__name__)

from .utils import get_observer_name


def fmtrow(r):
    return [fmt(x) for x in r]


def replace_unknown_name_if_needed(s):
    if s and isinstance(s, str) and s.startswith("unknown"):
        if "," in s:
            s_lst = [replace_unknown_name_if_needed(t.strip()) for t in s.split(",")]
            s = ", ".join(s_lst)
        else:
            s = get_observer_name(s[9:-1])

    return s


def fmt(s):
    s = replace_unknown_name_if_needed(s)
    if s and isinstance(s, str):
        s = (
            s.replace("\u00A0", " ")
            .encode("latin-1", "ignore")
            .decode("latin-1")
            .replace(" ", "\u00A0")
            .strip()
        )
    return s


def format_list_of_names(s):
    if s and isinstance(s, str):
        s = s.replace("\u00A0", " ").strip()

    lst = s.split(",")
    lst = [replace_unknown_name_if_needed(s.strip()) for s in lst]
    return ", ".join(lst)


def namedtuplefetchall(cursor):
    "Return all rows from a cursor as a namedtuple"
    desc = cursor.description

    return [fmtrow(row) for row in cursor.fetchall()]


@cache.memoize()
def execute_query(sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        vals = namedtuplefetchall(cursor)
        return cursor.description, vals
