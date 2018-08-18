import re
import time
from datetime import date, datetime

import pytz
import dateparser as dp
from freezegun import freeze_time
from dateutil.relativedelta import relativedelta
from lark import Lark, Transformer

from yasql.config import cfg

grammar = """
?start: "datetime |" expr                     -> dt_expr
      | "dt |" expr                           -> dt_expr
      | "timestamp |" expr                    -> ts_expr
      | "ts |" expr                           -> ts_expr

expr: datetime                                -> date_alone
     | "since" datetime                       -> since
     | "since" datetime "until" datetime      -> since_until
     | "in last" NUMBER UNIT                  -> in_last
     | "in last" NUMBER UNIT                  -> in_last

?datetime: abs_dt | rel_dt

abs_dt: YEAR | MONTH | DAY

rel_dt: CURRENT_DATE | LAST_DATE | X_AGO

CURRENT_DATE: "today" | /this (week|month|year)/
LAST_DATE: "yesterday" | /last (week|month|year)/
X_AGO: /\d+ (day|week|month|year)s? ago/
UNIT: /(day|week|month|year)s?/

YEAR: /\d{4}/
MONTH: /\d{4}-\d{2}/
DAY: /\d{4}-\d{2}-\d{2}/
TIME: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/

%import common.WS
%import common.NUMBER
%ignore WS
"""
parser = Lark(grammar)
identity = lambda x:x

dt_parse_func = dp.parse

def date_trunc(dt, unit):
    dt = {
        'year': lambda x: date(x.year, 1, 1),
        'month': lambda x: date(x.year, x.month, 1),
        'week': lambda x: x.date() - relativedelta(days=x.weekday()),
        'day': lambda x: x.date()
        }.get(unit, identity)(dt)
    if isinstance(dt, date):
        dt = datetime(dt.year, dt.month, dt.day)
    return dt

def dt_to_ts(dt, tz):
    if not tz:
        return int(dt.timestamp())
    return int(tz.localize(dt).timestamp())

def dt_to_str(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def convert_dt(item, converter, *args):
    if isinstance(item, (datetime, date)):
        return converter(item)
    return tuple((converter(i, *args) if i else None) for i in item)

class DateTransformer(Transformer):
    def __init__(self):
        super().__init__()
        self.tz = cfg.timezone

    def dt_expr(self, items):
        return convert_dt(items[0], dt_to_str)

    def ts_expr(self, items):
        return convert_dt(items[0], dt_to_ts, self.tz)

    def date_alone(self, items):
        item = items[0]
        unit = item.get('unit')
        dt = item['val']
        if unit == 'year':
            return dt, dt + relativedelta(years=1)
        elif unit == 'month':
            return dt, dt + relativedelta(months=1)
        elif unit == 'week':
            return dt, dt + relativedelta(days=7)
        elif unit == 'day':
            return dt, dt + relativedelta(days=1)
        return dt

    def since(self, items):
        item = items[0]
        return item['val'], None

    def since_until(self, items):
        start, end = items
        return start['val'], end['val']

    def in_last(self, items):
        num, unit = items
        return dt_parse_func('{} {} ago'.format(num, unit)), datetime.now()

    def abs_dt(self, items):
        item = items[0]
        if item.type == 'YEAR':
            val = dt_parse_func('{}-01-01'.format(item))
        elif item.type == 'MONTH':
            val = dt_parse_func('{}-01'.format(item))
        else:
            val = dt_parse_func(item)
        unit = item.type.lower()
        return {'val': date_trunc(val, unit), 'unit': unit}

    def rel_dt(self, items):
        item = items[0]
        unit = None
        if item.type in ['CURRENT_DATE', 'LAST_DATE']:
            if item in ['today', 'yesterday']:
                unit = 'day'
            else:
                unit = item.replace('this ', '').replace('last ','')
        val = date_trunc(dt_parse_func(item), unit)
        return {'val': val, 'unit': unit}

def parse_dt_expr(expr):
    tree = parser.parse(expr)
    return DateTransformer().transform(tree)

def is_dt_expr(expr):
    return bool(re.match('^(dt|datetime|ts|timestamp)\s+\|', expr))

if __name__ == '__main__':
    # print(parse("dt | 2018-01"))
    # print(parse("dt | 2018-01-01"))
    # print(parse("dt | last year"))
    # print(parse("dt | last week"))
    # print(parse("dt | this week"))
    # print(parse("dt | last month"))
    # print(parse("dt | this month"))
    # print(parse("dt | since last month"))
    # print(parse("dt | since 3 weeks ago"))
    # print(parse("dt | since last month until 3 days ago"))
    # print(parse("dt | since 2018-01-01 until 2018-07-15"))
    # print(parse("timestamp | in last 2 weeks"))
    # print(parse("ts | in last 2 weeks"))
    # print(parse_dt_expr("dt | in last 7 days"))
    with freeze_time('2018-01-08 8:00'):
        print(dt_parse_func('3 days ago'))
