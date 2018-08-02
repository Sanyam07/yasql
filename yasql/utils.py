import sqlparse

def sql_format(sql):
    sql = sql.strip()
    if not sql.endswith(';'):
        sql = sql + ';'
    return sqlparse.format(sql.strip(), reindent=True, keyword_case='upper')

