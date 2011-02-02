import re

class BaseMigration(object):
    def up(self):
        raise NotImplementedError
    
    def down(self):
        raise NotImplementedError

    def execute_sql(self, sql, return_rows=False):
        "Executes sql, which can be a string or a list of strings"
        if isinstance(sql, basestring):
            # Split string in to multiple statements
            statements_re = re.compile(r";[ \t]*$", re.M)
            statements = [s for s in statements_re.split(sql) if s.strip()]
        else:
            try:
                # Assume each item in the iterable is already an individual statement
                statements = iter(sql)
            except TypeError:
                assert False, 'sql argument must be string or iterable'

        return self.run_statements(statements, return_rows)

    def run_statements(self, statements, return_rows=False):
        from django.db import connection
        cursor = connection.cursor()

        for statement in statements:
            # Escape % due to format strings
            try:
                cursor.execute(statement.replace('%', '%%'))
            except:
                print "Exception running %r" % statement
                raise

        if return_rows:
            return cursor.fetchall()

    @classmethod
    def _digest(cls, *args):
        "Generate a 32 bit digest of a set of arguments that can be used to shorten identifying names"
        return '%x' % (abs(hash(args)) % (1<<32))

    @classmethod
    def fk_name(cls, col, remote_col, table, remote_table):
        return '%s_refs_%s_%s' % (remote_col, col, cls._digest(remote_table, table))
