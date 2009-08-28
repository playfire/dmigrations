"""
These classes represent possible migrations. A migration is simply an object
with an up() method and a down() method - the down() method is allowed to 
raise an IrreversibleMigration exception. These objects are instances of 
subclasses of BaseMigration. Migration classes will be provided for stuff 
ranging from basic SQL migrations to more specialised things such as adding 
or removing an index.
"""
from dmigrations.migrations import BaseMigration
import re
import sys

from django.utils import termcolors

class IrreversibleMigrationError(Exception):
    pass

class Migration(BaseMigration):
    "Explict SQL migration, with sql for migrating both up and down"
    
    def __init__(self, sql_up, sql_down=None):
        self.sql_up = sql_up
        self.sql_down = sql_down
    
    def up(self):
        self.execute_sql(self.sql_up)
    
    def down(self):
        if self.sql_down:
            self.execute_sql(self.sql_down)
        else:
            raise IrreversibleMigrationError, 'No sql_down provided'
    
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
        
        from django.db import connection
        cursor = connection.cursor()
        
        for statement in statements:
            # Escape % due to format strings
            cursor.execute(statement.replace('%', '%%'))
        
        if return_rows:
            return cursor.fetchall()
    
    def __str__(self):
        return 'Migration, up: %r, down: %r' % (self.sql_up, self.sql_down)
    
class Compound(BaseMigration):
    """
    A migration that is composed of one or more other migrations. DO NOT USE.
    """
    def __init__(self, migrations=[]):
        self.migrations = migrations
    
    def run(self, direction, migs):
        successful = []
        try:
            for migration in migs:
                getattr(migration, direction)()
                successful.append(migration)
        except:
            rollback_dir = {
                'up': 'down',
                'down': 'up',
            }[direction]

            print >>sys.stderr, termcolors.colorize(
                'Got exception, rolling back %d successful migrations' % len(successful),
                fg='red'
            )

            for migration in reversed(successful):
                try:
                    getattr(migration, rollback_dir)()
                except Exception, e:
                    print e
            raise

    def up(self):
        self.run('up', self.migrations)

    def down(self):
        self.run('down', reversed(self.migrations))

    def __str__(self):
        return 'Compound Migration: %s' % self.migrations


class AddColumn(Migration):
    "A migration that adds a database column"
    
    add_column_sql = 'ALTER TABLE `%s_%s` ADD COLUMN `%s` %s;'
    drop_column_sql = 'ALTER TABLE `%s_%s` DROP COLUMN `%s`;'
    constrain_to_table_sql = 'ALTER TABLE `%s_%s` ADD CONSTRAINT %s FOREIGN KEY (`%s`) REFERENCES `%s` (`id`);'
    constrain_to_table_down_sql = 'ALTER TABLE `%s_%s` DROP FOREIGN KEY `%s`;'
    
    def __init__(self, app, model, column, spec, constrain_to_table=None):
        model = model.lower()
        self.app, self.model, self.column, self.spec = app, model, column, spec
        if constrain_to_table:
            # this can only be used for ForeignKeys that link to another table's
            # id field. It is not for arbitrary relationships across tables!
            # Note also that this will create the ForeignKey field as allowing
            # nulls. Even if you don't want it to. This is because if it doesn't
            # allow null then the migration will blow up, because we're adding
            # the column without adding data to it. So you have to write another
            # migration later to change it from NULL to NOT NULL if you need to,
            # after you've populated it.
            
            # add the FK constraint
            constraint_name = "%s_refs_id_%x" % (column, abs(hash((model,constrain_to_table))))
            sql_up = [self.constrain_to_table_sql % (app, model, constraint_name, "%s_id" % column, constrain_to_table)]
            
            sql_up.insert(0,self.add_column_sql % (app, model, "%s_id" % column, spec))
            sql_down = [self.drop_column_sql % (app, model, "%s_id" % column)]
            # if add_column_sql has NOT NULL in it, bin it
            sql_up[0] = sql_up[0].replace(" NOT NULL", "")
            # drop FK on sql_down
            sql_down.insert(0,self.constrain_to_table_down_sql % (app, model, constraint_name))
            
        else:
            sql_up = [self.add_column_sql % (app, model, column, spec)]
            sql_down = [self.drop_column_sql % (app, model, column)]
            
        super(AddColumn, self).__init__(
            sql_up,
            sql_down,
        )
    
    def __str__(self):
        return "AddColumn: app: %s, model: %s, column: %s, spec: %s" % (
            self.app, self.model, self.column, self.spec
        )

class DropColumn(AddColumn):
    """
    A migration that drops a database column. Needs the full column spec so 
    it can correctly create the down() method.
    """
    def __init__(self, *args, **kwargs):
        super(DropColumn, self).__init__(*args, **kwargs)
        # Now swap over the sql_up and sql_down properties
        self.sql_up, self.sql_down = self.sql_down, self.sql_up

    def __str__(self):
        return super(DropColumn, self).replace('AddColumn', 'DropColumn')

class AddIndex(Migration):
    "A migration that adds an index (and removes it on down())"
    
    add_index_sql = 'CREATE INDEX `%s` ON `%s_%s` (`%s`);'
    drop_index_sql = 'ALTER TABLE %s_%s DROP INDEX `%s`;'
    
    def __init__(self, app, model, column, name=None):
        model = model.lower()
        self.app, self.model = app, model
        if isinstance(column, basestring):
            self.columns = [column]
        else:
            self.columns = column
        if name:
            index_name = name
        else:
            index_name = '%s_%s_%s' % (app, model, '_'.join(self.columns))
        super(AddIndex, self).__init__(
            sql_up = [self.add_index_sql % (index_name, app, model,
                                            ', '.join('`%s`' % c for c in self.columns))],
            sql_down = [self.drop_index_sql % (app, model, index_name)],
        )
    
    def __str__(self):
        return "AddIndex: app: %s, model: %s, column: %s" % (
            self.app, self.model, self.column
        )

class DropIndex(AddIndex):
    "Drops an index"
    def __init__(self, app, model, column):
        super(DropIndex, self).__init__(app, model, column)
        self.sql_up, self.sql_down = self.sql_down, self.sql_up
    
    def __str__(self):
        return super(DropIndex, self).replace('AddIndex', 'DropIndex')

class InsertRows(Migration):
    "Inserts some rows in to a table"
    
    insert_row_sql = 'INSERT INTO `%s` (%s) VALUES (%s)'
    delete_rows_sql = 'DELETE FROM `%s` WHERE id IN (%s)'
    
    def __init__(self, table_name, columns, insert_rows, delete_ids):
        self.table_name = table_name
        sql_up = []
        from django.db import connection # so we can use escape_string
        connection.cursor() # Opens connection if not already open
        
        def escape(v):
            if v is None:
                return 'null'
            v = unicode(v) # In case v is an integer or long
            # escape_string wants a bytestring
            escaped = connection.connection.escape_string(v.encode('utf8'))
            # We get bugs if we use bytestrings elsewhere, so convert back to unicode
            # http://sourceforge.net/forum/forum.php?thread_id=1609278&forum_id=70461
            return u"'%s'" % escaped.decode('utf8')
        
        for row in insert_rows:
            values = ', '.join(map(escape, row))
            sql_up.append(
                self.insert_row_sql % (
                    table_name, ', '.join(map(str, columns)), values
                )
            )
        
        if delete_ids:
            sql_down = [self.delete_rows_sql % (table_name, ', '.join(map(str, delete_ids)))]
        else:
            sql_down = ["SELECT 1"]
        
        super(InsertRows, self).__init__(
            sql_up = ["BEGIN"] + sql_up + ["COMMIT"],
            sql_down = ["BEGIN"] + sql_down + ["COMMIT"],
        )

class RenameTable(Migration):
    def __init__(self, oldname, newname):
        self.oldname = oldname
        self.newname = newname

        sql_up  = 'RENAME TABLE `%s` TO `%s`' % (oldname, newname)
        sql_down = 'RENAME TABLE `%s` TO `%s`' % (newname, oldname)

        super(RenameTable, self).__init__(
            sql_up=sql_up, sql_down=sql_down
        )

    def __repr__(self):
        return 'RenameTable(%s, %s)' % (self.oldname, self.newname)

class ChangeColumn(Migration):
    def __init__(self, table, oldname, newname=None, old_def=None, new_def=None):
        self.table = table
        self.oldname = oldname
        self.newname = newname
        # if no definition is given, try a simple rename
        self.old_def = old_def
        self.new_def = new_def

        assert (new_def and old_def) or (not new_def and not old_def), \
            'if you specify a new or old definition, you must specify both'

    class AlreadyDone(Exception):
        pass

    class Failure(Exception):
        pass

    class Conflict(Exception):
        pass

    def introspect_sql(self, from_name, to_name, to_definition):
        from django.db import connection
        cursor = connection.cursor()

        cursor.execute('DESC `%s`' % self.table)
        defs = list(cursor.fetchall())

        old_def, new_def = None, None

        for d in defs:
            if d[0] == from_name:
                old_def = d
            elif d[0] == to_name:
                new_def = d

        if new_def and not old_def:
            if to_definition and to_name:
                raise self.Failure('Cannot perform migration as column `%s` already exists.' % to_name)
            else:
                raise self.AlreadyDone()
        elif new_def and old_def:
            raise self.Conflict('Cannot perform migration as column `%s` already exists.' % to_name)
        elif not old_def:
            raise self.Failure('Cannot perform migration as column `%s` does not exist.' % from_name)

        coltype, null, key, default, extra = old_def[1:]
        if key.upper() == 'PRI':
            raise self.Failure("I can't deal with primary keys! Aaargh! (`%s.%s`)" % (self.table, from_name))
        if extra:
            raise self.Failure("I can't deal with special column types! Aaargh! (`%s.%s` is %s)"
                               % (self.table, from_name, extra))

        if to_definition:
            newdef = to_definition
        else:
            nullclause = 'NOT NULL' if null.upper() == 'NO' else 'NULL';
            newdef = '%s %s' % (coltype, nullclause)

        return self.change_sql(from_name, to_name, newdef)

    def change_sql(self, from_name, to_name, to_def):
        return 'ALTAER TABLE `%s` CHANGE `%s` `%s` %s' % (
            self.table,
            from_name,
            to_name,
            to_def,
            )

    def up(self):
        try:
            sql = self.introspect_sql(self.oldname, self.newname or self.oldname, self.new_def)
        except self.AlreadyDone, e:
            print e
            return

        self.execute_sql(sql)

    def down(self):
        try:
            sql = self.introspect_sql(self.newname or self.oldname, self.oldname, self.old_def)
        except self.AlreadyDone, e:
            print e
            return

        self.execute_sql(sql)
