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

    def __str__(self):
        return 'Migration, up: %r, down: %r' % (self.sql_up, self.sql_down)
    
class Compound(BaseMigration):
    """
    A migration that is composed of one or more other migrations. DO NOT USE.
    """
    def __init__(self, migrations=[]):
        self.migrations = migrations
        super(Compound, self).__init__()

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

            print >> sys.stderr, termcolors.colorize(
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


class TableAlteration(object):
    reverse = False

    def __init__(self, clause_up, clause_down):
        if self.reverse:
            clause_up, clause_down = clause_down, clause_up

        self.clause_up = clause_up
        self.clause_down = clause_down

    def reversed(self):
        import copy
        c = copy.copy(self)
        c.clause_up, c.clause_down = self.clause_down, self.clause_up
        return c

class AlterTable(BaseMigration):
    reverse = False

    def __init__(self, table_name, changes):
        if self.reverse:
            self.changes = list(reversed(changes))
        else:
            self.changes = changes
        self.table_name = table_name
        for c in changes:
            assert isinstance(c, TableAlteration)

        super(AlterTable, self).__init__()

    def up(self):
        clauses = ',\n  '.join(c.clause_up for c in self.changes)
        sql = "ALTER TABLE `%s` %s;" % (self.table_name, clauses)
        self.execute_sql([sql])

    def down(self):
        clauses = ',\n  '.join(c.clause_down for c in reversed(self.changes))
        sql = "ALTER TABLE `%s` %s;" % (self.table_name, clauses)
        self.execute_sql([sql])


class AddColumn(AlterTable):
    "A migration that adds a database column"

    class Alteration(TableAlteration):
        add_column_sql = 'ADD COLUMN `%(colname)s` %(spec)s'
        drop_column_sql = 'DROP COLUMN `%(colname)s`'

        def __init__(self, column, spec, force_nulls=False):
            self.column, self.spec = column, spec

            args = {
                'colname': column,
                'spec': spec,
            }
            clause_up = self.add_column_sql % args
            if force_nulls:
                # if add_column_sql has NOT NULL in it, bin it
                clause_up = clause_up.replace(" NOT NULL", "")
            clause_down = self.drop_column_sql % args

            super(AddColumn.Alteration, self).__init__(clause_up, clause_down)

    class ConstraintAlteration(TableAlteration):
        add_sql = 'ADD CONSTRAINT `%(constraint)s`' \
            ' FOREIGN KEY (`%(colname)s`) REFERENCES `%(constraint_table)s` (`%(remote_col)s`)%(ondelete)s'
        drop_sql = 'DROP FOREIGN KEY `%(constraint)s`'

        def __init__(self, column, remote_table, constraint_name, remote_col='id', ondelete=''):
            self.column = column
            args = {
                'colname': column,
                'constraint': constraint_name,
                'constraint_table': remote_table,
                'remote_col': remote_col,
                'ondelete': ' ON DELETE %s' % ondelete if ondelete else '',
            }

            clause_up = self.add_sql % args
            clause_down = self.drop_sql % args

            super(AddColumn.ConstraintAlteration, self).__init__(clause_up, clause_down)

    def __init__(self, app, model, column, spec, constrain_to_table=None, ondelete=''):
        model = model.lower()
        self.app, self.model = app, model
        table_name = '%s_%s' % (app.lower(), model)
        if constrain_to_table:
            column = '%s_id' % column
            # this can only be used for ForeignKeys that link to another table's
            # id field. It is not for arbitrary relationships across tables!
            # Note also that this will create the ForeignKey field as allowing
            # nulls. Even if you don't want it to. This is because if it doesn't
            # allow null then the migration will blow up, because we're adding
            # the column without adding data to it. So you have to write another
            # migration later to change it from NULL to NOT NULL if you need to,
            # after you've populated it.

            # add the FK constraint
            constraint_name = self.fk_name(column, 'id', table_name, constrain_to_table)

            changes = [self.Alteration(column, spec, force_nulls=True),
                       self.ConstraintAlteration(column,
                                                 constrain_to_table,
                                                 constraint_name,
                                                 ondelete=ondelete)
                       ]
        else:
            changes = [self.Alteration(column, spec)]

        super(AddColumn, self).__init__(
            table_name, changes
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
    reverse = True

    class Alteration(AddColumn.Alteration):
        reverse = True

    class ConstraintAlteration(AddColumn.ConstraintAlteration):
        reverse = True

    def __str__(self):
        return super(DropColumn, self).replace('AddColumn', 'DropColumn')

class AddIndex(AlterTable):
    "A migration that adds an index (and removes it on down())"

    class Alteration(TableAlteration):
        add_index_sql = 'ADD INDEX `%(index)s` (%(cols)s)'
        drop_index_sql = 'DROP INDEX `%(index)s`'

        def __init__(self, columns, index_name):
            args = {
                'index': index_name,
                'cols': ', '.join('`%s`' % c for c in columns),
                }

            super(AddIndex.Alteration, self).__init__(
                self.add_index_sql % args,
                self.drop_index_sql % args,
                )

    def __init__(self, app, model, column, name=None):
        model = model.lower()
        app = app.lower()
        self.app, self.model = app, model
        table = '%s_%s' % (app, model)

        if isinstance(column, basestring):
            self.columns = [column]
        else:
            self.columns = column
        index_name = name if name else '%s_%s' % (table, '_'.join(self.columns))

        changes = [self.Alteration(self.columns, index_name)]
        super(AddIndex, self).__init__(table, changes)

    def __str__(self):
        return "AddIndex: app: %s, model: %s, column: %s" % (
            self.app, self.model, self.column
        )

class DropIndex(AddIndex):
    "Drops an index"

    reverse = True

    class Alteration(AddIndex.Alteration):
        reverse = True

    def __str__(self):
        return super(DropIndex, self).replace('AddIndex', 'DropIndex')

class AddDjangoKey(Migration):
    def __init__(self, table, col, f_table, f_col='id', reverse=False, keyname=None):
        self.table = table
        self.col = col
        self.f_table = f_table
        self.f_col = f_col
        self.reverse = reverse
        args = (table, col, f_table, f_col)

        sqls = [
            self.add_key_sql(key_name=keyname, *args),
            self.drop_key_sql(key_name=keyname, *args),
        ]

        if reverse:
            sqls.reverse()

        super(AddDjangoKey, self).__init__(
            sql_up=sqls[0], sql_down=sqls[1]
        )

    @classmethod
    def add_key_sql(cls, table, col, dest_table, remote_col='id', key_name=None):
        if key_name is None:
            key_name = cls.fk_name(col, remote_col, table, dest_table)
        return 'ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES %s (%s)' \
            % (table, key_name, col, dest_table, 'id')

    @classmethod
    def drop_key_sql(cls, table, col, dest_table, remote_col='id', key_name=None):
        if key_name is None:
            key_name = cls.fk_name(col, remote_col, table, dest_table)
        return 'ALTER TABLE `%s` DROP FOREIGN KEY `%s`' % (table, key_name)

    def run(self, direction):
        assert direction in ('up', 'down')
        from MySQLdb import OperationalError
        try:
            getattr(super(AddDjangoKey, self), direction)()
        except OperationalError:
            if (self.reverse and direction == 'up') or (not self.reverse and direction == 'down'):
                # if this is the drop key part
                print 'key is already gone'
            else:
                raise

    def up(self):
        self.run('up')

    def down(self):
        self.run('down')

    def __repr__(self):
        return "%sDjangoKey(*%r)" % (
            "Drop" if self.reverse else "Add",
            (self.table, self.col, self.f_table, self.f_col)
        )

class DropDjangoKey(AddDjangoKey):
    def __init__(self, *args, **kwargs):
        kwargs['reverse'] = True
        super(DropDjangoKey, self).__init__(*args, **kwargs)

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

class ChangeColumn(BaseMigration):
    def __init__(self, table, oldname, newname=None, old_def=None, new_def=None):
        self.table = table
        self.oldname = oldname
        self.newname = newname
        # if no definition is given, try a simple rename
        self.old_def = old_def
        self.new_def = new_def

        assert (new_def and old_def) or (not new_def and not old_def), \
            'if you specify a new or old definition, you must specify both'

        super(ChangeColumn, self).__init__()

    class AlreadyDone(Exception):
        pass

    class Failure(Exception):
        pass

    class Conflict(Exception):
        pass

    def introspect_sql(self, from_name, to_name, to_definition):
        defs = list(self.run_statements(['DESC `%s`' % self.table], return_rows=True))

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
                raise self.AlreadyDone('Column has already been renamed')
        elif new_def and old_def:
            raise self.Conflict('Cannot perform migration as column `%s` already exists.' % to_name)
        elif not old_def:
            raise self.Failure('Cannot perform migration as column `%s` does not exist.' % from_name)

        coltype, null, key, _, extra = old_def[1:]
        if key.upper() == 'PRI':
            raise self.Failure("I can't deal with primary keys! Aaargh! (`%s.%s`)" % (self.table, from_name))
        if extra:
            raise self.Failure("I can't deal with special column types! Aaargh! (`%s.%s` is %s)"
                               % (self.table, from_name, extra))

        if to_definition:
            newdef = to_definition
        else:
            nullclause = 'NOT NULL' if null.upper() == 'NO' else 'NULL'
            newdef = '%s %s' % (coltype, nullclause)

        return self.change_sql(from_name, to_name, newdef)

    def change_sql(self, from_name, to_name, to_def):
        return 'ALTER TABLE `%s` CHANGE `%s` `%s` %s' % (
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
