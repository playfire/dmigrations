from common import *
from django.core.management import call_command
import sys
from StringIO import StringIO

class CommandsTest(TestCase):
  def set_up(self):
    self.stdout = sys.stdout

  def tear_down(self):
    sys.stdout = self.stdout

  def test_that_syncdb_raises_exception(self):
    try:
      call_command('syncdb')
    except Exception, e:
      self.assert_equal("Use migrations not syncdb - ./manage.py help dmigrate for help", str(e))
    else:
      self.assert_("Exception should be raised by syncdb")

  def test_that_fix_permissions_doesnt_break(self):
    # NOTE: Very basic test, it could as well do nothing and pass
    call_command('fix_permissions')

  def pipe_command(self, *args, **kwargs):
    sys.stdout = StringIO()
    call_command(*args, **kwargs)
    res = sys.stdout.getvalue()
    sys.stdout = self.stdout
    return res

  def test_migration(self):
    try:
      import gcap.apps.london_guide.models
    except ImportError, e:
      # NOTE: Disable on asset-manager branch
      return
    
    actual = self.pipe_command('migration', "addcolumn", "london_guide", "venue", "company", output=True)
    expected = """from gcap.apps.dmigrations import migrations as m\nimport datetime\n
migration = m.AddColumn('london_guide', 'venue', 'company', 'varchar(256) NOT NULL')\n\n"""
    self.assert_equal(expected, actual)


    actual = self.pipe_command('migration', "new", "something", output=True)
    expected = """from gcap.apps.dmigrations import migrations as m

class CustomMigration(m.Migration):
    def __init__(self):
        sql_up = []
        sql_down = []
        super(MyMigration, self).__init__(sql_up=sql_up, sql_down=sql_down)
    # Or override the up() and down() methods\n\nmigration = CustomMigration()\n\n"""
    self.assert_equal(expected, actual)

    actual = self.pipe_command('migration', "addindex", "london_guide", "venue", "company", output=True)
    expected = """from gcap.apps.dmigrations import migrations as m\nimport datetime\n\nmigration = m.AddIndex('london_guide', 'venue', 'company')\n\n"""
    self.assert_equal(expected, actual)

    # Simply check they don't raise an exception
    actual = self.pipe_command('migration', "app", "london_guide", output=True)

    actual = self.pipe_command('migration', "addtable", "london_guide", "venue", output=True)

    actual = self.pipe_command('migration', "insert", "articles", "article", output=True)
