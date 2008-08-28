from gcap.apps.dmigrations.tests.common import *
from gcap.apps.dmigrations.migration_state import MigrationState
from gcap.apps.dmigrations.migration_db import MigrationDb

create_old = """
CREATE TABLE `smigrations_schema` (
  `id` int(11) NOT NULL auto_increment,
  `version` int(11) NOT NULL,
  `scratch_version` int(11) NOT NULL,
   PRIMARY KEY  (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8
"""

create_new = """
CREATE TABLE `migrations` (
  `id` int(11) NOT NULL auto_increment,
  `migration` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

class MigrationStateTest(TestCase):
  """
  Test migration from the old migration system to the new migration system.
  It messes in the database, so is likely to break if you change the test loader.
  """
  
  def set_up(self):
    from django.db import connection
    self.cursor = connection.cursor()

    try: self.cursor.execute("DROP TABLE smigrations_schema")
    except: pass
    try: self.cursor.execute("DROP TABLE migrations")
    except: pass

  def set_schema_version(self, number):
    self.cursor.execute("DELETE FROM smigrations_schema")
    self.cursor.execute("INSERT INTO smigrations_schema VALUES (1, %d, 0)" % number)

  def test_table_present_methods(self):
    si = MigrationState()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    self.cursor.execute(create_old)

    self.assert_equal(True, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    self.cursor.execute(create_new)

    self.assert_equal(True, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())
    
    self.cursor.execute("DROP TABLE smigrations_schema")
    
    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())
 
  def test_list_of_old_migrations_applied(self):
    self.cursor.execute(create_old)

    db = MigrationDb(migrations = ['001_foo', '002_bar', '005_hello'])
    si = MigrationState(migration_db=db)

    self.set_schema_version(1)
    self.assert_equal(["001_foo"], si.list_of_old_migrations_applied())
    self.set_schema_version(2)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(3)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(4)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(5)
    self.assert_equal(["001_foo", "002_bar", "005_hello"], si.list_of_old_migrations_applied())
    self.set_schema_version(6)
    self.assert_equal(["001_foo", "002_bar", "005_hello"], si.list_of_old_migrations_applied())

  def test_list_of_old_migrations_applied_dev(self):
    self.cursor.execute(create_old)

    db = MigrationDb(migrations = ['001_foo', '002_bar', '003_DEV_kitty', '005_hello'])
    si = MigrationState(migration_db=db, dev=True)

    self.set_schema_version(1)
    self.assert_equal(["001_foo"], si.list_of_old_migrations_applied())
    self.set_schema_version(2)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(3)
    self.assert_equal(["001_foo", "002_bar", '003_DEV_kitty'], si.list_of_old_migrations_applied())
    self.set_schema_version(4)
    self.assert_equal(["001_foo", "002_bar", '003_DEV_kitty'], si.list_of_old_migrations_applied())
    self.set_schema_version(5)
    self.assert_equal(["001_foo", "002_bar", '003_DEV_kitty', "005_hello"], si.list_of_old_migrations_applied())
    self.set_schema_version(6)
    self.assert_equal(["001_foo", "002_bar", '003_DEV_kitty', "005_hello"], si.list_of_old_migrations_applied())

  def test_list_of_old_migrations_applied_nodev(self):
    self.cursor.execute(create_old)

    db = MigrationDb(migrations = ['001_foo', '002_bar', '003_DEV_kitty', '005_hello'])
    si = MigrationState(migration_db=db, dev=False)

    self.set_schema_version(1)
    self.assert_equal(["001_foo"], si.list_of_old_migrations_applied())
    self.set_schema_version(2)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(3)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(4)
    self.assert_equal(["001_foo", "002_bar"], si.list_of_old_migrations_applied())
    self.set_schema_version(5)
    self.assert_equal(["001_foo", "002_bar", "005_hello"], si.list_of_old_migrations_applied())
    self.set_schema_version(6)
    self.assert_equal(["001_foo", "002_bar", "005_hello"], si.list_of_old_migrations_applied())

  def test_init_on_fresh_db_creates_new_table(self):
    si = MigrationState()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    si.init()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

  def test_init_on_initialized_db_does_nothing(self):
    self.cursor.execute(create_new)

    si = MigrationState()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    si.init()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

  def test_applying_and_unapplying(self):
    def assert_applied(f, b, h):
      self.assert_equal([f, b, h], [si.is_applied('001_foo'), si.is_applied('002_bar'), si.is_applied('005_hello')])
  
    si = MigrationState()
    self.cursor.execute(create_new)
    
    assert_applied(False, False, False)
    
    si.mark_as_applied('001_foo')
    assert_applied(True, False, False)

    si.mark_as_applied('005_hello')
    assert_applied(True, False, True)

    si.mark_as_applied('005_hello')
    assert_applied(True, False, True)

    si.mark_as_unapplied('002_bar')
    assert_applied(True, False, True)

    si.mark_as_unapplied('005_hello')
    assert_applied(True, False, False)

    si.mark_as_applied('002_bar')
    assert_applied(True, True, False)

  def test_init_on_old_db_migrates_dev(self):
    self.cursor.execute(create_old)
    self.set_schema_version(4)

    db = MigrationDb(migrations = ['001_foo', '002_bar', '003_DEV_kitty', '005_hello'])
    si = MigrationState(migration_db=db, dev=True)

    self.assert_equal(True, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    si.init()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    self.assert_equal(True, si.is_applied('001_foo'))
    self.assert_equal(True, si.is_applied('002_bar'))
    self.assert_equal(True, si.is_applied('003_DEV_kitty'))
    self.assert_equal(False, si.is_applied('005_hello'))

  def test_init_on_old_db_migrates_nodev(self):
    self.cursor.execute(create_old)
    self.set_schema_version(4)

    db = MigrationDb(migrations = ['001_foo', '002_bar', '003_DEV_kitty', '005_hello'])
    si = MigrationState(migration_db=db, dev=False)

    self.assert_equal(True, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    si.init()

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    self.assert_equal(True, si.is_applied('001_foo'))
    self.assert_equal(True, si.is_applied('002_bar'))
    self.assert_equal(False, si.is_applied('003_DEV_kitty'))
    self.assert_equal(False, si.is_applied('005_hello'))

  def test_old_schema_version(self):
    self.cursor.execute(create_old)
    si = MigrationState()
    
    self.assert_equal(0, si.get_old_schema_version())
    self.set_schema_version(4)
    self.assert_equal(4, si.get_old_schema_version())
    self.set_schema_version(17)
    self.assert_equal(17, si.get_old_schema_version())
    self.set_schema_version(7)
    self.assert_equal(7, si.get_old_schema_version())
   

  def test_that_db_init_requires_explicit_dev_flag_if_it_would_affect_the_results(self):
    self.cursor.execute(create_old)
    db = MigrationDb(migrations = ['001_foo', '002_bar', '003_DEV_kitty', '005_hello'])

    self.set_schema_version(4)
    si = MigrationState(migration_db=db)
    self.assert_raises(Exception, lambda: si.list_of_old_migrations_applied())

    si = MigrationState(migration_db=db, dev=True)
    self.assert_equal(['001_foo', '002_bar', '003_DEV_kitty'], si.list_of_old_migrations_applied())
    si = MigrationState(migration_db=db, dev=False)
    self.assert_equal(['001_foo', '002_bar'], si.list_of_old_migrations_applied())

    self.set_schema_version(2)
    si = MigrationState(migration_db=db)
    self.assert_equal(['001_foo', '002_bar'], si.list_of_old_migrations_applied())
    si = MigrationState(migration_db=db, dev=True)
    self.assert_equal(['001_foo', '002_bar'], si.list_of_old_migrations_applied())
    si = MigrationState(migration_db=db, dev=False)
    self.assert_equal(['001_foo', '002_bar'], si.list_of_old_migrations_applied())

  def test_db_init_handles_dups_correctly(self):
    self.cursor.execute(create_old)
    db = MigrationDb(migrations = ['001_foo', '002_bar', '005_omg', '005_hello'])
    si = MigrationState(migration_db=db)

    self.set_schema_version(5)
    self.assert_raises(Exception, lambda: si.list_of_old_migrations_applied())

    self.set_schema_version(4)
    self.assert_equal(['001_foo', '002_bar'], si.list_of_old_migrations_applied())

  def assert_plans(self, si, *plans):
    while plans:
      query, expected_plan, plans = plans[0], plans[1], plans[2:]
      
      if isinstance(expected_plan, type(Exception)) and issubclass(expected_plan, Exception):
        self.assert_raises(expected_plan, lambda: si.plan(*query))
      else:
        actual_plan =  si.plan(*query)
        self.assert_equal(expected_plan, actual_plan, u"Plan for %s expected to be %s but was %s" % (query, expected_plan, actual_plan))

  def test_plan(self):
    db = MigrationDb(migrations = ['001_foo', '002_bar', '005_omg', '005_hello'])
    si = MigrationState(migration_db=db)

    si.init()
    
    self.assert_plans(si,
      ['apply', '1'],         [('001_foo', 'up')],
      ['apply', '001_foo'],   [('001_foo', 'up')],
      ['apply', '1', '2'],    [('001_foo', 'up'), ('002_bar', 'up')],
      ['unapply', '001_foo'], [],
      ['unapply', '1', '2'],  [],

      ['all'],                [('001_foo', 'up'), ('002_bar', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['up'],                 [('001_foo', 'up')],
      ['down'],               [],

      ['to', '1'],            [('001_foo', 'up')],
      ['to', '2'],            [('001_foo', 'up'), ('002_bar', 'up')],
      ['upto', '2'],          [('001_foo', 'up'), ('002_bar', 'up')],
      ['upto', '002_bar'],    [('001_foo', 'up'), ('002_bar', 'up')],
      ['upto', '4'],          [('001_foo', 'up'), ('002_bar', 'up')],
      ['upto', '004_foo'],    NoSuchMigrationError,
      ['upto', '5'],          AmbiguousMigrationNameError,
      ['upto', '005_hello'],  [('001_foo', 'up'), ('002_bar', 'up'), ('005_hello', 'up')],
      ['upto', '005_omg'],    [('001_foo', 'up'), ('002_bar', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['downto', '1'],        [],
      ['downto', '2'],        [],
    )
    
    si.mark_as_applied('002_bar')

    self.assert_plans(si,
      ['apply', '1'],         [('001_foo', 'up')],
      ['apply', '001_foo'],   [('001_foo', 'up')],
      ['apply', '1', '2'],    [('001_foo', 'up')],
      ['unapply', '001_foo'], [],
      ['unapply', '1', '2'],  [('002_bar', 'down')],

      ['all'],                [('001_foo', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['up'],                 [('001_foo', 'up')],
      ['down'],               [('002_bar', 'down')],

      ['to', '1'],            [('002_bar', 'down'), ('001_foo', 'up')],
      ['to', '2'],            [('001_foo', 'up')],
      ['upto', '2'],          [('001_foo', 'up')],
      ['upto', '002_bar'],    [('001_foo', 'up')],
      ['upto', '4'],          [('001_foo', 'up')],
      ['upto', '004_foo'],    NoSuchMigrationError,
      ['upto', '5'],          AmbiguousMigrationNameError,
      ['upto', '005_hello'],  [('001_foo', 'up'), ('005_hello', 'up')],
      ['upto', '005_omg'],    [('001_foo', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['downto', '1'],        [('002_bar', 'down')],
      ['downto', '2'],        [],
    )

    si.mark_as_applied('001_foo')

    self.assert_plans(si,
      ['apply', '1'],         [],
      ['apply', '001_foo'],   [],
      ['apply', '1', '2'],    [],
      ['unapply', '001_foo'], [('001_foo', 'down')],
      ['unapply', '1', '2'],  [('001_foo', 'down'), ('002_bar', 'down')],

      ['all'],                [('005_hello', 'up'), ('005_omg', 'up')],
      ['up'],                 [('005_hello', 'up')],
      ['down'],               [('002_bar', 'down')],

      ['to', '1'],            [('002_bar', 'down')],
      ['to', '2'],            [],
      ['upto', '2'],          [],
      ['upto', '002_bar'],    [],
      ['upto', '4'],          [],
      ['upto', '004_foo'],    NoSuchMigrationError,
      ['upto', '5'],          AmbiguousMigrationNameError,
      ['upto', '005_hello'],  [('005_hello', 'up')],
      ['upto', '005_omg'],    [('005_hello', 'up'), ('005_omg', 'up')],
      ['downto', '1'],        [('002_bar', 'down')],
      ['downto', '2'],        [],
    )

    si.mark_as_unapplied('002_bar')

    self.assert_plans(si,
      ['apply', '1'],         [],
      ['apply', '001_foo'],   [],
      ['apply', '1', '2'],    [('002_bar', 'up')],
      ['unapply', '001_foo'], [('001_foo', 'down')],
      ['unapply', '1', '2'],  [('001_foo', 'down')],

      ['all'],                [('002_bar', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['up'],                 [('002_bar', 'up')],
      ['down'],               [('001_foo', 'down')],

      ['to', '1'],            [],
      ['to', '2'],            [('002_bar', 'up')],
      ['upto', '2'],          [('002_bar', 'up')],
      ['upto', '002_bar'],    [('002_bar', 'up')],
      ['upto', '4'],          [('002_bar', 'up')],
      ['upto', '004_foo'],    NoSuchMigrationError,
      ['upto', '5'],          AmbiguousMigrationNameError,
      ['upto', '005_hello'],  [('002_bar', 'up'), ('005_hello', 'up')],
      ['upto', '005_omg'],    [('002_bar', 'up'), ('005_hello', 'up'), ('005_omg', 'up')],
      ['downto', '1'],        [],
      ['downto', '2'],        [],
    )

    si.mark_as_applied('002_bar')
    si.mark_as_applied('005_hello')
    si.mark_as_applied('005_omg')
    
    self.assert_plans(si,
      ['apply', '1'],         [],
      ['apply', '001_foo'],   [],
      ['apply', '1', '2'],    [],
      ['unapply', '001_foo'], [('001_foo', 'down')],
      ['unapply', '1', '2'],  [('001_foo', 'down'), ('002_bar', 'down')],

      ['all'],                [],
      ['up'],                 [],
      ['down'],               [('005_omg', 'down')],

      ['to', '1'],            [('005_omg', 'down'), ('005_hello', 'down'), ('002_bar', 'down')],
      ['to', '2'],            [('005_omg', 'down'), ('005_hello', 'down')],
      ['upto', '2'],          [],
      ['upto', '002_bar'],    [],
      ['upto', '4'],          [],
      ['upto', '004_foo'],    NoSuchMigrationError,
      ['upto', '5'],          AmbiguousMigrationNameError,
      ['upto', '005_hello'],  [],
      ['upto', '005_omg'],    [],
      ['downto', '1'],        [('005_omg', 'down'), ('005_hello', 'down'), ('002_bar', 'down')],
      ['downto', '2'],        [('005_omg', 'down'), ('005_hello', 'down')],
    )

  def test_plan_with_dev(self):
    # NOTE: si.mark_as_applied modifies global state, not si (MigrationState) object.
    #       There is only one meaningful MigrationState object per database,
    #       so it's an almost-singleton unless we support multiple databases at the same time.
    #       This makes testing quite ugly.
    db = MigrationDb(migrations = ['001_foo', '002_DEV_bar', '005_omg', '006_DEV_hello'])

    si = MigrationState(migration_db=db)
    si.init()
    self.assert_plans(si,
      ['up'],        [('001_foo', 'up')],
      ['upto', '5'], [('001_foo', 'up'), ('005_omg', 'up')],
      ['upto', '6'], [('001_foo', 'up'), ('005_omg', 'up')],
      ['all'],       [('001_foo', 'up'), ('005_omg', 'up')],
    )

    si = MigrationState(migration_db=db, dev=True)
    si.init()
    self.assert_plans(si,
      ['up'],        [('001_foo', 'up')],
      ['upto', '5'], [('001_foo', 'up'), ('002_DEV_bar', 'up'), ('005_omg', 'up')],
      ['upto', '6'], [('001_foo', 'up'), ('002_DEV_bar', 'up'), ('005_omg', 'up'), ('006_DEV_hello', 'up')],
      ['all'],       [('001_foo', 'up'), ('002_DEV_bar', 'up'), ('005_omg', 'up'), ('006_DEV_hello', 'up')],
    )

    si.mark_as_applied('001_foo')

    si = MigrationState(migration_db=db)
    si.init()
    self.assert_plans(si,
      ['up'],        [('005_omg', 'up')],
      ['upto', '5'], [('005_omg', 'up')],
      ['upto', '6'], [('005_omg', 'up')],
      ['all'],       [('005_omg', 'up')],
    )

    si = MigrationState(migration_db=db, dev=True)
    si.init()
    self.assert_plans(si,
      ['up'],        [('002_DEV_bar', 'up')],
      ['upto', '5'], [('002_DEV_bar', 'up'), ('005_omg', 'up')],
      ['upto', '6'], [('002_DEV_bar', 'up'), ('005_omg', 'up'), ('006_DEV_hello', 'up')],
      ['all'],       [('002_DEV_bar', 'up'), ('005_omg', 'up'), ('006_DEV_hello', 'up')],
    )

    si.mark_as_applied('002_DEV_bar')

    si = MigrationState(migration_db=db)
    si.init()
    self.assert_plans(si,
      ['up'],        [('005_omg', 'up')],
      ['upto', '5'], [('005_omg', 'up')],
      ['upto', '6'], [('005_omg', 'up')],
      ['all'],       [('005_omg', 'up')],
      ['down'],      [('001_foo', 'down')], # Is it better to always assume --dev on down ?
    )

    si = MigrationState(migration_db=db, dev=True)
    si.init()
    self.assert_plans(si,
      ['up'],        [('005_omg', 'up')],
      ['upto', '5'], [('005_omg', 'up')],
      ['upto', '6'], [('005_omg', 'up'), ('006_DEV_hello', 'up')],
      ['all'],       [('005_omg', 'up'), ('006_DEV_hello', 'up')],
      ['down'],      [('002_DEV_bar', 'down')],
    )

  def test_uninit_requires_dev(self):
    db = MigrationDb(migrations = ['001_foo', '002_DEV_bar', '005_omg', '006_DEV_hello'])
    si = MigrationState(migration_db=db)
    si.init()

    si.mark_as_applied('001_foo')
    si.mark_as_applied('002_DEV_bar')

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    self.assert_raises(DevFlagRequiredError, lambda: si.uninit())

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

  def test_uninit(self):
    db = MigrationDb(migrations = ['001_foo', '002_DEV_bar', '005_omg', '006_DEV_hello'])
    si = MigrationState(migration_db=db, dev=True)
    si.init()

    si.mark_as_applied('001_foo')
    si.mark_as_applied('002_DEV_bar')

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    si.uninit()

    self.assert_equal(True, si.old_migration_table_present())
    self.assert_equal(False, si.new_migration_table_present())

    self.assert_equal(2, si.get_old_schema_version())

  def test_uninit_inconsistent(self):
    db = MigrationDb(migrations = ['001_foo', '002_DEV_bar', '005_omg', '006_DEV_hello'])
    si = MigrationState(migration_db=db, dev=True)
    si.init()

    si.mark_as_applied('001_foo')
    si.mark_as_applied('005_omg')

    self.assert_equal(False, si.old_migration_table_present())
    self.assert_equal(True, si.new_migration_table_present())

    self.assert_raises(InconsistentStateError, lambda: si.uninit())
  
  def test_all_migrations_applied(self):
    db = MigrationDb(migrations = ['001_foo', '002_DEV_bar', '005_omg', '006_DEV_hello'])
    si = MigrationState(migration_db=db)
    si.init()

    si.mark_as_applied('005_omg')
    si.mark_as_applied('001_foo')
    si.mark_as_applied('009_bogus')

    self.assert_equal(['001_foo', '005_omg', '009_bogus'], si.all_migrations_applied())
