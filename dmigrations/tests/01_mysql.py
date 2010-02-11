import unittest

TC = unittest.TestCase

import mysql.migrations as m

class Behavior(object):
    def __call__(self, statements, return_rows):
        pass

class StatementLogger(Behavior):
    def __init__(self):
        self.log = []

    def __call__(self, statements, return_rows):
        self.log.extend(statements)

class StatementFaker(StatementLogger):
    def __init__(self, handler):
        super(StatementFaker, self).__init__()
        self.handler = handler

    def __call__(self, statements, return_rows):
        super(StatementFaker, self).__call__(statements, return_rows)
        if return_rows:
            return self.handler(statements)

class StatementFailer(StatementLogger):
    def __call__(self, statements, return_rows):
        super(StatementFailer, self).__call__(statements, return_rows)
        from MySQLdb import OperationalError
        raise OperationalError(9999, 'This is a fake error')

class DualTest(TC):
    def check(self, mig, up_sql, down_sql, up_behavior=StatementLogger, down_behavior=StatementLogger):
        def instance(cls_or_obj):
            if isinstance(cls_or_obj, Behavior):
                return cls_or_obj
            return cls_or_obj()

        mig.run_statements = instance(up_behavior)
        mig.up()
        self.failUnlessEqual(mig.run_statements.log, up_sql)

        mig.run_statements = instance(down_behavior)
        mig.down()
        self.failUnlessEqual(mig.run_statements.log, down_sql)


class TestMigration(DualTest):
    def test_normal(self):
        mig = m.Migration('sql up', 'sql down')
        self.check(mig, ['sql up'], ['sql down'])

    def test_irreversible(self):
        mig = m.Migration('sql up')

        mig.run_statements = StatementLogger()
        mig.up()
        self.failUnlessEqual(mig.run_statements.log, ['sql up'])

        self.assertRaises(m.IrreversibleMigrationError,
                          lambda: mig.down())


class TestAddDropColumn(DualTest):
    def test_fancy(self):
        add_sql = ['ALTER TABLE `quiz_answer` ADD COLUMN `question_id` INT UNSIGNED,'
                   '\n  ADD CONSTRAINT `yomama_123` FOREIGN KEY (`question_id`) REFERENCES `quiz_question` (`id`) ON DELETE CASCADE;']
        drop_sql = ['ALTER TABLE `quiz_answer` DROP FOREIGN KEY `yomama_123`,\n  DROP COLUMN `question_id`;']

        m.AddColumn.fk_name = classmethod(lambda cls, *args: 'yomama_123')

        mig = m.AddColumn('quiz', 'answer', 'question', 'INT UNSIGNED NOT NULL', 'quiz_question', ondelete='CASCADE')
        self.check(mig, add_sql, drop_sql)

        mig = m.DropColumn('quiz', 'answer', 'question', 'INT UNSIGNED NOT NULL', 'quiz_question', ondelete='CASCADE')
        self.check(mig, drop_sql, add_sql)

    def test_plain(self):
        add_sql = ['ALTER TABLE `quiz_answer` ADD COLUMN `text` VARCHAR(50);']
        drop_sql = ['ALTER TABLE `quiz_answer` DROP COLUMN `text`;']

        m.AddColumn.fk_name = classmethod(lambda cls, *args: 'yomama_123')
        mig = m.AddColumn('quiz', 'answer', 'text', 'VARCHAR(50)')
        self.check(mig, add_sql, drop_sql)

        mig = m.DropColumn('quiz', 'answer', 'text', 'VARCHAR(50)')
        self.check(mig, drop_sql, add_sql)


class TestAddDropIndex(DualTest):
    def test_plain(self):
        add_sql = ['ALTER TABLE `quiz_answer` ADD INDEX `foobar` (`text`);']
        drop_sql = ['ALTER TABLE `quiz_answer` DROP INDEX `foobar`;']

        mig = m.AddIndex('quiz', 'answer', 'text', 'foobar')
        self.check(mig, add_sql, drop_sql)

        mig = m.DropIndex('quiz', 'answer', 'text', 'foobar')
        self.check(mig, drop_sql, add_sql)

    def test_fancy(self):
        add_sql = ['ALTER TABLE `quiz_answer` ADD INDEX `quiz_answer_a_b_c` (`a`, `b`, `c`);']
        drop_sql = ['ALTER TABLE `quiz_answer` DROP INDEX `quiz_answer_a_b_c`;']

        mig = m.AddIndex('quiz', 'answer', ['a', 'b', 'c'])
        self.check(mig, add_sql, drop_sql)

        mig = m.DropIndex('quiz', 'answer', ['a', 'b', 'c'])
        self.check(mig, drop_sql, add_sql)


class TestChangeColumn(DualTest):
    def check(self, mig, add_sql, drop_sql):
        def get_faker():
            def handler(statements):
                if 'DESC' in statements[0]:
                    return [
                        (u'id', u'int(11)', u'NO', u'PRI', None, u'auto_increment'),
                        (u'user_id', u'int(11)', u'NO', u'MUL', None, u''),
                        (u'ip', u'char(15)', u'NO', u'', u'', u''),
                        (u'when', u'datetime', u'NO', u'', None, u'')
                        ]
                return []
            return StatementFaker(handler)

        super(TestChangeColumn, self).check(mig, add_sql, drop_sql,
                                            get_faker(), get_faker())

    def test_plain(self):
        add_sql = ['DESC `site_userip`', 'ALTER TABLE `site_userip` CHANGE `ip` `ip` char(32) NOT NULL']
        drop_sql = ['DESC `site_userip`', 'ALTER TABLE `site_userip` CHANGE `ip` `ip` char(15) NOT NULL']

        mig = m.ChangeColumn('site_userip', oldname='ip', old_def='char(15) NOT NULL', new_def='char(32) NOT NULL')
        self.check(mig, add_sql, drop_sql)

        rename_sql = ['DESC `site_userip`', 'ALTER TABLE `site_userip` CHANGE `ip` `ipe` char(15) NOT NULL']
        rename_sql2 = ['DESC `site_userip`']
        mig = m.ChangeColumn('site_userip', oldname='ip', newname='ipe')
        self.check(mig, rename_sql, rename_sql2)

        mig = m.ChangeColumn('site_userip', oldname='ipe', newname='ip')
        self.check(mig, rename_sql2, rename_sql)

    def test_errors(self):
        add_sql = ['DESC `site_userip`', 'ALTER TABLE `site_userip` CHANGE `ip` `ip` char(32) NOT NULL']
        drop_sql = ['DESC `site_userip`', 'ALTER TABLE `site_userip` CHANGE `ip` `ip` char(15) NOT NULL']

        mig = m.ChangeColumn('site_userip', oldname='ipeee', old_def='char(15) NOT NULL', new_def='char(32) NOT NULL')
        self.assertRaises(m.ChangeColumn.Failure, lambda:self.check(mig, add_sql, drop_sql))

        mig = m.ChangeColumn('site_userip', oldname='ip', newname='user_id', old_def='char(15) NOT NULL', new_def='char(32) NOT NULL')
        self.assertRaises(m.ChangeColumn.Conflict, lambda:self.check(mig, add_sql, drop_sql))

        mig = m.ChangeColumn('site_userip', oldname='ipee', newname='user_id', old_def='char(15) NOT NULL', new_def='char(32) NOT NULL')
        self.assertRaises(m.ChangeColumn.Failure, lambda:self.check(mig, add_sql, drop_sql))

        mig = m.ChangeColumn('site_userip', oldname='id', newname='id2',
                             old_def='INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
                             new_def='char(32) NOT NULL')
        self.assertRaises(m.ChangeColumn.Failure, lambda:self.check(mig, add_sql, drop_sql))

        mig = m.ChangeColumn('site_userip', oldname='id', newname='id2',
                             old_def='INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY',
                             new_def='char(32) NOT NULL')
        self.assertRaises(m.ChangeColumn.Failure, lambda:self.check(mig, add_sql, drop_sql))



if __name__ == '__main__':
    unittest.main()
