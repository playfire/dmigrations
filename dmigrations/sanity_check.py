import re
import os.path
from django.db import connection, models as db_models
from django.core.management.color import no_style
# Gone from Django 1.0!
# from django.core.management.sql import many_to_many_sql_for_model
from migration_state import _execute

def zip_sets(a, b):
    aset = set(a)
    bset = set(b)
    return [(e, e in aset, e in bset) for e in sorted(aset.union(bset))]

def get_app_names():
    apps_dir = os.path.dirname(os.path.dirname(__file__))
    for f in os.listdir(apps_dir):
        if os.path.isdir(os.path.join(apps_dir, f)):
            if os.path.exists(os.path.join(apps_dir, f, 'models.py')) or \
              os.path.exists(os.path.join(apps_dir, f, 'models/__init__.py')):
                yield('gcap.apps.'+f)
    yield 'django.contrib.admin'
    yield 'django.contrib.auth'
    yield 'django.contrib.sessions'
    yield 'django.contrib.sites'
    yield 'django.contrib.contenttypes'
    yield 'gcap.ext.tagging'

def get_apps():
    for app_name in get_app_names():
        exec('import %s.models' % app_name)
        yield eval('%s.models' % app_name)

def mm_tables_for_model(model):
    return re.findall(r'CREATE TABLE `(.*?)`', "\n".join(
        many_to_many_sql_for_model(model, no_style()))
    )

def get_tables_that_should_exist():
    models = [
        model for app in get_apps() for model in db_models.get_models(app)
    ]
    main_tables = [model._meta.db_table for model in models]
    m2m_tables = [
        table for model in models for table in mm_tables_for_model(model)
    ]
  
    return main_tables + m2m_tables + ['dmigrations', 'dmigrations_log']

def get_tables_existence_status():
    """
    Returns a list of tuples (table_name, should_exist, does_exist)
    """
    tables_that_should_exist = get_tables_that_should_exist()
    tables_that_exist = [row[0] for row in _execute("SHOW TABLES").fetchall()]
    return zip_sets(tables_that_should_exist, tables_that_exist)

def sanity_check():
    # Temporarily disabled:
    return True
    
    
    """
    Reports discrepancies between state of the database and models.
    
    Among the things we want to know eventuallly are:
    * Models
    * Model fields
    * Model field types
    * Indexes
    * etc.
    
    For now we'll just cover the basics, especially since it reports so much.
    """
    
    for table_name, should_exist, exists in get_tables_existence_status():
        if should_exist and not exists:
            print "EXPECTED %s does not exist" % table_name
        if exists and not should_exist:
            print "NOT EXPECTED %s exists" % table_name
