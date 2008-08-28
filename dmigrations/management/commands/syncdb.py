import sys
from optparse import make_option
from django.core.management.base import NoArgsCommand
from django.contrib.auth.management import create_permissions

class Command(NoArgsCommand):
    """
    Over-ride syncdb - people should not be using it if they are managing 
    their database with dmigrations instead.
    """
    def handle_noargs(self, **options):
        raise Exception(
            "Use migrations not syncdb - ./manage.py help dmigrate for help"
        )
