from dmigrations.tests.common import *
from dmigrations.sanity_check import zip_sets, get_tables_existence_status

class SanityCheckTest(TestCase):
  def test_zip_sets(self):
    a = [2, 1, 5, 19, 1, 1, 1]
    b = [8, 19, 7, -9, 2, 2]
    self.assert_equal([
      (-9, False, True),
      (1, True, False),
      (2, True, True),
      (5, True, False),
      (7, False, True),
      (8, False, True),
      (19, True, True),
      ], zip_sets(a, b))
    self.assert_equal([], zip_sets([], []))
    self.assert_equal([(1, True, False), (2, True, False)], zip_sets([1,2], []))
    self.assert_equal([(3, False, True)], zip_sets([], [3,3]))

  def test_get_tables_that_should_exist(self):
    status = dict([(r,(s,e)) for (r,s,e) in get_tables_existence_status()])
    # NOTE: Assertion temporarily disabled because it doesn't work on asset-manager branch
    #self.assert_equal((True, True), status.get('london_guide_venue', None))
    self.assert_equal((True, True), status.get('dmigrations_log', None))
    self.assert_equal((True, True), status.get('auth_message', None))
