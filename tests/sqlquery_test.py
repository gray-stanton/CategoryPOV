import unittest
import CategoryPov
import tests.queryned
class TestSQL(unittest.TestCase):
    """Test the reading of the raw SQL capabilities"""
    def test_loadconf(self):
       confs = get_confs("~/scripts/categorypov/CategoryPOV/sqlserver.yaml")
       self.assertEqual(confs[1], "nielsen")

if __name__ == '__main__':
    unittest.main()

