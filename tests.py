import unittest
import sys
sys.path.append('/home/gray/scripts/categorypov/CategoryPOV')
import queryned as qn
class TestSQL(unittest.TestCase):
    """Test the reading of the raw SQL capabilities"""
    def test_loadconf(self):
       confs = qn.get_confs()
       self.assertEqual(confs["db"], "nielsen")
    def test_query(self):
        query = """ SELECT * FROM data LIMIT 1 """
        result = qn.query_server(query)
        #just check that it is non-empty
        self.assertTrue(result[1])


if __name__ == "__main__":
    unittest.main()

