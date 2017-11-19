import unittest
import sys
import pandas as pd
sys.path.append('/home/gray/scripts/categorypov/CategoryPOV')
import SQLManager as qn
import CategoryData as qd
import pandas.util.testing as pdt
class TestSQL(unittest.TestCase):
    """Test the reading of the raw SQL capabilities"""
    def test_loadconf(self):
       confs = qn.get_confs('sql.yaml')
       self.assertEqual(confs["db"], "nielsen")
    def test_query(self):
        query = """ SELECT * FROM data LIMIT 1 """
        result = qn.query_server(query)
        #just check that it is non-empty
        self.assertTrue(result[1])

class TestCategoryData(unittest.TestCase):
    """Test the CategoryData class methods"""
   #TODO Write CategoryData Tests
   #TODO Create test data sets in R for each of the prune methods
    def setUp(self):
        brands = ['BrandA', 'BrandB']
        upcs = ['U101010', 'U2020202', 'U3030303', 'U4040404', 'U50505050']
        #3 observations each for the upcs
        observations = [1, 2, 3,
                        3, 2, 1,
                        5, 6, 7,
                        100, 50, 25,
                        1.5, 1.6, 1.7]
        obsupc_id = [0,0,0,1,1,1,2,2,2,3,3,3,4,4,4]
        self.obstable = pd.DataFrame(dict(upc_id = obsupc_id, value = observations,
                                     obs_id = list(range(0, len(observations)))))
        upcbrand_id = [0, 0, 0, 1, 1]
        descriptors = ['Corn Chips', 'Tortilla Chips', 'Bran Chips',
                       'Oatmeal Cookies', 'Raisin Cookies']
        self.upctable = pd.DataFrame(dict(upc = upcs,
                                          upc_id = list(range(0, len(upcs))),
                                          brand_id = upcbrand_id,
                                          desc = descriptors))
        prem = ['Premium', 'Nonpremium']
        self.brandtable = pd.DataFrame(dict(brand = brands,
                                            brand_id = list(range(0, len(brands))),
                                            premstatus = prem ))

    def test_create(self):
        cat = qd.CategoryData(self.brandtable, self.upctable, self.obstable)
        pdt.assert_frame_equal(self.brandtable, cat.brandtable)

    def test_prune_obs_with_small_values(self):
        cat = qd.CategoryData(self.brandtable, self.upctable, self.obstable)
        cat.prune_min_obs_value(2)



if __name__ == "__main__":
    unittest.main()

