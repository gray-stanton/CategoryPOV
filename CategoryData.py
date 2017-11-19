import pandas as pd
import numpy as np
import collections as cl

class CategoryData():
    """Category sales data holder and manipulator.

    Stores database-structured sales data as pandas dataframes."""
    def __init__(self, brandtable, upctable, obstable):
        """Store tables in object, check basic invariants"""
        try:
            brand_ids = brandtable['brand_id']
            upc_ids = upctable['upc_id']
            obs_ids = obstable['obs_id']
            upc_brands = upctable['brand_id']
            obs_upcs = obstable['upc_id']
        except KeyError:
            print('''Invalid Table State: all tables must have appropriate
                  primary and foreign keys.''')
            raise ValueError
        all_brands_present = set(upc_brands) == set(brand_ids)
        all_upcs_present   = set(obs_upcs)   == set(upc_ids)
        if not (all_brands_present and all_upcs_present):
            print('Invalid Table State: missing brands or upcs')
            raise ValueError
        duplicated_brands = any(brand_ids.duplicated())
        duplicated_upcs = any(upc_ids.duplicated())
        duplicated_obs = any(obs_ids.duplicated())
        if duplicated_brands or duplicated_upcs or duplicated_obs:
            print('Invalid Table State: duplicated ids')
            raise ValueError
        #If those invariants check out, wrap tables in class
        self.brandtable = brandtable
        self.upctable = upctable
        self.obstable = obstable

    #TODO write code so that all ids are kept as permanently sequential
    #which implies that ids must change as pruning operations are completed
    #which means losing easy backportability to larger, unpruned index,
    #but is basically necessary due to how stan doesn't have a hashmap type.
    #Possibly we'll want
    #TODO AMMENDMENT! Actually, we'll probably just want to cordone off a
    #special stan index/id, because were still dealing with the in-brand issue.
    #Same problem really, possibly will have to add a map somewhere, someone
    #should keep track of this...
    #TODO Add multipredictor functionality, both at obs, upc, and brand level.
    def exportstan(self, x_column_name, y_column_name, separate_brands = True):
        """Process and output the sales data into the long format required for
        stan.

        If seperate_brands is true, break up into a list of individual brand
        dumps."""
        if separate_brands == False:
             stan_data = {
                'X' : list(obs_table[x_column_name]),
                'Y' : list(obs_table[y_column_name]),
                'N_brand' : len(brandtable),
                'N_upc' : len(upctable),
                'N_obs' : len(obstable),
                 'upc_id' : list(obs_table['upc_id']),
                 'brand_id' : list(upctable['upc_id'])}
        else:
            stan_data = []
            for brand_id in brandtable['brand_id']:
                rel_upcs = upctable.query(
                    'brand_id == ' + str(brand_id))['upc_id']
                rel_obstable = obstable[obstable['upc_id'].isin(rel_upcs)]
                brand_data = {
                    'X' : list(rel_obstable[x_column_name]),
                    'Y' : list(rel_obstable[y_column_name]),
                    'N_upc' : len(rel_obstable),
                    'N_obs' : len(obstable),
                    #FIXME these ids should just be 1:N_upc, and they arent.
                    'upc_id' : list(obs_table['upc_id'])}
                stan_data.append(brand_data)
        return stan


    def prune(self, brand_reqs = None, upc_reqs = None, obs_reqs = None):
        """Cut out elements that do not meet the requirements set in the arg
        files
        Args:
            brand_reqs (OrderedDict): Brand level requirements
            upc_reqs (OrderedDict):   UPC level requirements
            obs_reqs (OrderedDict):   Observation level requirements

        Returns:
            A new CategoryData object, containing only elements meeting the
            requirements."""
        #Requirements are evaluated in the order as follows:
        #All obs requirements, then all upc requirements, then all brand
        #requirements. In each level, the requirements are evaluated in the
        #order specified in the ordereddict
        #Currently supported requirements:
            #obs-level:
            #   minimum unit sales, minimum acv,
            #   maximal consecutive run membership
            #upc-level:
                #minimum # of  observations
                #minimum total upc sales
                #minimum 'last-year' sales
            #brand-level:
                #minimum number of UPCs
                #minimum total sales
                #minimum average annualized sales
        #TODO May want to seperate these into individual functions, let run.py
        #manage order
        oreqs = ['min_unit_sales', 'min_acv', 'in_longest_run']
        new_obstable = self.obstable
        for oreq_name, oreq_value in obs_reqs:
            if oreq_name not in oreqs:
                raise ValueError(
                    'Invalid observation requirement: {}'.format(oreq_name))
            if oreq_name == 'min_unit_sales':
                try:
                    new_obstable.query('units >= ' + str(oreq_value),
                                       inplace = True)
                except KeyError:
                    raise ValueError('Column "units" not found!')
            elif oreq_name == 'min_acv':
                try:
                    new_obstable.query('acv >= ' + str(oreq_value),
                                       inplace = True)
                except KeyError:
                    raise ValueError('Column "acv" not found!')
            elif oreq_name == 'in_longest_run' and oreq_value == True:
                try:
                    new_obstable['in_longest_run'] = new_obstable.groupby(
                        'upc_id').t.transform(CategoryData._in_longest_block)
                    new_obstable.query('in_longest_run == True',  inplace=True)
                except AttributeError:
                    raise ValueError('Column "t" not found!')
        new_upctable = self.upctable
        ureqs = ['min_obs', 'min_total_unit_sales', 'min_average_annual_sales']
        for ureq_name, ureq_value in upc_recs:
            if ureqs_name not in ureqs:
                raise ValueError(
                    'Invalid UPC requirement: {}'.format(ureqs_name))
            if ureq_name == 'min_obs':
                sizes = obstable.groupby('upc_id').size()
                new_upctable = new_upctable.merge(sizes, on = 'upc_id',
                                                  how = 'left')
                new_upctable.query('sizes >= ' + str(ureq_value),
                                   inplace = True)
                del new_upctable['sizes']
            if ureq_name == 'min_total_unit_sales':
                    try:
                        totals = obstable.groupby('upc_id')['units'].sum()
                    except KeyError:
                        raise ValueError('Column "units" not found!')
                    #FIXME don't merge a series onto a df, assign a new col
                    new_upctable = new_upctable.merge(totals,
                                                      on = 'upc_id',
                                                      how = 'left')
                    new_upctable.query('totals >= ' + str(ureq_value),
                                      inplace = True)
            #TODO Finish upc prunes, write brand prunes
            #TODO Add in normalizing code to eliminate orphans


    def remove_orphans(self):
        """Removes brands which have no upcs, upcs with no obs."""
        num_of_upcs_in_brand = self.upctable.groupby('brand_id').count()
        pass


        #Currently supported requirements:
            #obs-level:
            #   minimum unit sales, minimum acv,
            #   maximal consecutive run membership
            #upc-level:
                #minimum # of  observations
                #minimum total upc sales
                #minimum 'last-year' sales
            #brand-level:
                #minimum number of UPCs
                #minimum total sales
    def obsprune_min_units(self, units_cutoff):
        """Removes observations with units below units_cutoff"""
        pass

    def obsprune_maxrun(self):
        """Removes observations that aren't in the maximal conseq run"""
        pass

    def obsprune_min_acv(self, acv_cutoff):
        """Removes observations with acv below acv_cutoff"""
        pass

    def upcprune_min_obs(self, obs_cutoff):
        """Removes upcs with fewer than obs_cutoff obs"""
        pass

    def upcprune_min_total_units(self, total_units_cutoff):
        """Removes upcs with total unit sales less than total_units_cutoff"""
        pass

    def upcprune_min_lastyear_sales(self, lastyear_sales_cutoff):
        """Removes upcs with total unit sales in the last year less than
        lastyear_sales_cutoff"""
        pass

    def brandprune_min_upcs(self, upc_cutoff):
        """Removes brands with fewer upcs than upc_cutoff"""
        pass

    def brandprune_min_total_units(self, total_cutoff):
        """Removes brands with fewer total sales across all upcs for all time
        than cutoff"""
        pass

    def brandprune_min_lastyear_units(self, lastyear_cutoff):
        """Removes brands with fewer total sales in the last year across all upcs
        than cutoff"""
        pass

    def flatten(self, level = 'all'):
        """Combine the UPC-level data onto the observation level and return

        Args:
            level (string): either 'all' or 'upc', which of the higher level
                tables to pull onto the obstable.
            """
        if level not in ['all', 'upc']:
            raise ValueError('Invalid "level" argument {}'.format(level))
        if level == 'upc':
            return self.obstable.merge(self.upctable, how = 'left', on = 'upc_id')
        elif level == 'all':
            merge1 = self.upctable.merge(self.brandtable,
                                         how = 'left', on ='brand_id')
            return self.obstable.merge(merge1, how = 'left', on = 'upc_id')

    @staticmethod
    def _make_obs_blocks(time_series):
        """Compute blocks, referring to which temporally consequtive block
        the observation belongs to.
        Args:
            df: a pandas data frame of observations in a 1-d order
            time_column: A column of ints, giving the period to which the
                observation corresponds to. No Nas.
        Returns:
            (list of block_ids same length as df, smallest most frequent block
        """
        if time_series.isnull().any():
            raise ValueError('{} has missing values!'.format(time_series))
        time_series.sort_values(inplace = True)
        diff = time_series.diff()
        block = pd.Series(np.zeros(shape = (len(time_series),), dtype = np.int8),
                          index = time_series.index)
        curblock = 1
        for i in time_series.index:
            d = diff[i]
            if d == 1 or pd.isnull(d):
                block[i] = curblock
            else:
                curblock += 1
                block[i] = curblock
        #Get the first element from pd.mode, which is the earlist, most
        #frequent block
        return(block, block.mode()[0])

    @staticmethod
    def _in_longest_block(time_series):
        """Determine if each observation is in the longest block."""
        blocks, long_block = CategoryData._make_obs_blocks(time_series)
        return blocks == long_block




