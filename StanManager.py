import pystan
import pandas as pd

class StanManager():
    """Controls input, process running, and output manipulation for CmdStan."""
    def __init__(dump_path, output_path):
        pass

    def make_standumps(brand_seperated = True, data_to_dump):
        """Create standump files.

        Args:
            brand_seperated: Whether or not to create a seperate dump file for
                each brand's data.
            data_to_dump (list): a list of dictionaries of the components of
                the data that needs to be dumped out.
            dump_path: Where to deposit the standump files
            """
        pass

    def launch_stan(stan_args):
        """Initiate CmdStan processes on stan dumps.
        Args:
            stan_args (dict): kwargs that will be passed to the cmdstan process
            """
        pass

