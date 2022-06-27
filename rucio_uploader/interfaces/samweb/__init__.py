import samweb_client

import rucio_uploader.utils as utils
import rucio_uploader.rucio.wrappers as wrapper

class SamReplicaItem:
    """Class to represent a file in filesystem
    """

    def __init__(self, path: str):
        """SamReplicaItem constructor

        Args:
            path (str): filepath
        """
        self.path = path

class SamwebReader:
    """Reader of items from samweb
    """
    
    def __init__(self, run_number: int, data_tier: str, data_stream: str):
        """SamwebReader constructor

        Args:
            run_number (int): run number
            data_tier (str): data tier ['raw', 'reco1', 'reco2', ...]
            data_stream (str): data stream ['numi', 'bnb', ...]
        """
        self.run_number = run_number
        self.data_tier = data_tier
        self.data_stream = data_stream
        self.items = {}
        self.get_file_locations()
    
    def get_file_locations(self):
        """Retrieve the file locations from samweb
        """
        samweb = samweb_client.SAMWebClient(experiment='icarus')
        file_list = samweb.listFiles(dimensions=f"run_number = {self.run_number} and data_tier = '{self.data_tier}' and data_stream = '{self.data_stream}'")

        nmax = 50
        for i in range(int(len(file_list)/nmax)+1):
            location_list = samweb.locateFiles(file_list[i*50:min([len(file_list), (i+1)*50])])
            for key, value in location_list.items():
                self.items[key] = SamReplicaItem(f"{value[0]['full_path'].replace('enstore:','')}/{key}")

class SamwebItemsConfigurator:
    """Configurator of the items from samweb
    """

    def __init__(self, fitems: list, config: dict):
        """SamwebItemsConfigurator constructor

        Args:
            fitems (list): list of files
            config (dict): configuration
        """
        self.config = config
        self.dids = {}
        self.datasets = {}
        self.rules = {}
        self.zero_size_dids = {}
        self.createDIDs(fitems)
        self.createDatasets()
        self.createRules()

    def dataset_name(self) -> str:
        """Construct dataset name from run number and data_tier

        Returns:
            str: RUCIO dataset name
        """
        return f"run-{self.config['run_number']}-{self.config['data_tier']}"
    
    def createDIDs(self, fitems: list):
        """Create RUCIO DIDs from files

        Args:
            fitems (list): list of files
        """
        for name, item in fitems.items.items():
            did = wrapper.RucioDID(item.path, 
                        name, 
                        self.config["scope"], 
                        self.dataset_name(), 
                        self.config["scope"])
            did.configure(self.config["register_after_upload"], self.config["upl_rse"])
            self.dids[did.get_scoped_name()] = did

    def createDatasets(self):
        """Creates RUCIO Datasets
        """
        for did in self.dids.values():
            sname = utils.get_scoped_name(did.ds_name, did.ds_scope)
            if sname not in self.datasets:
                self.datasets[sname] = wrapper.RucioDataset(did.ds_name, self.config["scope"], [did])
            self.datasets[sname].dids.append(did)


    def createRules(self):
        """Creates RUCIO rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = wrapper.RucioRule(self.config["dst_rse"], ds.name, ds.scope)
