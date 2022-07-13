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
    
    def __init__(self, run_number: list, data_tier: list, data_stream: list):
        """SamwebReader constructor

        Args:
            run_number (list): run number
            data_tier (list): data tier ['raw', 'reco1', 'reco2', ...]
            data_stream (list): data stream ['numi', 'bnb', ...]
        """
        self.run_number = run_number
        self.data_tier = data_tier
        self.data_stream = data_stream
        self.items = []
        self.get_file_locations()
    
    def get_file_locations(self):
        """Retrieve the file locations from samweb
        """
        samweb = samweb_client.SAMWebClient(experiment='icarus')
        for r in self.run_number:
            for t in self.data_tier:
                items = {}
                for s in self.data_stream:
                    file_list = samweb.listFiles(dimensions=f"run_number = {r} and data_tier = '{t}' and data_stream = '{s}'")

                    nmax = 50
                    for i in range(int(len(file_list)/nmax)+1):
                        location_list = samweb.locateFiles(file_list[i*50:min([len(file_list), (i+1)*50])])
                        for key, value in location_list.items():
                            items[key] = SamReplicaItem(f"{value[0]['full_path'].replace('enstore:','')}/{key}")
                
                self.items.append({"run_number": r,
                                   "data_tier": t,
                                   "items": items})

class SamwebItemsConfigurator:
    """Configurator of the items from samweb
    """

    def __init__(self, fitems: SamwebReader, config: dict):
        """SamwebItemsConfigurator constructor

        Args:
            fitems (SamwebReader): SamwebReader
            config (dict): configuration
        """
        self.config = config
        self.dids = {}
        self.datasets = {}
        self.rules = {}
        self.createDIDs(fitems)
        self.createDatasets()
        self.createRules()

    def dataset_name(self, run_number, data_tier) -> str:
        """Construct dataset name from run number and data_tier

        Returns:
            str: RUCIO dataset name
        """
        return f"run-{run_number}-{data_tier}"
    
    def createDIDs(self, fitems: SamwebReader):
        """Create RUCIO DIDs from files

        Args:
            fitems (SamwebReader): SamwebReader
        """
        for el in fitems.items:
            for name, item in el["items"].items():
                did = wrapper.RucioDID(item.path, 
                            name, 
                            self.config["scope"], 
                            self.dataset_name(el["run_number"],el["data_tier"]), 
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
