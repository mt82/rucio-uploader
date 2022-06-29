import re 
import json

import rucio_uploader.utils as utils
import rucio_uploader.rucio.wrappers as wrapper

class RucioLogReader:
    """Reader of items from log file
    """
    
    def __init__(self, log_files: list):
        """RucioLogReader constructor

        Args:
            file (list): log file
        """
        self.log_files = log_files
        self.items = {}
        self.read()
    
    def read(self):
        for log_file in self.log_files:
            f = open(log_file, 'r')
            for l in f.readlines():
                matches = re.match("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,3} \[root\] \[INFO\] :   _RECOVERY_JSON_STRING_ : (.*)", l)
                if not matches is None:
                    items = json.loads(matches.group(1))
                    for item in items:
                        if item["did_name"] not in self.items:
                            self.items[item["did_name"]] = item

class RucioLogItemsConfigurator:
    """Configurator of the items from log file
    """

    def __init__(self, fitems: RucioLogReader, config: dict):
        """RucioLogItemsConfigurator constructor

        Args:
            fitems (RucioLogReader): RucioLogReader
            config (dict): configuration
        """
        self.config = config
        self.dids = {}
        self.datasets = {}
        self.rules = {}
        self.createDIDs(fitems)
        self.createDatasets()
        self.createRules()
    
    def createDIDs(self, fitems: RucioLogReader):
        """Create RUCIO DIDs from files

        Args:
            fitems (RucioLogReader): RucioLogReader
        """
        for item in fitems.items.values():
            did = wrapper.RucioDID(item['path'], 
                        item['did_name'], 
                        item['did_scope'],
                        item['dataset_name'], 
                        item['dataset_scope'])
            did.configure(item['register_after_upload'], item['rse'])
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
