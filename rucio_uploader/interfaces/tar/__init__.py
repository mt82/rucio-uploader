import os
import re
import tarfile

import rucio_uploader.rucio.wrappers as wrapper
import rucio_uploader.utils as utils

class TarItem:
    """Class to represent a file from tar archive
    """

    def __init__(self, surl: str, file: str):
        """Constructor

        Args:
            surl (str): file surl
            file (str): filename
        """
        self.s = surl
        self.f = file

class TarReader:
    """Reader of items from tar archieve
    """

    def __init__(self, tarlists: list):
        """TarReader constructor

        Args:
            tarlists (list): list of tar archieves to be read
        """
        self.tars = tarlists
        self.items = []
        self.read()
    
    def read(self):
        """Read items from tar archieve
        """
        self.items = []
        for t in self.tars:
            tar = tarfile.open(t,"r:gz")
            for el in tar.getmembers():
                if el.isfile():
                    for filesurl in tar.extractfile(el).readlines():
                        self.items.append(TarItem(filesurl.decode("utf-8").strip(),el.name))

class TarItemsConfigurator:
    """Configurator of the items from tar archieves
    """

    def __init__(self, titems: list, config: dict):
        """TarItemsConfigurator constructor

        Args:
            titems (list): list of items from tar archieve
            config (dict): configuration
        """
        self.config = config
        self.dids = {}
        self.datasets = {}
        self.rules = {}
        self.zero_size_dids = {}
        self.createDIDs(titems)
        self.createDatasets()
        self.createRules()

    def filepath(self, filesurl: str) -> str:
        """Translate file surl into file path

        Args:
            filesurl (str): file surl

        Returns:
            str: file path
        """
        return re.sub("gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr",
                      "/pnfs",
                      filesurl)

    def filename(self, filepath: str) -> str:
        """Extract filename from file path

        Args:
            filepath (str): file path

        Returns:
            str: filename
        """
        return os.path.basename(filepath)

    def did_name(self, filepath: str) -> str:
        """Obtain RUCIO DID name from filepath. It coincides with filename

        Args:
            filepath (str): file path

        Returns:
            str: RUCIO DID name
        """
        return self.filename(filepath)
        
    def run_number(self, filename: str) -> str:
        """Extract run number from txt filename

        Args:
            filename (str): filename

        Returns:
            str: run number
        """
        matches = re.match(self.config["filename_run_pattern"], filename)
        if matches is None:
            return None
        else:
            return matches.group(1)

    def dataset_name(self, run: str) -> str:
        """Construct dataset name from run number and template

        Args:
            run (str): run number

        Returns:
            str: RUCIO dataset name
        """
        return self.config["ds_name_template"].format(run)
    
    def createDIDs(self, titems: list):
        """Create RUCIO DIDs from tar items

        Args:
            titems (list): list of tar items
        """
        for item in titems.items:
            path = self.filepath(item.s)
            name = self.did_name(path)
            run = self.run_number(item.f)
            ds_name = self.dataset_name(run)
            did = wrapper.RucioDID(path, 
                           name, 
                           self.config["scope"], 
                           ds_name, 
                           self.config["scope"])
            did.configure(self.config["register_after_upload"], self.config["upl_rse"])
            if did.size == 0:
                self.zero_size_dids[did.get_scoped_name()] = did
            else:
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
        """Creates RUCIO Rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = wrapper.RucioRule(self.config["dst_rse"], ds.name, ds.scope)
