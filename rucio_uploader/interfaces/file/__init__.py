import os
import re

import rucio_uploader.utils as utils
import rucio_uploader.rucio.wrappers as wrapper

class FileItem:
    """Class to represent a file in filesystem
    """

    def __init__(self, path: str):
        """FileItem constructor

        Args:
            path (str): filepath
        """
        self.path = path

class DirectoryTreeReader:
    """Reader of items from directories
    """

    def __init__(self, dirs: list):
        """DirectoryTreeReader constructor

        Args:
            dirs (list): list of directories to be scanned
        """
        self.items = {}
        self.read_all(dirs)

    def read(self, dir: str):
        """Scan directory and collect items in it

        Args:
            dir (str): directory
        """
        for fname in os.listdir(dir):
            it = os.path.join(dir, fname)
            if os.path.isfile(it):
                if fname in self.items:
                    path1 = self.items[fname].path
                    path2 = it
                    if utils.md5(path1) != utils.md5(path2):
                        print("ERROR: items with same filename:")
                        print(" - {}".format(self.items[fname].path))
                        print(" - {}".format(it))
                        exit(1)
                self.items[fname] = FileItem(it)
            elif os.path.isdir(it):
                self.read(it)

    def read_all(self, dirs: list):
        """Scan all the directories and collect all the items

        Args:
            dirs (list): list of directories
        """
        for dir in dirs:
            self.read(dir)

class FileItemsConfigurator:
    """Configurator of the items from directories
    """

    def __init__(self, fitems: list, config: dict):
        """FileItemsConfigurator constructor

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

    def run_number(self, filename: str) -> str:
        """Extract run number from filename

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
    
    def file_matches(self, filename: str) -> bool:
        """Check if filename matches with pattern

        Args:
            filename (str): filename

        Returns:
            bool: True if filename macthes pattern
        """
        matches = re.match(self.config["filename_run_pattern"], filename)
        if matches is None:
            return False
        else:
            return True

    def dataset_name(self, run: str) -> str:
        """Construct dataset name from run number and template

        Args:
            run (str): run number

        Returns:
            str: RUCIO dataset name
        """
        return self.config["ds_name_template"].format(run)
    
    def createDIDs(self, fitems: list):
        """Create RUCIO DIDs from files

        Args:
            fitems (list): list of files
        """
        for name, item in fitems.items.items():
            if self.file_matches(name):
                path = item.path
                run = self.run_number(name)
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
        """Creates RUCIO rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = wrapper.RucioRule(self.config["dst_rse"], ds.name, ds.scope)
