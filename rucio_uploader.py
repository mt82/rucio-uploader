#!/icarus/app/home/icaruspro/rucio_client_py3/bin/python -u
#!/usr/bin/python3 -u

"""@package rucio-uploader

 This script expects, as input either a list of tar files
 or a list of directories.
 
 TAR FILES AS INPUT: ['-type tar'] The tar files are 
 expected to  contain a set of text files,  each 
 containing  a list of files (one for  each line) 
 related  to a run. The names  of the txt files are  
 expected to match the template and the run number is 
 extracted from it.

 DIRECTORIES AS INPUT: ['-type dir'] The files in the 
 directories which match the pattern are taken as input.
 The run number is extracted from the name.
 
 This script:
 1- reads all the input files
 3- Checks if the dataset exists, if not add the missing ones
 4- Checks if the rules exists, if not add the missing ones
 5- checks if the files are already in RUCIO
 6- checks if the files are already in the datatset, if not add the missing ones
 7- checks if a transfer rule already exists, if not define it for the run 
 8- uploads missing files in parallel threads

"""
import re
import os
import sys
import tarfile
import argparse
import hashlib

from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from rucio.common import exception
from threading import Thread, Lock
from datetime import datetime
from io import TextIOWrapper

# lock for cuncurrent writing of log file
mutex = Lock()

def format_now() -> str:
    """Return a string with formatted datetime

    Returns:
        str: string with formatted datetime
    """
    return "[{}]".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def format_log_message(message: str, function) -> str:
    """Return formatted log message

    Args:
        message (str): text message to be formatted
        function (function): function logging the message

    Returns:
        str: formatted log message
    """
    return "   {} : [{}] -> \"{}\"\n".format(format_now(), function.__name__, message)

def get_scoped_name(name: str, scope: str) -> str:
    """Return a string in the form: "<scope>:<name>"

    Args:
        name (str): filename
        scope (str): scope

    Returns:
        str: a string in the form: "<scope>:<name>"]
    """
    return "{}:{}".format(scope,name)

def get_scope_and_name(sname: str) -> str:
    """Split the scoped name and return a tuple with scope and name

    Args:
        sname (str): scoped name

    Returns:
        str: a tuple with scope and name
    """
    splits = sname.split(':')
    return splits[0],splits[1]

def sources_exist(sources: list) -> bool:
    """Check the sources exist

    Args:
        sources (list): list of sources

    Returns:
        bool: True if all sources exist, False otherwise
    """
    for s in sources:
        if not os.path.exists(s):
            print("Error: {} does not exits".format(s))
            return False
    return True

def md5(fname):
    """Evaluate md5 checksum of a file

    Args:
        fname (str): path of a file

    Returns:
        str: checksum of a file
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

class RucioDID:
    """A RUCIO DID
    """
    
    def __init__(self, path: str, name: str, scope: str = None, ds_name: str = None, ds_scope: str = None):
        """RUCIO DID constructor

        Args:
            path (str): filepath
            name (str): name of the RUCIO DID
            scope (str, optional): scope of the RUCIO DID. Defaults to None.
            ds_name (str, optional): name of the RUCIO DID dataset. Defaults to None.
            ds_scope (str, optional): scope of the RUCIO DID dataset. Defaults to None.
        """
        self.name = name
        self.scope = scope
        self.path = path
        self.ds_name = ds_name
        self.ds_scope = ds_scope
        self.asUpload = {}
        self.asAttach = {}
        self.in_rucio=False
        self.in_dataset=False
        self.size = 0
        try:
            self.size = os.path.getsize(self.path)
        except:
            pass
    
    def get_scoped_name(self):
        """Return a string in the form: "<scope>:<item name>"

        Returns:
            str: a string in the form: "<scope>:<item name>"
        """
        return get_scoped_name(self.name,self.scope)

    def toUpload(self, register_after_upload: bool, rse: str):
        """Prepare DID to be uploaded in a RUCIO RSE

        Args:
            register_after_upload (bool): True to register after upload
            rse (str): RUCIO storage elenent
        """
        self.asUpload["path"]=self.path
        self.asUpload["did_name"] = self.name
        self.asUpload["did_scope"] = self.scope
        self.asUpload['dataset_name'] = self.ds_name
        self.asUpload['dataset_scope'] = self.ds_scope
        self.asUpload["register_after_upload"] = register_after_upload
        self.asUpload["rse"] = rse
        self.asUpload["upload_ok"] = False
        self.asUpload["size"] = self.size
    
    def toAttach(self):
        """Prepare DID to be attached to its dataset
        """
        self.asAttach['scope'] = self.scope
        self.asAttach['name'] = self.name
    
    def configure(self, register_after_upload: bool, rse: str):
        """Configure the RUCIO DID

        Args:
            register_after_upload (bool): True to register after upload
            rse (str): RUCIO storage elenent
        """
        self.toUpload(register_after_upload, rse)
        self.toAttach()

class RucioDataset:
    """A RUCIO dataset
    """

    def __init__(self, name: str, scope: str, dids: list):
        """RUCIO dataset constructor

        Args:
            name (str): name of the RUCIO dataset
            scope (str): scope of the RUCIO dataset
            dids (list): list of the dids
        """
        self.name = name
        self.scope = scope
        self.dids = dids
        self.in_rucio=False
    
    def get_scoped_name(self):
        """Return a string in the form: "<scope>:<item name>"

        Returns:
            str: a string in the form: "<scope>:<item name>"
        """
        return get_scoped_name(self.name,self.scope)

class RucioRule:
    """A RUCIO rule
    """

    def __init__(self, rse: str, name: str, scope: str, ncopy: int = 1):
        """RUCIO rule constructor

        Args:
            rse (str): name of the RUCIO storage element
            name (str): name of the dataset 
            scope (str): scope of the dataset
            ncopy (int, optional): number of copies. Defaults to 1.
        """
        self.rse = rse
        self.name = name
        self.scope = scope
        self.ncopy = ncopy
        self.in_rucio=False
    
    def get_scoped_name(self):
        """Return a string in the form: "<scope>:<item name>"

        Returns:
            str: a string in the form: "<scope>:<item name>"
        """
        return get_scoped_name(self.name,self.scope)

class FileItem:
    """Class to represent a file in filesystem
    """

    def __init__(self, path: str):
        """FileItem constructor

        Args:
            path (str): filepath
        """
        self.path = path

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
                    if md5(path1) != md5(path2):
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
            did = RucioDID(path, 
                           name, 
                           self.config["scope"], 
                           ds_name, 
                           self.config["scope"])
            did.configure(self.config["register_after_upload"], self.config["upl_rse"])
            self.dids[did.get_scoped_name()] = did


    def createDatasets(self):
        """Creates RUCIO Datasets
        """
        for did in self.dids.values():
            sname = get_scoped_name(did.ds_name, did.ds_scope)
            if sname not in self.datasets:
                self.datasets[sname] = RucioDataset(did.ds_name, self.config["scope"], [did])
            self.datasets[sname].dids.append(did)


    def createRules(self):
        """Creates RUCIO Rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = RucioRule(self.config["dst_rse"], ds.name, ds.scope)

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
                did = RucioDID(path, 
                            name, 
                            self.config["scope"], 
                            ds_name, 
                            self.config["scope"])
                did.configure(self.config["register_after_upload"], self.config["upl_rse"])
                self.dids[did.get_scoped_name()] = did


    def createDatasets(self):
        """Creates RUCIO Datasets
        """
        for did in self.dids.values():
            sname = get_scoped_name(did.ds_name, did.ds_scope)
            if sname not in self.datasets:
                self.datasets[sname] = RucioDataset(did.ds_name, self.config["scope"], [did])
            self.datasets[sname].dids.append(did)


    def createRules(self):
        """Creates RUCIO rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = RucioRule(self.config["dst_rse"], ds.name, ds.scope)

class RucioClient:
    """Wrapper of several RUCIO clients
    """
    def __init__(self, log_file: TextIOWrapper):
        """RucioClient constructor

        Args:
            log_file (TextIOWrapper): log file
        """
        self.DIDCLIENT = DIDClient()
        self.RULECLIENT = RuleClient()
        self.log_file = log_file
    
    def log(self, message: str):
        """Log message

        Args:
            message (str): txt message to be logged
        """
        mutex.acquire()
        try:
            self.log_file.write(message)
            self.log_file.flush()
        finally:
            mutex.release()

    def dids_in_rucio(self, scope: str) -> list:
        """Get list of DIDs in RUCIO within the scope

        Args:
            scope (str): scope

        Returns:
            list: list of DIDs in RUCIO within the scope
        """
        return [get_scoped_name(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="file"))]
    
    def dataset_in_rucio(self, scope: str) -> list:
        """Get list of datasets in RUCIO within the scope

        Args:
            scope (str): scope

        Returns:
            list: list of datasets in RUCIO within the scope
        """
        return [get_scoped_name(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="dataset"))]

    def dids_in_dataset(self, dataset_scope: str, dataset_name: str) -> list:
        """Get list of DIDs within a dataset

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name

        Returns:
            list: list of DIDs within a dataset
        """
        return [get_scoped_name(x["name"],x["scope"]) for x in list(self.DIDCLIENT.list_content(dataset_scope,dataset_name))]
    
    def upload(self, items: list, client: UploadClient) -> bool:
        """Upload items

        Args:
            items (list): list of items to be uploaded
            client (UploadClient): RUCIO Upload Client

        Returns:
            bool: True if upload is ok, False otherwise
        """
        self.log(format_log_message("uploading {}".format([x['did_name'] for x in items]), self.upload))
        try:
            client.upload(items)
            pass
        except exception.NoFilesUploaded:
            self.log(format_log_message("uploading {} .. fail: NoFilesUploaded".format([x['did_name'] for x in items]), self.upload))
            return False
        except exception.ServerConnectionException:
            self.log(format_log_message("uploading {} .. fail: ServerConnectionException".format([x['did_name'] for x in items]), self.upload))
            return False
        except exception.DataIdentifierNotFound:
            self.log(format_log_message("uploading {} .. fail: DataIdentifierNotFound".format([x['did_name'] for x in items]), self.upload))
            return False
        else:
            self.log(format_log_message("uploading {} .. done".format([x['did_name'] for x in items]), self.upload))
            for x in items:
                x["upload_ok"] = True
            return True
    
    def upload_batch(self, items: list):
        """Upload a batch of items

        Args:
            items (list): batch of items

        Returns:
            tuple: list of successfully and failed uploaded items
        """
        UPCLIENT = UploadClient()
        for item in items:
            if not self.upload([item], UPCLIENT):
                UPCLIENT = UploadClient()
    
    def attach(self, dataset_scope: str, dataset_name: str, items: list):
        """Attach items to RUCIO dataset

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
            items (list): list of items to be attached
        """
        self.log(format_log_message("attaching {} in {}:{}".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
        self.DIDCLIENT.attach_dids(dataset_scope,dataset_name,items)
        self.log(format_log_message("attaching {} in {}:{} .. done".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
    
    def rules_in_rucio(self, filter: dict) -> list:
        """Get list of rules in RUCIO

        Args:
            filter (dict): filter

        Returns:
            list: list of rules in RUCIO
        """
        return [get_scoped_name(rule['name'],rule['scope']) for rule in list(self.RULECLIENT.list_replication_rules(filter))]
    
    def add_dataset(self, dataset_scope: str, dataset_name: str):
        """Add a dataset to RUCIO

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
        """
        self.log(format_log_message("adding dataset {}:{}".format(dataset_scope, dataset_name), self.add_dataset))
        self.DIDCLIENT.add_dataset(dataset_scope, dataset_name)
        self.log(format_log_message("adding dataset {}:{} .. done".format(dataset_scope, dataset_name), self.add_dataset))
    
    def add_rule(self, dataset_scope: str, dataset_name: str, n_replicas: int, rse: str):
        """Add a rule to RUCIO

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
            n_replicas (int): number of replicas
            rse (str): RUCIO storage element
        """
        self.log(format_log_message("adding rule for {}:{} to {}".format(dataset_scope, dataset_name, rse), self.add_rule))
        self.RULECLIENT.add_replication_rule([{"scope":dataset_scope, "name": dataset_name}], n_replicas, rse)
        self.log(format_log_message("adding rule for {}:{} to {} .. done".format(dataset_scope, dataset_name, rse), self.add_rule))

class RucioManager:
    """Manager of the interaction with RUCIO 
    """
  
    def __init__(self, dids: dict, datasets: dict, rules: dict, config: dict):
        """RucioManager constructor

        Args:
            dids (dict): input items
            datasets (dict): input datasets
            rules (dict): input rules
            config (dict): configuration
        """
        self.log = open(datetime.now().strftime('uploader_%Y_%m_%d_%H_%M_%S.log'),"w")
        self.rucio = RucioClient(self.log)
        self.scope = config["scope"]
        self.rse = config["dst_rse"]
        self.dids = dids
        self.datasets = datasets
        self.rules = rules
        self.to_upload = []
    
    def __del__(self):
        """RucioManager destructor
        """
        self.log.close()
    
    def log_summary(self):
        up_all = len(self.to_upload)
        up_ok = sum(map(lambda x : x["upload_ok"] == True, self.to_upload))
        up_all_size = sum(map(lambda x : x["size"], self.to_upload))
        up_ok_size = sum(map(lambda x : x["size"], list(filter(lambda x: x["upload_ok"] == True, self.to_upload))))
        
        self.log.write(" ============ summary =====================\n")
        self.log.write("   uploaded files: {} of {}\n".format(up_ok, up_all))
        self.log.write("   uploaded bytes: {} of {}\n".format(up_ok_size, up_all_size))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_dids_input(self):
        """Log
        """
        self.log.write(" ============ input dids =====================\n")
        for v in self.dids.values():
            self.log.write("   {}, {}, {}, {}, {}, {}, {}, {}\n".format(v.name, v.scope, v.ds_name, v.ds_scope, v.path, v.in_rucio, v.in_dataset, v.size))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_datasets_input(self):
        """Log
        """
        self.log.write(" ============ input datasets =================\n")
        for v in self.datasets.values():
            self.log.write("   {}, {}, {}\n".format(v.name, v.scope, v.in_rucio))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_rules_input(self):
        """Log
        """
        self.log.write(" ============ input rules ====================\n")
        for k,v in self.rules.items():
            self.log.write("   {}, {}, {}, {}, {}\n".format(v.name, v.scope, v.ncopy, v.rse, v.in_rucio))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_input(self):
        """Log inputs
        """
        self.log_dids_input()
        self.log_datasets_input()
        self.log_rules_input()

    def log_dids_in_rucio(self, files: list):
        """Log
        """
        self.log.write(" ============ files in RUCIO =================\n")
        for f in files:
            self.log.write("   {}\n".format(f))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_dids_in_dataset(self, datasets: dict):
        """Log
        """
        self.log.write(" ============ files in datasets ==============\n")
        for name, ds in datasets.items():
            for did in ds:
                self.log.write("   {}, {}\n".format(name,did))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_rules_in_rucio(self, rules: list):
        """Log
        """
        self.log.write(" ============ rules in RUCIO =================\n")
        for r in rules:
            self.log.write("   {}\n".format(r))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_datasets_in_rucio(self, datasets: list):
        """Log
        """
        self.log.write(" ============ datasets in RUCIO ==============\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_rucio(self, dids: list, datasets: list, dids_in_dataset: dict, rules: list):
        """Log
        """
        self.log_dids_in_rucio(dids)
        self.log_datasets_in_rucio(datasets)
        self.log_dids_in_dataset(dids_in_dataset)
        self.log_rules_in_rucio(rules)

    def log_dids_to_upload(self, files: list):
        """Log
        """
        self.log.write(" ============ files to upload ================\n")
        for f in files:
            self.log.write("   {}, {}, {}, {}, {}, {}, {}, {}\n".format(f['did_scope'], 
                                                                  f['did_name'], 
                                                                  f['dataset_scope'],
                                                                  f['dataset_name'],
                                                                  f['rse'],
                                                                  f['register_after_upload'],
                                                                  f['path'],
                                                                  f['size']))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_datasets_to_add(self, datasets: list):
        """Log
        """
        self.log.write(" ============ datasets to add ================\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds.get_scoped_name()))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_rules_to_add(self, rules: list):
        """Log
        """
        self.log.write(" ============ rules to add ===================\n")
        for rule in rules:
            self.log.write("   {}\n".format(rule.get_scoped_name()))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_dids_to_attach(self, datasets: list):
        """Log
        """
        self.log.write(" ============ files to attach ================\n")
        for dn, files in datasets.items():
            for f in files:
                self.log.write("   {}, {}, {}\n".format(dn, f['name'], f['scope']))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_todo(self, dids: list, datasets: list, rules: list):
        """Log
        """
        self.log_dids_to_upload(self, dids)
        self.log_datasets_to_add(self, datasets)
        self.log_dids_to_attach(self, datasets)
        self.log_rules_to_add(self, rules)

    def start_log(self):
        """Log
        """
        self.log.write(" ============ run start ======================\n")
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def stop_log(self):
        """Log
        """
        self.log.write(" ============ run complete ===================\n")
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def add_datasets(self, datasets: list):
        """Add datasets to RUCIO

        Args:
            datasets (list): list of datasets to be added
        """
        self.log.write(" ============ add datasets ===================\n")
        for ds in datasets:
            self.rucio.add_dataset(ds.scope, ds.name)
        self.log.write(" =============================================\n\n")
    
    def add_rules(self, datasets: list):
        """Add rules to RUCIO

        Args:
            datasets (list): list of datasets for which rules are added
        """
        self.log.write(" ============ add rules ======================\n")
        for ds in datasets:
            self.rucio.add_rule(ds.scope, ds.name, ds.ncopy, ds.rse) 
        self.log.write(" =============================================\n\n")

    def rucio_info(self):
        """Get info from RUCIO for the input items
        """
        dids_in_rucio = self.rucio.dids_in_rucio(self.scope)
        rules_in_rucio = self.rucio.rules_in_rucio({'rse_expression': self.rse})
        dataset_in_rucio = self.rucio.dataset_in_rucio(self.scope)

        dids_in_dataset = {}
        for name, ds in self.datasets.items():
            dids_in_dataset[name] = self.rucio.dids_in_dataset(ds.scope, ds.name)
            ds.in_rucio = True if name in dataset_in_rucio else False

        self.log_rucio(dids_in_rucio, dataset_in_rucio, dids_in_dataset, rules_in_rucio)

        for name, rule in self.rules.items():
            rule.in_rucio = True if name in rules_in_rucio else False

        for name, did in self.dids.items():
            did.in_rucio = True if name in dids_in_rucio else False
            did.in_dataset = True if name in dids_in_dataset[get_scoped_name(did.ds_name, did.ds_scope)] else False

    def dids_to_upload(self) -> list:
        """Create list of items to be uploaded 

        Returns:
            list: list of items to be uploaded 
        """
        to_upload = []
        for v in self.dids.values():
            if v.in_rucio == False:
                to_upload.append(v.asUpload)
        self.log_dids_to_upload(to_upload)
        return to_upload
    
    def dids_to_attach(self) -> dict:
        """Create dictionary ("dataset":"list of items") of items to be attached to datasets

        Returns:
            dict: dictionary ("dataset":"list of items") of items to be attached to datasets
        """
        to_attach = {}
        for v in self.dids.values():
            if v.in_dataset == False and v.in_rucio == True:
                dn = get_scoped_name(v.ds_name, v.ds_scope)
                if dn not in to_attach:
                    to_attach[dn] = []
                to_attach[dn].append(v.asAttach)
        self.log_dids_to_attach(to_attach)
        return to_attach

    def datasets_to_add(self) -> list:
        """Create list of datasets to be added to RUCIO

        Returns:
            list: list of datasets to be added to RUCIO
        """
        datasets_to_add = [ds for ds in self.datasets.values() if not ds.in_rucio]
        self.log_datasets_to_add(datasets_to_add)
        return datasets_to_add

    def rules_to_add(self) -> list:
        """Create list of datasets to be added to RUCIO

        Returns:
            list: list of datasets to be added to RUCIO
        """
        rules_to_add = [rule for rule in self.rules.values() if not rule.in_rucio]
        self.log_rules_to_add(rules_to_add)
        return rules_to_add
    
    def add_all_datasets(self):
        """Add datasets to RUCIO
        """
        datasets_to_add = self.datasets_to_add()
        self.add_datasets(datasets_to_add)
    
    def add_all_rules(self):
        """Add rules to RUCIO
        """
        rules_to_add = self.rules_to_add()
        self.add_rules(rules_to_add)
    
    def upload_all(self, n_batches: int):
        """Upload all items

        Args:
            n_batches (int): number of parallel threads
        """
        batches = []
        for i in range(n_batches):
            batches.append([])
        
        self.to_upload = self.dids_to_upload()
        for i in range(len(self.to_upload)):
            batches[i%n_batches].append(self.to_upload[i])
            
        self.log.write(" ============ upload =========================\n")
        threads = []
        for i in range(n_batches):
            threads.append(Thread(target = self.rucio.upload_batch, args = ([batches[i]])))
        
        for t in threads:
            t.start()

        for t in threads:
            t.join()
        self.log.write(" =============================================\n\n")

    def attach_all(self):
        """Attach all items 
        """
        dids_to_attach = self.dids_to_attach()
        self.log.write(" ============ attach =========================\n")
        for ds, items in dids_to_attach.items():
          scope, name = get_scope_and_name(ds)
          self.rucio.attach(scope, name, items)
        self.log.write(" =============================================\n\n")

    def run(self):
        """Process all items
        """
        self.start_log()
        self.rucio_info()
        self.log_input()
        self.attach_all()
        self.add_all_datasets()
        self.add_all_rules()
        self.upload_all(20)
        self.log_summary()
        self.stop_log()

parser = argparse.ArgumentParser(prog="rucio_uploader.py", 
                            description='Upload files to RUCIO and create replicas at CNAF')
                            
parser.add_argument('--type', 
                    nargs=1,
                    choices=['dir', 'tar'],
                    required=True,
                    help="type of input either directory or list of tar file",
                    dest="type")

parser.add_argument('source',
                    nargs="+",
                    help="list of sources")

if __name__ == '__main__':
    args = parser.parse_args()

    config = {"scope":"user.icaruspro",
            "upl_rse":"FNAL_DCACHE", 
            "dst_rse":"INFN_CNAF_DISK_TEST",
            "register_after_upload":True}
    
    if not sources_exist(args.source):
        sys.exit(0)
    
    items = None
    if args.type[0] == "tar":
        config["ds_name_template"] = "run-{}-raw"
        config["filename_run_pattern"] = r"run_([0-9]{4})_filelist.dat"
        items = TarItemsConfigurator(TarReader(args.source), config)
    elif args.type[0] == "dir":
        config["ds_name_template"] = "run-{}-calib"
        config["filename_run_pattern"] = r"hist.*_run([0-9]{4})_.*.root"
        items = FileItemsConfigurator(DirectoryTreeReader(args.source), config)
        
    RucioManager(items.dids, 
                items.datasets, 
                items.rules, 
                config).run()
