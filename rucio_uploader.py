#!/icarus/app/home/icaruspro/rucio_client_py3/bin/python -u

"""@package rucio-uploader

 This script expects, as input, a set of tar files
 containig a set of text files, each of which 
 contains a list of files related to a run.
 
 Assumptions:
 - the name of the files containg the list of files
   of a run should match a pattern and 
   the run number is extracted from it
 - the dataset defined by the script represents a run
   and is named run-XXXX-raw 

 This script:
 1- reads all the files related to the runs from a tar file
 2- translates file surl to local path
 3- Checks if the dataset exists, if not add the missing ones
 4- Checks if the rules exists, if not add the missing ones
 5- checks if the files are already in RUCIO
 6- checks if the files are already in the datatset, if not add the missing ones
 7- checks if a transfer rule already exists, if not define it for the run 
 8- uploads missing files in parallel threads

"""

# remove warning from cryptography not supporting python2 
import warnings

from upload_all import UPCLIENT
warnings.filterwarnings("ignore")

import re
import os
import sys
import tarfile

from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from rucio.common import exception
from threading import Thread, Lock
from datetime import datetime

# pattern expected for text file containing the list of list of a run
filename_run_pattern=r"run_([0-9]{4})_filelist.dat"

# lock for cuncurrent writing of log file
mutex = Lock()

def format_now():
    """!
    Return a string with date and hours formatted
    """
    return "[{}]".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def log(message, function):
    """!
    Return a line for log
    """
    return "   {} : [{}] -> \"{}\"\n".format(format_now(), function.__name__, message)

def scopedName(name, scope):
    """!
    Get name in format <scope>:<name>
    """
    return "{}:{}".format(scope,name)

def scopedItemName(item):
    """!
    Get name in format <scope>:<name>
    """
    return scopedName(item.name,item.scope)

def ScopeAndName(sname):
    splits = sname.split(':')
    return splits[0],splits[1]

class RucioDID:
    """!
    Represent a RUCIO dataset
    """
    def __init__(self, path, name, scope = None, ds_name = None, ds_scope = None):
        """!
        Dataset constructor
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

    def toUpload(self, register_after_upload, rse):
        """!
        Prepare dids to be attached to dataset
        """
        self.asUpload["path"]=self.path
        self.asUpload["did_name"] = self.name
        self.asUpload["did_scope"] = self.scope
        self.asUpload['dataset_name'] = self.ds_name
        self.asUpload['dataset_scope'] = self.ds_scope
        self.asUpload["register_after_upload"] = register_after_upload
        self.asUpload["rse"] = rse
    
    def toAttach(self):
        """!
        Prepare dids to be attached to dataset
        """
        self.asAttach['scope'] = self.scope
        self.asAttach['name'] = self.name

class RucioDataset:
    """!
    Represent a RUCIO dataset
    """

    def __init__(self, name, scope, dids):
        """!
        Dataset constructor
        """
        self.name = name
        self.scope = scope
        self.dids = dids
        self.in_rucio=False

class RucioRule:
    """!
    Represent a RUCIO rule
    """

    def __init__(self, rse, name, scope, ncopy = 1):
        """!
        Rule constructor
        """
        self.rse = rse
        self.name = name
        self.scope = scope
        self.ncopy = ncopy
        self.in_rucio=False

class RawFileItem:
    """!
    Local file descripted by a path 
    and a run it delongs
    """

    def __init__(self, path, run):
        """!
        Item constructor
        """
        self.p = path
        self.r = run

class TarReader:
    """!
    Read and process tar files
    """

    def __init__(self, tarlists):
        """!
        TarReader constructor
        """
        self.tars = tarlists
        self.items = []
        self.dids = []
        self.read()
        self.toDID()
    
    def reset(self):
        """!
        reset TarReader 
        """
        self.items = []
        self.dids = []

    
    def read(self):
        """!
        read tar and fill items 
        """
        self.reset()
        for t in self.tars:
            tar = tarfile.open(t,"r:gz")
            for el in tar.getmembers():
                if el.isfile():
                    r = self.run_number(el.name)
                    for filesurl in tar.extractfile(el).readlines():
                        fp = self.filepath(filesurl.decode("utf-8").strip())
                        self.items.append(RawFileItem(fp,r))

    def run_number(self, filename):
        """!
        Extract run number from file name
        """
        matches = re.match(filename_run_pattern, filename)
        if matches is None:
            return None
        else:
            return matches.group(1)

    def filename(self, filepath):
        """!
        Return filename from filepath
        """
        return os.path.basename(filepath)

    # return dataset name 
    def dataset_name(self, run):
        """!
        Construct dataset name from run number
        """
        return "run-{}-raw".format(run)

    def did_name(self, filepath):
        """!
        Return did name from filepath
        """
        return self.filename(filepath)

    # def run_files(directory):
    #     """!
    #     Scan a directory and return a list 
    #     of files with name matching a pattern
    #     """
    #     return [file for file in os.listdir(directory) if re.match(filename_run_pattern,file)]

    # def files_in_run(self, filename):
    #     """!
    #     Return a list of the lines within a file.
    #     This corresponds to get a list of paths of files
    #     belonging to a run
    #     """
    #     files = []
    #     with open(filename) as f:
    #         files=f.read().splitlines()
    #     return files

    def filepath(self, filesurl):
        """!
        Translate file surl to local file path
        """
        return re.sub("gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr",
                    "/pnfs",
                    filesurl)
    
    def toDID(self):
        """!
        Convert RawFileItems into Rucio dids
        and related datasets and rules
        """
        for item in self.items:
            self.dids.append(RucioDID(item.p, self.did_name(item.p), None, self.dataset_name(item.r), None))

class RucioItemsCreator:
    """!
    RucioItemsCreator creates dataset and rules 
    """

    def __init__(self, dids, scope, upl_rse, dst_rse):
        """!
        RucioItemsCreator constructor
        """
        self.scope = scope
        self.upl_rse = upl_rse
        self.dst_rse = dst_rse
        self.dids = {}
        self.datasets = {}
        self.rules = {}
        self.createDIDs(dids)
        self.createDatasets()
        self.createRules()
    
    def createDIDs(self, dids):
        """!
        Creates DIDs
        """
        for did in dids:
            did.scope = self.scope
            did.ds_scope = self.scope
            did.toUpload(True, self.upl_rse)
            did.toAttach()
            self.dids[scopedItemName(did)] = did


    def createDatasets(self):
        """!
        Creates Datasets
        """
        for did in self.dids.values():
            sname = scopedName(did.ds_name, did.ds_scope)
            if sname not in self.datasets:
                self.datasets[sname] = RucioDataset(did.ds_name, self.scope, [did])
            self.datasets[sname].dids.append(did)


    def createRules(self):
        """!
        Creates Rules
        """
        for sname, ds in self.datasets.items():
            self.rules[sname] = RucioRule(self.dst_rse, ds.name, ds.scope)

class RucioClient:
    """!
    Rucio client with did, upload 
    and rule functionality
    """
    def __init__(self, log_file):
        """!
        Rucio client constructor
        """
        self.DIDCLIENT = DIDClient()
        self.RULECLIENT = RuleClient()
        self.log_file = log_file
    
    def log(self, message):
        """!
        log message
        """
        mutex.acquire()
        try:
            self.log_file.write(message)
            self.log_file.flush()
        finally:
            mutex.release()

    def dids_in_rucio(self, scope):
        """!
        Get list of files in Rucio
        """
        return [scopedName(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="file"))]
    
    def dataset_in_rucio(self, scope):
        """!
        Get list of datasets in Rucio
        """
        return [scopedName(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="dataset"))]

    def dids_in_dataset(self,dataset_scope,dataset_name):
        """!
        Get list of files grouped by dataset in Rucio
        """
        return [scopedName(x["name"],x["scope"]) for x in list(self.DIDCLIENT.list_content(dataset_scope,dataset_name))]
    
    def upload(self, items, client):
        """!
        Upload files
        """
        self.log(log("uploading {}".format([x['did_name'] for x in items]), self.upload))
        try:
            #client.upload(items)
            pass
        except exception.NoFilesUploaded:
            self.log(log("uploading {} .. fail".format([x['did_name'] for x in items]), self.upload))
            return False
        else:
            self.log(log("uploading {} .. done".format([x['did_name'] for x in items]), self.upload))
            return True
    
    def upload_batch(self, items):
        """!
        upload batch of items
        """
        UPCLIENT = UploadClient()
        for item in items:
            if not self.upload([item], UPCLIENT):
                UPCLIENT = UploadClient()
    
    def attach(self, dataset_scope, dataset_name, items):
        """!
        Attach files to a dataset
        """
        self.log(log("attaching {} in {}:{}".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
        #self.DIDCLIENT.attach_dids(dataset_scope,dataset_name,items)
        self.log(log("attaching {} in {}:{} .. done".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
    
    def rules_in_rucio(self, filter):
        """!
        Get list of rules surviving filter 
        """
        return [scopedName(rule['name'],rule['scope']) for rule in list(self.RULECLIENT.list_replication_rules(filter))]
    
    def add_dataset(self, dataset_scope, dataset_name):
        """!
        Add dataset
        """
        self.log(log("adding dataset {}:{}".format(dataset_scope, dataset_name), self.add_dataset))
        #self.DIDCLIENT.add_dataset(dataset_scope, dataset_name)
        self.log(log("adding dataset {}:{} .. done".format(dataset_scope, dataset_name), self.add_dataset))
    
    def add_rule(self, dataset_scope, dataset_name, n_replicas, rse):
        """!
        Add rule
        """
        self.log(log("adding rule for {}:{} to {}".format(dataset_scope, dataset_name, rse), self.add_rule))
        #self.RULECLIENT.add_replication_rule([{"scope":dataset_scope, "name": dataset_name}], n_replicas, rse)
        self.log(log("adding rule for {}:{} to {} .. done".format(dataset_scope, dataset_name, rse), self.add_rule))

class Uploader:
    """!
    Uploader manages all the processes
    needed to replicate data at the final RSE

    In particular, it:
    - add missing datasets
    - add missing rules
    - add missing files to datasets
    - upload missing files to Rucio
    """
  
    def __init__(self, scope, rse, dids, datasets, rules):
        """!
        uploader constructor
        """
        self.log = open(datetime.now().strftime('uploader_%Y_%m_%d_%H_%M_%S.log'),"w")
        self.rucio = RucioClient(self.log)
        self.scope = scope
        self.rse = rse
        self.dids = dids
        self.datasets = datasets
        self.rules = rules
    
    def __del__(self):
        """!
        uploader destructor
        """
        self.log.close()
    
    # def log_initialization(self):
    #     """!
    #     log initialization
    #     """
    #     self.log.write(" ============ initialization =================\n")
    #     self.log.write("   scope    : {}\n".format(self.scope))
    #     self.log.write("   rse      : {}\n".format(self.rse))
    #     self.log.write(" =============================================\n\n")
    #     self.log.flush()
    
#    def log_directory(self, directory):
#        self.log.write(" ============ directory ======================\n")
#        self.log.write("   directory: {}\n".format(directory))
#        self.log.write(" =============================================\n\n")
    
    # def log_tarfile(self, tfiles):
    #     """!
    #     log tar files
    #     """
    #     self.log.write(" ============ input tar file =================\n")
    #     for f in tfiles:
    #         self.log.write("   tar file : {}\n".format(f))
    #     self.log.write(" =============================================\n\n")
    #     self.log.flush()
    
    def log_dids_input(self):
        """!
        log input dids
        """
        self.log.write(" ============ input dids =====================\n")
        for v in self.dids.values():
            self.log.write("   {}, {}, {}, {}, {}, {}, {}\n".format(v.name, v.scope, v.ds_name, v.ds_scope, v.path, v.in_rucio, v.in_dataset))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_datasets_input(self):
        """!
        log input datasets
        """
        self.log.write(" ============ input datasets =================\n")
        for v in self.datasets.values():
            self.log.write("   {}, {}, {}\n".format(v.name, v.scope, v.in_rucio))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_rules_input(self):
        """!
        log input rules
        """
        self.log.write(" ============ input rules ====================\n")
        for k,v in self.rules.items():
            self.log.write("   {}, {}, {}, {}, {}\n".format(v.name, v.scope, v.ncopy, v.rse, v.in_rucio))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_input(self):
        self.log_dids_input()
        self.log_datasets_input()
        self.log_rules_input()

    def log_dids_in_rucio(self, files):
        """!
        log files in rucio
        """
        self.log.write(" ============ files in RUCIO =================\n")
        for f in files:
            self.log.write("   {}\n".format(f))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_dids_in_dataset(self, datasets):
        """!
        log files in dataset
        """
        self.log.write(" ============ files in datasets ==============\n")
        for name, ds in datasets.items():
            for did in ds:
                self.log.write("   {}, {}\n".format(name,did))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_rules_in_rucio(self, rules):
        """!
        log rules in rucio
        """
        self.log.write(" ============ rules in RUCIO =================\n")
        for r in rules:
            self.log.write("   {}\n".format(r))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_datasets_in_rucio(self, datasets):
        """!
        log datasets in rucio
        """
        self.log.write(" ============ datasets in RUCIO ==============\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_rucio(self, dids, datasets, dids_in_dataset, rules):
        self.log_dids_in_rucio(dids)
        self.log_datasets_in_rucio(datasets)
        self.log_dids_in_dataset(dids_in_dataset)
        self.log_rules_in_rucio(rules)

    def log_dids_to_upload(self, files):
        """!
        log files to upload
        """
        self.log.write(" ============ files to upload ================\n")
        for f in files:
            self.log.write("   {}, {}, {}, {}, {}, {}, {}\n".format(f['did_scope'], 
                                                                  f['did_name'], 
                                                                  f['dataset_scope'],
                                                                  f['dataset_name'],
                                                                  f['rse'],
                                                                  f['register_after_upload'],
                                                                  f['path']))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_datasets_to_add(self, datasets):
        """!
        log datasets to add
        """
        self.log.write(" ============ datasets to add ================\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_rules_to_add(self, rules):
        """!
        log rules to add
        """
        self.log.write(" ============ rules to add ===================\n")
        for rule in rules:
            self.log.write("   {}\n".format(rule))
        self.log.write(" =============================================\n\n")
        self.log.flush()

    def log_dids_to_attach(self, datasets):
        """!
        log files to attach
        """
        self.log.write(" ============ files to attach ================\n")
        for dn, files in datasets.items():
            for f in files:
                self.log.write("   {}, {}, {}\n".format(dn, f['name'], f['scope']))
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    def log_todo(self, dids, datasets, rules):
        self.log_dids_to_upload(self, dids)
        self.log_datasets_to_add(self, datasets)
        self.log_dids_to_attach(self, datasets)
        self.log_rules_to_add(self, rules)


    def log_finalize(self):
        """!
        log finalize
        """
        self.log.write(" ============ run complete ===================\n")
        self.log.write(" =============================================\n\n")
        self.log.flush()
    
    # def init(self, scope, rse):
    #     """!
    #     initialize uploader
    #     """
    #     self.scope = scope
    #     self.rse = rse

    # def did_name(self, item):
    #     """!
    #     get did name from item
    #     """
    #     return item.did_name()

    # def did_scope(self):
    #     """!
    #     get did scope
    #     """
    #     return self.scope
    
#    def did(self, item):
#        return f"{self.did_scope()}:{self.did_name(item)}"

    # def dataset_name(self, run):
    #     """!
    #     get did name from item
    #     """
    #     return dataset_name(run)
    
    # def dataset_scope(self):
    #     """!
    #     get dateset scope
    #     """
    #     return self.scope

#    def dataset(self,item):
#        return f"{self.dataset_scope()}:{self.dataset_name(item)}"

    # def reset(self):
    #     """!
    #     reset item dictionary
    #     """
    #     self.items = {}
    
    # def read_tar(self, tfiles):
    #     """!
    #     read tar and fill items 
    #     """
    #     self.reset()
    #     for t in tfiles:
    #         tar = tarfile.open(t,"r:gz")
    #         for el in tar.getmembers():
    #             if el.isfile():
    #                 r = run_number(el.name)
    #                 for filesurl in tar.extractfile(el).readlines():
    #                     fp = filepath(filesurl.strip())
    #                     fn = filename(fp)
    #                     self.items[fn] = RawFileItem(fp,r)

#    def read(self, directory):
#        self.reset()
#        for run_file in run_files(directory):
#            r = run_number(run_file)
#            for filesurl in files_in_run(os.path.join(directory,run_file)):
#                fp = filepath(filesurl)
#                fn = filename(fp)
#                self.items[fn] = item(fp,r)
    
    # def runs(self):
    #     """!
    #     get list of runs
    #     """
    #     runs = set()
    #     for k,v in self.items.items():
    #         runs.add(v.run())
    #     return runs
    
    def add_datasets(self, datasets):
        """!
        add datasets
        """
        self.log.write(" ============ add datasets ===================\n")
        for ds in datasets:
            self.rucio.add_dataset(ds.scope, ds.name)
        self.log.write(" =============================================\n\n")
    
    def add_rules(self, datasets):
        """!
        add rules
        """
        self.log.write(" ============ add rules ======================\n")
        for ds in datasets:
            self.rucio.add_rule(ds.scope, ds.name, ds.ncopy, ds.rse) 
        self.log.write(" =============================================\n\n")

    def rucio_info(self):
        """!
        add rucio info to items
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
            did.in_dataset = True if name in dids_in_dataset[scopedName(did.ds_name, did.ds_scope)] else False
        
        # datasets_to_add = [ds for name, ds in self.datasets().items() if name not in dataset_in_rucio]
        # rules_to_add = [rule for name, rule in self.rules().items() if name not in rules_in_rucio]
        
        # self.log_datasets_to_add(datasets_to_add)
        # self.log_rules_to_add(rules_to_add)
        
        # self.add_datasets(datasets_to_add)
        # self.add_rules(rules_to_add)
    
    # def to_upload(self, fileitem):
    #     """!
    #     prepare an item to be uploaded to rucio
    #     """
    #     fup={}
    #     fup["path"]=fileitem.path()
    #     fup["did_name"] = self.did_name(fileitem)
    #     fup["did_scope"] = self.did_scope()
    #     fup['dataset_name'] = self.dataset_name(fileitem.run())
    #     fup['dataset_scope'] = self.dataset_scope()
    #     fup["register_after_upload"] = True
    #     fup["rse"] = 'FNAL_DCACHE'
    #     return fup
    
    # def to_attach(self, item):
    #     """!
    #     prepare an item to be attached to dataset
    #     """
    #     fat={}
    #     fat['scope'] = self.did_scope()
    #     fat['name'] = self.did_name(item)
    #     return fat

    def dids_to_upload(self):
        """!
        prepare list of items to be uploaded
        """
        to_upload = []
        for v in self.dids.values():
            if v.in_rucio == False:
                to_upload.append(v.asUpload)
        self.log_dids_to_upload(to_upload)
        return to_upload
    
    def dids_to_attach(self):
        """!
        prepare list of items to be attached to datasets
        """
        to_attach = {}
        for v in self.dids.values():
            if v.in_dataset == False and v.in_rucio == True:
                dn = scopedItemName(v)
                if dn not in to_attach:
                    to_attach[dn] = []
                to_attach[dn].append(v.asAttach)
        self.log_dids_to_attach(to_attach)
        return to_attach

    def datasets_to_add(self):
        datasets_to_add = [ds for ds in self.datasets().values() if not ds.in_rucio]
        self.log_datasets_to_add(datasets_to_add)
        return datasets_to_add

    def rules_to_add(self):
        rules_to_add = [rule for rule in self.rules().values() if not rule.in_rucio]
        self.log_rules_to_add(rules_to_add)
        return rules_to_add
    
    def add_datasets(self):
        datasets_to_add = self.datasets_to_add()
        self.add_datasets(datasets_to_add)
    
    def add_rules(self):
        rules_to_add = self.rules_to_add()
        self.add_rules(rules_to_add)
    
    # def upload_item(self, item):
    #     """!
    #     upload list of items: generarly one
    #     """
    #     self.rucio.upload([item])

    # def upload_batch(self, items):
    #     """!
    #     upload batch of items
    #     """
    #     for item in items:
    #         self.upload_item(item)
    
    def upload_all(self, n_batches):
        """!
        upload all items
        """
        batches = []
        for i in range(n_batches):
            batches.append([])
        
        to_upload = self.dids_to_upload()
        for i in range(len(to_upload)):
            batches[i%n_batches].append(to_upload[i])
            
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
        """!
        attach all items
        """
        dids_to_attach = self.dids_to_attach()
        self.log.write(" ============ attach =========================\n")
        for ds, items in dids_to_attach.items():
          name, scope = ScopeAndName(ds)
          self.rucio.attach(scope, name, items)
        self.log.write(" =============================================\n\n")

    def run(self):
        """!
        process items
        """
        self.rucio_info()
        self.log_input()
        self.attach_all()
        self.upload_all(20)
        self.log_finalize()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        trf = TarReader(sys.argv[1:])
        ric = RucioItemsCreator(trf.dids, "user.icaruspro", "FNAL_DCACHE", "INFN_CNAF_DISK_TEST")
        up = Uploader(ric.scope, ric.dst_rse, ric.dids, ric.datasets, ric.rules)
        up.run()
