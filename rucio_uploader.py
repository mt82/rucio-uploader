#!/icarus/app/home/icaruspro/rucio_client/bin/python -u

"""@package docstring
Documentation for this module.

More details.

# This script expects, as input, a set of tar files
# containig a set of text files, each of which 
# contains a list of files related to a run.
# 
# This script:
# 1- reads all the files related to the runs from a tar file
# 2- translates file surl to local path
# 3- checks if the files are already in RUCIO
# 4- checks if the files are already in the datatset, if not add the missing ones
# 5- checks if a transfer rule already exists, if not define it for the run 
# 6- uploads missing files in parallel threads
#
# Assumptions:
# - the name of the files containg the list of files
#   of a run should match a pattern and 
#   the run number is extracted from it
"""


# remove warning from cryptography not supporting python2 
import warnings
warnings.filterwarnings("ignore")

import re
import os
import sys
import tarfile

from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from threading import Thread, Lock
from datetime import datetime

# pattern expected for text file containing the list of list of a run
filename_run_pattern=r"run_([0-9]{4})_filelist.dat"

# lock for cuncurrent writing of log file
mutex = Lock()

def run_number(filename):
    """!
    Extract run number from file name

    @param filename String: name of the file

    @return Return the run number extracted from file name
    """
    matches = re.match(filename_run_pattern, filename)
    if matches is None:
        return None
    else:
        return matches.group(1)

def filename(filepath):
    """!
    Return filename from filepath 

    @param filename String: name of the file

    @return Return the run number extracted from file name
    """
    return os.path.basename(filepath)

# return dataset name 
def dataset_name(run):
    return "run-{}-raw".format(run)

def did_name(filepath):
    return filename(filepath)

def run_files(directory):
    return [file for file in os.listdir(directory) if re.match(filename_run_pattern,file)]

def files_in_run(filename):
    files = []
    with open(filename) as f:
        files=f.read().splitlines()
    return files

def filepath(filesurl):
    return re.sub("gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr",
                  "/pnfs",
                  filesurl)

def format_now():
    return "[{}]".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def log(message, function):
    return "   {} : [{}] -> \"{}\"\n".format(format_now(), function.__name__, message)

class rucio_client:
    def __init__(self, log_file):
        self.DIDCLIENT = DIDClient()
        self.UPCLIENT = UploadClient()
        self.RULECLIENT = RuleClient()
        self.log_file = log_file
    
    def log(self, message):
        mutex.acquire()
        try:
            self.log_file.write(message)
        finally:
            mutex.release()

    def files_in_rucio(self, scope):
        return list(self.DIDCLIENT.list_dids(scope,{},type="file"))
    
    def dataset_in_rucio(self, scope):
        return list(self.DIDCLIENT.list_dids(scope,{},type="dataset"))

    def files_in_dataset(self,dataset_scope,dataset_name):
        return [x["name"] for x in list(self.DIDCLIENT.list_content(dataset_scope,dataset_name))]
    
    def upload(self, items):
        self.log(log("uploading {}".format([x['did_name'] for x in items]), self.upload))
        self.UPCLIENT.upload(items)
        self.log(log("uploading {} .. done".format([x['did_name'] for x in items]), self.upload))
    
    def attach(self, dataset_scope, dataset_name, items):
        self.log(log("attaching {} in {}:{}".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
        self.DIDCLIENT.attach_dids(dataset_scope,dataset_name,items)
        self.log(log("attaching {} in {}:{} .. done".format([x['name'] for x in items], dataset_scope, dataset_name), self.attach))
    
    def rules_in_rucio(self, filter):
        return [rule['name'] for rule in list(self.RULECLIENT.list_replication_rules(filter))]
    
    def add_dataset(self, dataset_scope, dataset_name):
        self.log(log("adding dataset {}:{}".format(dataset_scope, dataset_name), self.add_dataset))
        self.DIDCLIENT.add_dataset(dataset_scope, dataset_name)
        self.log(log("adding dataset {}:{} .. done".format(dataset_scope, dataset_name), self.add_dataset))
    
    def add_rule(self, dataset_scope, dataset_name, n_replicas, rse):
        self.log(log("adding rule for {}:{} to {}".format(dataset_scope, dataset_name, rse), self.add_rule))
        self.RULECLIENT.add_replication_rule([{"scope":dataset_scope, "name": dataset_name}], n_replicas, rse)
        self.log(log("adding rule for {}:{} to {} .. done".format(dataset_scope, dataset_name, rse), self.add_rule))

class item:

    def __init__(self, path, run):
        self.p = path
        self.r = run
        self.in_rucio=False
        self.in_dataset=False
    
    def run(self):
        return self.r

    def path(self):
        return self.p

    def did_name(self):
        return did_name(self.p)
    
    def dataset_name(self):
        return dataset_name(self.r)

class uploader:
  
    def __init__(self):
        self.log = open(datetime.now().strftime('uploader_%H_%M_%d_%m_%Y.log'),"w")
        self.rucio = rucio_client(self.log)
        self.items = {}
        self.scope = None
        self.rse = None
    
    def __del__(self):
        self.log.close()
    
    def log_initialization(self):
        self.log.write(" ============ initialization =================\n")
        self.log.write("   scope    : {}\n".format(self.scope))
        self.log.write("   rse      : {}\n".format(self.rse))
        self.log.write(" =============================================\n\n")
    
#    def log_directory(self, directory):
#        self.log.write(" ============ directory ======================\n")
#        self.log.write("   directory: {}\n".format(directory))
#        self.log.write(" =============================================\n\n")
    
    def log_tarfile(self, tfiles):
        self.log.write(" ============ input tar file =================\n")
        for f in tfiles:
            self.log.write("   tar file : {}\n".format(f))
        self.log.write(" =============================================\n\n")
    
    def log_items(self):
        self.log.write(" ============ items ==========================\n")
        for k,v in self.items.items():
            self.log.write("   {}, {}, {}, {}, {}\n".format(k,v.path(),v.run(),v.in_rucio,v.in_dataset))
        self.log.write(" =============================================\n\n")

    def log_files_in_rucio(self, files):
        self.log.write(" ============ files in RUCIO =================\n")
        for f in files:
            self.log.write("   {}\n".format(f))
        self.log.write(" =============================================\n\n")

    def log_files_in_dataset(self, datasets):
        self.log.write(" ============ files in datasets ==============\n")
        for run, files in datasets.items():
            for f in files:
                self.log.write("   {}, {}\n".format(run,f))
        self.log.write(" =============================================\n\n")

    def log_rules_in_rucio(self, rules):
        self.log.write(" ============ rules in RUCIO =================\n")
        for r in rules:
            self.log.write("   {}\n".format(r))
        self.log.write(" =============================================\n\n")

    def log_datasets_in_rucio(self, datasets):
        self.log.write(" ============ datasets in RUCIO ==============\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds))
        self.log.write(" =============================================\n\n")

    def log_files_to_upload(self, files):
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

    def log_datasets_to_add(self, datasets):
        self.log.write(" ============ datasets to add ================\n")
        for ds in datasets:
            self.log.write("   {}\n".format(ds))
        self.log.write(" =============================================\n\n")

    def log_rules_to_add(self, rules):
        self.log.write(" ============ rules to add ===================\n")
        for rule in rules:
            self.log.write("   {}\n".format(rule))
        self.log.write(" =============================================\n\n")

    def log_files_to_attach(self, datasets):
        self.log.write(" ============ files to attach ================\n")
        for dn, files in datasets.items():
            for f in files:
                self.log.write("   {}, {}, {}\n".format(dn, f['name'], f['scope']))
        self.log.write(" =============================================\n\n")

    def log_finalize(self):
        self.log.write(" ============ run finished    ================\n")
        self.log.write(" =============================================\n\n")
    
    def init(self, scope, rse):
        self.scope = scope
        self.rse = rse

    def did_name(self, item):
        return item.did_name()

    def did_scope(self):
        return self.scope
    
#    def did(self, item):
#        return f"{self.did_scope()}:{self.did_name(item)}"

    def dataset_name(self, run):
        return dataset_name(run)
    
    def dataset_scope(self):
        return self.scope

#    def dataset(self,item):
#        return f"{self.dataset_scope()}:{self.dataset_name(item)}"

    def reset(self):
        self.items = {}
    
    def read_tar(self, tfiles):
        self.reset()
        for t in tfiles:
            tar = tarfile.open(t,"r:gz")
            for el in tar.getmembers():
                if el.isfile():
                    r = run_number(el.name)
                    for filesurl in tar.extractfile(el).readlines():
                        fp = filepath(filesurl.strip())
                        fn = filename(fp)
                        self.items[fn] = item(fp,r)

#    def read(self, directory):
#        self.reset()
#        for run_file in run_files(directory):
#            r = run_number(run_file)
#            for filesurl in files_in_run(os.path.join(directory,run_file)):
#                fp = filepath(filesurl)
#                fn = filename(fp)
#                self.items[fn] = item(fp,r)
    
    def runs(self):
        runs = set()
        for k,v in self.items.items():
            runs.add(v.run())
        return runs
    
    def add_datasets(self, dataset):
        self.log.write(" ============ add datasets ===================\n")
        for ds in dataset:
            self.rucio.add_dataset(self.scope, ds)
        self.log.write(" =============================================\n\n")
    
    def add_rules(self, dataset):
        self.log.write(" ============ add rules ======================\n")
        for ds in dataset:
            self.rucio.add_rule(self.scope, ds, 1, self.rse) 
        self.log.write(" =============================================\n\n")

    def rucio_info(self):
        files_in_rucio = self.rucio.files_in_rucio(self.scope)
        rules_in_rucio = self.rucio.rules_in_rucio({'rse_expression': self.rse})
        dataset_in_rucio = self.rucio.dataset_in_rucio(self.scope)
        
        datasets_to_add = []
        rules_to_add = []
        files_in_dataset = {}
        
        for r in self.runs():
            dn = self.dataset_name(r)
            if dn not in rules_in_rucio:
                rules_to_add.append(dn)
            if dn not in dataset_in_rucio:
                datasets_to_add.append(dn)
            files_in_dataset[r] = self.rucio.files_in_dataset(self.dataset_scope(), dn)

        for filename, item in self.items.items():
            item.in_rucio = True if filename in files_in_rucio else False
            item.in_dataset = True if filename in files_in_dataset[item.run()] else False
            
        self.log_files_in_rucio(files_in_rucio)
        self.log_files_in_dataset(files_in_dataset)
        self.log_rules_in_rucio(rules_in_rucio)
        self.log_datasets_in_rucio(dataset_in_rucio)
        
        self.log_datasets_to_add(datasets_to_add)
        self.log_rules_to_add(rules_to_add)
        
        self.add_datasets(datasets_to_add)
        self.add_rules(rules_to_add)
    
    def to_upload(self, fileitem):
        fup={}
        fup["path"]=fileitem.path()
        fup["did_name"] = self.did_name(fileitem)
        fup["did_scope"] = self.did_scope()
        fup['dataset_name'] = self.dataset_name(fileitem.run())
        fup['dataset_scope'] = self.dataset_scope()
        fup["register_after_upload"] = True
        fup["rse"] = 'FNAL_DCACHE'
        return fup
    
    def to_attach(self, item):
        fat={}
        fat['scope'] = self.did_scope()
        fat['name'] = self.did_name(item)
        return fat

    def files_to_upload(self):
        to_upload = []
        for k,v in self.items.items():
            if v.in_rucio == False:
                to_upload.append(self.to_upload(v))
        self.log_files_to_upload(to_upload)
        return to_upload
    
    def files_to_attach(self):
        to_attach = {}
        for k,v in self.items.items():
            if v.in_dataset == False and v.in_rucio == True:
                dn = self.dataset_name(v.run())
                if dataset_name not in to_attach:
                    to_attach[dn] = []
                to_attach[dn].append(self.to_attach(v))
        self.log_files_to_attach(to_attach)
        return to_attach
    
    def upload_item(self, item):
        self.rucio.upload([item])

    def upload_batch(self, items):
        for item in items:
            self.upload_item(item)
    
    def upload_all(self, n_batches):
        batches = []
        for i in range(n_batches):
            batches.append([])
        
        to_upload = self.files_to_upload()
        for i in range(len(to_upload)):
            batches[i%n_batches].append(to_upload[i])
            
        self.log.write(" ============ upload =========================\n")
        for i in range(n_batches):
            t = Thread(target = self.upload_batch, args = ([batches[i]]))
            t.start()
        self.log.write(" =============================================\n\n")

    def attach_all(self):
        self.log.write(" ============ attach =========================\n")
        for ds, items in self.files_to_attach().items():
          self.rucio.attach(self.dataset_scope(), ds, items)
        self.log.write(" =============================================\n\n")

    def run(self, tfiles):
      self.log_initialization()
      self.log_tarfile(tfiles)
      self.read_tar(tfiles)
      self.rucio_info()
      self.log_items()
      self.attach_all()
      self.upload_all(20)
      self.log_finalize()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        up = uploader()
        up.init("user.icaruspro","INFN_CNAF_DISK_TEST")
        up.run(sys.argv[1:])
