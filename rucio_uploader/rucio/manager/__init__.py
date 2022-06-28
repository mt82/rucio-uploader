import os
import time
import logging
import hashlib

import rucio_uploader.utils as utils

from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from rucio.common import exception
from threading import Thread, Lock
from datetime import datetime
from io import TextIOWrapper

# lock for cuncurrent writing of log file
mutex = Lock()

class RucioClient:
    """Wrapper of several RUCIO clients
    """
    def __init__(self, rse_local_path):
        """RucioClient constructor

        """
        self.DIDCLIENT = DIDClient()
        self.RULECLIENT = RuleClient()
        self.logger = logging.getLogger()
        self.rse_local_path = rse_local_path
    
    def log(self, message: str):
        """Log message

        Args:
            message (str): txt message to be logged
        """
        mutex.acquire()
        try:
            self.logger.info(message)
        finally:
            mutex.release()

    def dids_in_rucio(self, scope: str) -> list:
        """Get list of DIDs in RUCIO within the scope

        Args:
            scope (str): scope

        Returns:
            list: list of DIDs in RUCIO within the scope
        """
        return [utils.get_scoped_name(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="file"))]
    
    def dataset_in_rucio(self, scope: str) -> list:
        """Get list of datasets in RUCIO within the scope

        Args:
            scope (str): scope

        Returns:
            list: list of datasets in RUCIO within the scope
        """
        return [utils.get_scoped_name(name, scope) for name in list(self.DIDCLIENT.list_dids(scope,{},type="dataset"))]

    def dids_in_dataset(self, dataset_scope: str, dataset_name: str) -> list:
        """Get list of DIDs within a dataset

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name

        Returns:
            list: list of DIDs within a dataset
        """
        return [utils.get_scoped_name(x["name"],x["scope"]) for x in list(self.DIDCLIENT.list_content(dataset_scope,dataset_name))]
    
    def upload(self, items: list, client: UploadClient, id: int) -> bool:
        """Upload items

        Args:
            items (list): list of items to be uploaded
            client (UploadClient): RUCIO Upload Client
            id (int): batch id

        Returns:
            bool: True if upload is ok, False otherwise
        """
        self.log("uploading {} - Thread ID: {}".format([x['did_name'] for x in items], id))
        
        result = False
        
        start = time.time()
        
        for item in items:
            hash = hashlib.md5("{}:{}".format(item['did_scope'],item['did_name']).encode('utf-8')).hexdigest()
            destination_path="{}/{}/{}/{}".format(self.rse_local_path,hash[:2],hash[2:4],item["did_name"])
            temp_destination_path="{}.rucio.upload".format(destination_path)
            if os.path.exists(destination_path):
                os.remove(destination_path)
            if os.path.exists(temp_destination_path):
                os.remove(temp_destination_path)

        try:
            client.upload(items)
        except exception.NoFilesUploaded as e:
            self.log("uploading {} - Thread ID: {} .. fail: {}".format([x['did_name'] for x in items], id, e))
        except exception.ServerConnectionException as e:
            self.log("uploading {} - Thread ID: {} .. fail: {}".format([x['did_name'] for x in items], id, e))
        except exception.DataIdentifierNotFound as e:
            self.log("uploading {} - Thread ID: {} .. fail: {}".format([x['did_name'] for x in items], id, e))
        except exception.ServiceUnavailable as e:
            self.log("uploading {} - Thread ID: {} .. fail: {}".format([x['did_name'] for x in items], id, e))
        except exception.RucioException as e:
            self.log("uploading {} - Thread ID: {} .. fail: {}".format([x['did_name'] for x in items], id, e))
        else:
            self.log("uploading {} - Thread ID: {} .. done".format([x['did_name'] for x in items], id))
            for x in items:
                x["upload_ok"] = True
            result = True
            
        
        stop = time.time()
        
        if(stop - start < 60):
            time.sleep(60.)
        
        return result
    
    def upload_batch(self, items: list, id: int):
        """Upload a batch of items

        Args:
            items (list): batch of items
            id (int): batch id

        Returns:
            tuple: list of successfully and failed uploaded items
        """
        UPCLIENT = UploadClient()
        for item in items:
            if not self.upload([item], UPCLIENT, id):
                UPCLIENT = UploadClient(logger=self.logger)
    
    def attach(self, dataset_scope: str, dataset_name: str, items: list):
        """Attach items to RUCIO dataset

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
            items (list): list of items to be attached
        """
        self.log("attaching {} in {}:{}".format([x['name'] for x in items], dataset_scope, dataset_name))
        self.DIDCLIENT.attach_dids(dataset_scope,dataset_name,items)
        self.log("attaching {} in {}:{} .. done".format([x['name'] for x in items], dataset_scope, dataset_name))
    
    def rules_in_rucio(self, filter: dict) -> list:
        """Get list of rules in RUCIO

        Args:
            filter (dict): filter

        Returns:
            list: list of rules in RUCIO
        """
        return [utils.get_scoped_name(rule['name'],rule['scope']) for rule in list(self.RULECLIENT.list_replication_rules(filter))]
    
    def add_dataset(self, dataset_scope: str, dataset_name: str):
        """Add a dataset to RUCIO

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
        """
        self.log("adding dataset {}:{}".format(dataset_scope, dataset_name))
        self.DIDCLIENT.add_dataset(dataset_scope, dataset_name)
        self.log("adding dataset {}:{} .. done".format(dataset_scope, dataset_name))
    
    def add_rule(self, dataset_scope: str, dataset_name: str, n_replicas: int, rse: str):
        """Add a rule to RUCIO

        Args:
            dataset_scope (str): dataset scope
            dataset_name (str): dataset name
            n_replicas (int): number of replicas
            rse (str): RUCIO storage element
        """
        self.log("adding rule for {}:{} to {}".format(dataset_scope, dataset_name, rse))
        self.RULECLIENT.add_replication_rule([{"scope":dataset_scope, "name": dataset_name}], n_replicas, rse)
        self.log("adding rule for {}:{} to {} .. done".format(dataset_scope, dataset_name, rse))

class RucioManager:
    """Manager of the interaction with RUCIO 
    """
  
    def __init__(self, dids: dict, datasets: dict, rules: dict, config: dict, args: dict, logging_level=logging.DEBUG):
        """RucioManager constructor

        Args:
            dids (dict): input items
            datasets (dict): input datasets
            rules (dict): input rules
            config (dict): configuration
        """
        #self.log = open(datetime.now().strftime('uploader_%Y_%m_%d_%H_%M_%S.log'),"w")
        logging.basicConfig(filename=datetime.now().strftime('uploader_%Y_%m_%d_%H_%M_%S.log'),
                    filemode='a',
                    format='%(asctime)s,%(msecs)d [%(name)s] [%(levelname)s] : %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging_level)
        self.logger = logging.getLogger()
        self.rucio = RucioClient(config["rse_local_path"])
        self.scope = config["scope"]
        self.rse = config["dst_rse"]
        self.dids = dids
        self.datasets = datasets
        self.rules = rules
        self.args = args
        self.to_upload = []
    
    def log_summary(self):
        up_all = len(self.to_upload)
        up_ok = sum(map(lambda x : x["upload_ok"] == True, self.to_upload))
        up_all_size = sum(map(lambda x : x["size"], self.to_upload))
        up_ok_size = sum(map(lambda x : x["size"], list(filter(lambda x: x["upload_ok"] == True, self.to_upload))))
        
        self.logger.info(" =============== summary =====================")
        self.logger.info("   uploaded files: {} of {}".format(up_ok, up_all))
        self.logger.info("   uploaded bytes: {} of {}".format(up_ok_size, up_all_size))
        self.logger.info(" =============================================")
    
    def log_dids_input(self):
        """Log
        """
        self.logger.debug(" ============ input dids =====================")
        for v in self.dids.values():
            self.logger.debug("   {}, {}, {}, {}, {}, {}, {}, {}".format(v.name, v.scope, v.ds_name, v.ds_scope, v.path, v.in_rucio, v.in_dataset, v.size))
        self.logger.debug(" =============================================")
    
    def log_datasets_input(self):
        """Log
        """
        self.logger.debug(" ============ input datasets =================")
        for v in self.datasets.values():
            self.logger.debug("   {}, {}, {}".format(v.name, v.scope, v.in_rucio))
        self.logger.debug(" =============================================")
    
    def log_rules_input(self):
        """Log
        """
        self.logger.debug(" ============ input rules ====================")
        for k,v in self.rules.items():
            self.logger.debug("   {}, {}, {}, {}, {}".format(v.name, v.scope, v.ncopy, v.rse, v.in_rucio))
        self.logger.debug(" =============================================")
    
    def log_input(self):
        """Log inputs
        """
        self.log_dids_input()
        self.log_datasets_input()
        self.log_rules_input()

    def log_dids_in_rucio(self, files: list):
        """Log
        """
        self.logger.debug(" ============ files in RUCIO =================")
        for f in files:
            self.logger.debug("   {}".format(f))
        self.logger.debug(" =============================================")

    def log_dids_in_dataset(self, datasets: dict):
        """Log
        """
        self.logger.debug(" ============ files in datasets ==============")
        for name, ds in datasets.items():
            for did in ds:
                self.logger.debug("   {}, {}".format(name,did))
        self.logger.debug(" =============================================")

    def log_rules_in_rucio(self, rules: list):
        """Log
        """
        self.logger.debug(" ============ rules in RUCIO =================")
        for r in rules:
            self.logger.debug("   {}".format(r))
        self.logger.debug(" =============================================")

    def log_datasets_in_rucio(self, datasets: list):
        """Log
        """
        self.logger.debug(" ============ datasets in RUCIO ==============")
        for ds in datasets:
            self.logger.debug("   {}".format(ds))
        self.logger.debug(" =============================================")
    
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
        self.logger.debug(" ============ files to upload ================")
        for f in files:
            self.logger.debug("   {}, {}, {}, {}, {}, {}, {}, {}".format(f['did_scope'], 
                                                                  f['did_name'], 
                                                                  f['dataset_scope'],
                                                                  f['dataset_name'],
                                                                  f['rse'],
                                                                  f['register_after_upload'],
                                                                  f['path'],
                                                                  f['size']))
        self.logger.debug(" =============================================")

    def log_datasets_to_add(self, datasets: list):
        """Log
        """
        self.logger.debug(" ============ datasets to add ================")
        for ds in datasets:
            self.logger.debug("   {}".format(ds.get_scoped_name()))
        self.logger.debug(" =============================================")

    def log_rules_to_add(self, rules: list):
        """Log
        """
        self.logger.debug(" ============ rules to add ===================")
        for rule in rules:
            self.logger.debug("   {}".format(rule.get_scoped_name()))
        self.logger.debug(" =============================================")

    def log_dids_to_attach(self, datasets: list):
        """Log
        """
        self.logger.debug(" ============ files to attach ================")
        for dn, files in datasets.items():
            for f in files:
                self.logger.debug("   {}, {}, {}".format(dn, f['name'], f['scope']))
        self.logger.debug(" =============================================")
    
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
        self.logger.info(" ============ run start ======================")
        self.logger.info(" =============================================")

    def stop_log(self):
        """Log
        """
        self.logger.info(" ============ run complete ===================")
        self.logger.info(" =============================================")
    
    def log_arguments(self):
        """Log
        """
        self.logger.info(self.args)
    
    def add_datasets(self, datasets: list):
        """Add datasets to RUCIO

        Args:
            datasets (list): list of datasets to be added
        """
        if len(datasets) != 0:
            self.logger.info(" ============ add datasets ===================")
            for ds in datasets:
                self.rucio.add_dataset(ds.scope, ds.name)
            self.logger.info(" =============================================")
    
    def add_rules(self, datasets: list):
        """Add rules to RUCIO

        Args:
            datasets (list): list of datasets for which rules are added
        """
        if len(datasets) != 0:
            self.logger.info(" ============ add rules ======================")
            for ds in datasets:
                self.rucio.add_rule(ds.scope, ds.name, ds.ncopy, ds.rse) 
            self.logger.info(" =============================================")

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
            did.in_dataset = True if name in dids_in_dataset[utils.get_scoped_name(did.ds_name, did.ds_scope)] else False

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
                dn = utils.get_scoped_name(v.ds_name, v.ds_scope)
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
        
        self.to_upload = self.dids_to_upload()
        
        if len(self.to_upload) == 0:
            return
        
        self.logger.info(" number of files to upload: {}".format(len(self.to_upload)))
        
        batches = []
        for i in range(n_batches):
            batches.append([])
        
        for i in range(len(self.to_upload)):
            batches[i%n_batches].append(self.to_upload[i])
            
        self.logger.info(" ============ upload =========================")
        threads = []
        for i in range(n_batches):
            threads.append(Thread(target = self.rucio.upload_batch, args = ([batches[i],i])))
        
        for t in threads:
            t.start()

        for t in threads:
            t.join()
        self.logger.info(" =============================================")

    def attach_all(self):
        """Attach all items 
        """
        dids_to_attach = self.dids_to_attach()
        if len(dids_to_attach) != 0:
            self.logger.info(" ============ attach =========================")
            for ds, items in dids_to_attach.items():
                scope, name = utils.get_scope_and_name(ds)
                self.rucio.attach(scope, name, items)
            self.logger.info(" =============================================")

    def run(self):
        """Process all items
        """
        self.start_log()
        self.log_arguments()
        self.rucio_info()
        self.log_input()
        self.attach_all()
        self.add_all_datasets()
        self.add_all_rules()
        self.upload_all(20)
        self.log_summary()
        self.stop_log()