"""@package rucio_items_wrapper

 Wrapper of RUCIO items: did, dataset, rule

"""

import os
import rucio_uploader.utils as utils

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
        return utils.get_scoped_name(self.name,self.scope)

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
        return utils.get_scoped_name(self.name,self.scope)

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
        return utils.get_scoped_name(self.name,self.scope)