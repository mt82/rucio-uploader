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
import logging
import sys
import argparse

import rucio_uploader.utils as utils
import rucio_uploader.interfaces.file as file_interface
import rucio_uploader.interfaces.tar as tar_interface
import rucio_uploader.interfaces.samweb as sam_interface
import rucio_uploader.rucio.manager as rucio_manager

parser = argparse.ArgumentParser(prog="rucio_uploader.py", 
                            description='Upload files to RUCIO and create replicas at CNAF')
                            
parser.add_argument('--type', 
                    nargs=1,
                    choices=['dir', 'tar', 'sam'],
                    required=True,
                    help="type of input either directory, list of tar file or samweb",
                    dest="type")

parser.add_argument('--source',
                    required='tar' in sys.argv or 'dir' in sys.argv,
                    nargs="+",
                    help="list of sources")

parser.add_argument('--run_number',
                    nargs=1,
                    required='sam' in sys.argv,
                    help="run number")

parser.add_argument('--data_tier',
                    nargs=1,
                    required='sam' in sys.argv,
                    help="data tier ['raw', 'reco1', reco2', ...]")

parser.add_argument('--data_stream',
                    nargs=1,
                    required='sam' in sys.argv,
                    help="data stream ['numi', 'bnb', ...]")

if __name__ == '__main__':
    args = parser.parse_args()

    config = {"scope":"user.icaruspro",
            "upl_rse":"FNAL_DCACHE", 
            "dst_rse":"INFN_CNAF_DISK_TEST",
            "register_after_upload":True}
    
    if not utils.sources_exist(args.source):
        sys.exit(0)
    
    items = None
    if args.type[0] == "tar":
        config["ds_name_template"] = "run-{}-raw"
        config["filename_run_pattern"] = r"run_([0-9]{4})_filelist.dat"
        items = tar_interface.TarItemsConfigurator(tar_interface.TarReader(args.source), config)
    elif args.type[0] == "dir":
        config["ds_name_template"] = "run-{}-calib"
        config["filename_run_pattern"] = r"hist.*_run([0-9]{4})_.*.root"
        items = file_interface.FileItemsConfigurator(file_interface.DirectoryTreeReader(args.source), config)
    elif args.type[0] == "sam":
        config["run_number"] = args.run_number[0]
        config["data_tier"] = args.data_tier[0]
        config["data_stream"] = args.data_stream[0]
        items = sam_interface.SamwebItemsConfigurator(sam_interface.SamwebReader(args.run_number[0], args.data_tier[0], args.data_stream[0]), config)
        
    rucio_manager.RucioManager(items.dids, 
                items.datasets, 
                items.rules, 
                config,
                args,
                logging_level=logging.INFO).run()
