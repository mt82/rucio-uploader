#!/icarus/app/home/icaruspro/rucio_client/bin/python

import os
import sys
import re
import time
import json
from threading import Thread
from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from rucio.common import exception
from datetime import datetime

DIDCLIENT = DIDClient()
UPCLIENT = UploadClient()
RULECLIENT = RuleClient()

#scope="user.icaruspro"
#dirname_run_file="/icarus/app/home/icaruspro/rucio-op/run_to_transfer_102021/run_files/posix"
#timeout = 300
#nbatch = 10
#config["run_file_name_pattern"]=r"run_([0-9]{4})_filelist.local.dat"

file_in_rucio = []

config = {}

def read_config():
  global config
  with open('config.json') as f:
    config = json.load(f)

def get_did(name, scope):
  return "{}:{}".format(scope,name)

def filename_from_path(filepath):
  return os.path.basename(filepath)

def file_exists_in_fs(filepath):
  return os.path.isfile(filepath)

def get_file_in_rucio():
  return list(DIDCLIENT.list_dids(config["scope"],{},type="file"))

def file_exists_in_rucio(name):
  global file_in_rucio
  if name in file_in_rucio:
    return True
  else:
    return False

def get_dataset_name(run):
  return "run-{}-raw".format(run)

def get_file_to_upload(filepath, did_name, did_scope, dataset_scope, dataset_name):
  fup={}
  fup["path"]=filepath
  fup["did_name"] = did_name
  fup["did_scope"] = did_scope
  fup["register_after_upload"] = True
  fup["rse"] = 'FNAL_DCACHE'
  fup['dataset_scope'] = dataset_scope
  fup['dataset_name'] = dataset_name
  return fup

def get_file_to_attach_to_dataset(filename, scope):
  fat={}
  fat['scope'] = scope
  fat['name'] = filename
  return fat

def get_now():
  return "{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def upload_files(list_of_files, log_file):
  log_file.write(" Start Upload = {}\n".format(get_now()))
  imin = 0
  imax = min(config["nbatch"],len(list_of_files))
  while(True):
    if imin >= len(list_of_files):
      break
    if imax > len(list_of_files):
      imax = len(list_of_files)
    ok = False
    items = list_of_files[imin:imax]
    while ok == False:
      try:
        for item in items:
          log_file.write(" Uploading: {} start  {}\n".format(item["did_name"], get_now()))  
        UPCLIENT.upload(items)
        for item in items:
          log_file.write(" Uploading: {} finish {}\n".format(item["did_name"], get_now()))
        ok = True
        imin += config["nbatch"]
        imax += config["nbatch"]
      except exception.ServerConnectionException:
        time.sleep(config["timeout"])
  log_file.write(" Finish Upload = {}\n".format(get_now()))

def attach_files(list_of_files, dataset_scope, dataset_name, log_file):
  ok = False
  while ok == False:
    try:
      log_file.write(" Start Attach  = {}\n".format(get_now()))
      DIDCLIENT.attach_dids(dataset_scope,dataset_name,list_of_files)
      log_file.write(" Finish Attach = {}\n".format(get_now()))
      ok = True
    except Exception:
      time.sleep(config["timeout"])
  

def loop_missing_files(list_files, list_dataset, scope, dataset_name, dataset_scope, log_file):
  file_to_upload = []
  file_to_attach = []
  for filepath in list_files:
    filename = filename_from_path(filepath)
    if filename not in list_dataset:
      if not file_exists_in_fs(filepath):
        log_file.write("file: {} doesn't exist\n".format(filepath))
        exit(1)
      if file_exists_in_rucio(filename):
        #log_file.write("file: {} is in RUCIO\n".format(filename))
        file_to_attach.append(get_file_to_attach_to_dataset(filename, scope))
      else:
        #log_file.write("file: {} is NOT in RUCIO\n".format(filename))
        file_to_upload.append(get_file_to_upload(filepath, filename, scope, dataset_scope, dataset_name))
  if len(file_to_upload) > 0:
    upload_files(file_to_upload, log_file)
  if len(file_to_attach) > 0:
    attach_files(file_to_attach, dataset_scope, dataset_name, log_file)

def list_of_files_in_dataset(scope,name):
  while True:
    try:
      dataset_content = list(DIDCLIENT.list_content(scope,name))
      files_in_dataset = [x["name"] for x in dataset_content]
      return files_in_dataset
    except Exception:
      time.sleep(config["timeout"])

def list_of_files_in_run(run, log_file):
  path_run_file=os.path.join(config["run_file_dir"],"run_{:04d}_filelist.local.dat".format(run))

  if not os.path.exists(path_run_file):
    log_file.write("  {} doesn't exists\n".format(path_run_file))
    exit(1)

  run_files=[]

  with open(path_run_file) as f:
    run_files=f.read().splitlines()

  return run_files

def help():
  print("usage: {} <run>".format(sys.argv[0]))
  exit(1)

def upload_run(run):
  log_file = open("logs/log-{:04d}.txt".format(run),"a",0)
  log_file.write(" Start Time = {}\n".format(get_now()))

  did_scope = config["scope"]
  dataset_scope = config["scope"]
  dataset_name = get_dataset_name(run)
  dataset_did = get_did(dataset_name, dataset_scope)
  rse = "INFN_CNAF_DISK_TEST"

  log_file.write(" run           : {}\n".format(run))
  log_file.write(" did_scope     : {}\n".format(did_scope))
  log_file.write(" dataset_scope : {}\n".format(dataset_scope))
  log_file.write(" dataset_name  : {}\n".format(dataset_name))
  log_file.write(" dataset_did   : {}\n".format(dataset_did))

  ok = False
  while ok == False:
    try:
      log_file.write(" adding dataset: {}\n".format(dataset_did))
      DIDCLIENT.add_dataset(dataset_scope, dataset_name)
      ok = True
    except exception.DataIdentifierAlreadyExists:
      log_file.write(" dataset       : {} already exists\n".format(dataset_did))
      ok = True
    except exception.ServerConnectionException:
      time.sleep(config["timeout"])

  ok = False
  while ok == False:
    try:
      log_file.write(" adding rule to: {}\n".format(rse))
      RULECLIENT.add_replication_rule([{"scope":dataset_scope, "name": dataset_name}], 1, rse)
      ok = True
    except exception.DuplicateRule:
      log_file.write(" rule to       : {} already exists\n".format(rse))
      ok = True
    except exception.ServerConnectionException:
      time.sleep(config["timeout"])


  files_in_run = list_of_files_in_run(run, log_file)
  files_in_dataset = list_of_files_in_dataset(dataset_scope, dataset_name)

  log_file.write("{:6d} files in run\n".format(len(files_in_run)))
  log_file.write("{:6d} files in dataset\n".format(len(files_in_dataset)))

  loop_missing_files(files_in_run, files_in_dataset, did_scope, dataset_name, dataset_scope, log_file)

  log_file.write(" Finish Time = {}\n".format(get_now()))
  log_file.close()

def upload_runs(run_list):
  for r in run_list:
    upload_run(int(r))

if __name__ == '__main__':

  read_config()

  runs = [re.match(config["run_file_name_pattern"], f).group(1) for f in os.listdir(config["run_file_dir"]) if re.match(config["run_file_name_pattern"], f)]
  
  file_in_rucio = get_file_in_rucio()

  npar = 10
  run_lists = [] 

  for i in range(npar):
    run_lists.append([])
 
  for i in range(len(runs)):
    run_lists[i%npar].append(runs[i])

  for i in range(npar):
    run_list = run_lists[i]
    t = Thread(target = upload_runs, args = ([run_list]))
    t.start()
    print("launched thread with target: upload_runs and arguments: {}".format(run_list))

#  imin = 0
#  imax = min(imin + npar,len(runs))

#  while(True):
#    if imin >= len(runs):
#      break
#    if imax > len(runs):
#      imax = len(runs)
#    run_list = runs[imin:imax]
#    t = Thread(target = upload_runs, args = ([run_list]))
#    t.start()
#    print("launched thread with target: upload_runs and arguments: {}".format(run_list))
#    imin += npar
#    imax += npar

