# rucio-uploader

### Requirements
- [RUCIO client](https://rucio.readthedocs.io/en/latest/installing_clients.html)

### Usage
    usage: rucio_uploader.py [-h] --type {dir,tar,sam,log}
                         [--source SOURCE [SOURCE ...]]
                         [--run_number RUN_NUMBER [RUN_NUMBER ...]]
                         [--data_tier DATA_TIER [DATA_TIER ...]]
                         [--data_stream DATA_STREAM [DATA_STREAM ...]]
                         [--timeout TIMEOUT]

Files to be uploaded are provided by the sources. Source can be:

- `dir`: a directory. All the files with name matching the pattern `hist.*_run([0-9]{4})_.*.root` are search for in the directory and subdirectories. Run number is extracted from the name of the file. The files are then in grouped according to their run number in datasets with name `run-XXXX-calib`.

- `tar`: a tar archive. The txt files contained in the input archive and with name matching the pattern `run_([0-9]{4})_filelist.dat` are read. Run number is extracted from the name of the txt file. The files are then in grouped according to their run number in datasets with name `run-XXXX-raw`.

- `sam`: *samweb* service. `run_number`, `data_tier`, `data_stream` are also required for this source. 

- `log`: a log file produced by *rucio_uploader.py*. 


