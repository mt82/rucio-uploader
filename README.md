# rucio-uploader

### Requirements
- [RUCIO client](https://rucio.readthedocs.io/en/latest/installing_clients.html)

### Usage
`usage: rucio_uploader.py [-h] --type {dir,tar} source [source ...]`

Files to be uploaded are provided by the sources. Source can be either a *directory* or *tar archive*.

In the directory, files with name matching the pattern `hist.*_run([0-9]{4})_.*.root` are search for. Run number is extracted from the name of the file. 
The files are then in grouped according to their run number in datasets with name `run-XXXX-calib`.

In tar archive, the txt files with name matching the pattern `run_([0-9]{4})_filelist.dat` are read.  Run number is extracted from the name of the txt file.
The files are then in grouped according to their run number in datasets with name `run-XXXX-raw`.
