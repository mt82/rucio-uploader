#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "$(basename ${0}) <input folder> <output folder>"
  exit 1;
fi 

ifolder="${1}"
ofolder="${2}"

if [[ ! -d ${ifolder} ]]; then
  mkdir -p ${ifolder}
else
  rm -fr ${ifolder}/*
fi

if [[ ! -d ${ofolder} ]]; then
  mkdir -p ${ofolder}
else
  rm -fr ${ofolder}/*
fi

rlist=$(ls ./run_files/*.tgz)

for frun in ${rlist}; do
  # remove all files
  rm -fr ${ifolderi}

  # unpack
  tar -xzvf ${frun} --directory ${ifolder}
  
  # list files
  flist=$(ls ${ifolder}/*.dat)

  # loop ver files
  for fin in ${flist}; do
    # output file name
    fname=$(basename ${fin})
    fout=$(echo ${fname} | sed "s:.dat:.local.dat:g"); 

    # remove gsiftp stuff
    sed "s,gsiftp://fndca1.fnal.gov:2811/pnfs/fnal.gov/usr,/pnfs,g" $fin >> ${ofolder}/$fout;

    # construct a list of unique file name
    cat ${ofolder}/$fout | sort | uniq > tmp;

    # rename the list
    mv tmp ${ofolder}/$fout;
  done;
done;
