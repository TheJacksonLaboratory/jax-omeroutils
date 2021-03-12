#!/bin/bash -l


usage="$(basename "$0") [-h] [-e exclude ] target_folder [optional arguments] -- runs imports on all subfolders from the argument folder.
where:
    -h  show this help text
    -e  Passes a list of folders to be excluded from import

    Any optional arguments passed after the target folder are passed through to import_workflow.py. 
    This script will run whatever is in your \$IMPORT env variable over the immediate subfolders of your target folder.

"

##Default options in case user doesn't pass any arguments
exclude=""
folder=""
arguments=""

##Parse arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -e|--exclude) exclude="$2"; shift 2;;
    
    -h) echo "$usage"; exit 0;;
    -*) echo "unknown option: $1" >&2; exit 1;;
    **) break;;
  esac

done
folder="$1"; shift 1;
# need to change into a folder where sudo has permission - using folder where script is
cd "$(dirname "$0")"

arguments="$*"
for dir in $(find $folder -mindepth 1 -maxdepth 1 -type d -mmin +60); do 
    skip=false
    if [ "$exclude" ]; then
        for exc in $(cat $exclude); do
            if [ "$(realpath $exc)" == "$(realpath $dir)" ] ; then
                echo "Skipping" $dir " - directory in exclude file";
                skip=true
            fi
        done
    fi
    if [ "$skip" = false ]; then
        echo "Processing" $dir;
        $IMPORT $dir $arguments; 
    fi
    
done

