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
# check for folders last modified more than 60 mins ago
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

        # check whether email will need to be sent
        email=false
        # time cutoff is 60 mins + gap between cron runs (in our case, 360 mins)
        modified = $(find $folder/$dir -mindepth 1 -maxdepth 1 -type f -mmin +420 | wc -l)
        if [ $modified -gt 1 ]; then
            email=true
        fi

        
        echo "Processing" $dir;
        $IMPORT $dir $arguments; 

        #check whether folder is "empty" now
        empty=false
        allfiles = $(find $folder/$dir -mindepth 1 -maxdepth 1 -type f | wc -l)
        nonimages = $(find $folder/$dir -mindepth 1 -maxdepth 1 -regex ".*\.\(xlsx\|csv\|log\|json\|db\|txt\)" -type f | wc -l)
        if [ $allfiles -eq $nonimages ]; then
            empty=true
        fi
        logfile = $(find $folder/$dir -mindepth 1 -maxdepth 1 -regex ".*\.\(log\)" -type f -mmax +10

        # add message for empty folder
        if [ "$empty" = true ]; then
            echo -e "\n\nAll image files from your folder were imported and the folder will be deleted soon." >> $logfile
            echo "(that does NOT mean that all files in your SPREADSHEET were imported; it is your" >>$logfile
            echo -e "responsibility to check all files from your submission were present in the folder.)\n" >>$logfile
        fi

        # send email if necessary
        if [ "$email" = true ]; then
            #retrieve email of user
            $address = ???????

            # add subject/to/from for email
            sed -i "1s/^/\n/" $logfile
            sed -i "1iSubject: omero import log for folder $dir" $logfile
            sed -i "1iFrom: noreply-omero-importer@jax.org" $logfile
            sed -i "1iTo: $address" $logfile

            #send email
            ssmtp $address < $logfile

            # remove email-specific lines
            sed -i '1,4d' $logfile
        fi
    fi

    
done

