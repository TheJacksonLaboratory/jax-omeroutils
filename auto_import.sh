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
        echo "Processing" $dir;
        # check whether email will need to be sent
        email=false
        # time cutoff is 60 mins + gap between cron runs (in our case, 360 mins)
        modified=$(find $dir -mindepth 1 -maxdepth 1 -type f -cmin -420 | wc -l)
        echo "Folder has $modified modified files since last cron run."
        if [ $modified -gt 1 ]; then
            email=true
        fi

        # actually do imports
        
        $IMPORT $dir $arguments; 

        #check whether folder is "empty" now
        empty=false
        allfiles=$(find $dir -mindepth 1 -maxdepth 1 -type f | wc -l)
        nonimages=$(find $dir -mindepth 1 -maxdepth 1 -regex ".*\.\(xlsx\|csv\|log\|json\|db\|ini\|txt\)" -type f | wc -l)
        echo "Folder has $allfiles files left, of which $nonimages are typical non-images."
        if [ $allfiles -eq $nonimages ]; then
            empty=true
        fi
        logfile=$(find $dir -mindepth 1 -maxdepth 1 -regex ".*\.\(log\)" -type f -mmin -10)
        empty_msg=""
        # add message for empty folder
        if [ "$empty" = true ]; then
            echo "folder is now empty - adding message to email"
            empty_msg=$( echo -e "\n\nAll image files from your folder were imported and the folder will be deleted soon." && \
            echo "(that does NOT mean that all files in your SPREADSHEET were imported; it is your" && \
            echo -e "responsibility to check all files from your submission were present in the folder.)\n" )
        fi

        # send email if necessary
        if [ "$email" = true -a -f $logfile ]; then
            #retrieve email of user by splitting dir name on underscore
            address=$(echo ${dir##*/} | cut -f1 -d_)"@jax.org"
            echo "Sending email to user $address"
            email_dir=${dir##*/}
            #send email
            (
                                echo "To: $address" && \
                                echo "From: noreply-omero-importer@jax.org" && \
                                echo "Subject: omero import log for folder $email_dir" && \
                                echo "" && \
                                cat $logfile && \
                                echo "$empty_msg"
                                ) > $HOME/temp_email.txt
            ssmtp $address < $HOME/temp_email.txt

        fi
    fi

    
done

