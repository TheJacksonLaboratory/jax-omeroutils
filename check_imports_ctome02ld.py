import sys
import pathlib
import pandas as pd
import re
import os
from collections import Counter
import ezomero
from omero.sys import Parameters
from omero.rtypes import rstring
import argparse
from config import OMERO_HOST, OMERO_PORT, OMERO_USER, OMERO_PASS # for use on ctome02ld.jax.org

# Created for svc-import at /opt/miniforge
# conda create -n omeroutils python=3.9
# conda activate omeroutils
# conda install -c conda-forge zeroc-ice=3.6.5
# conda install -c bioconda bftools
# pip install pandas==2.2.3 numpy==1.26.4 openpyxl==3.1.2 omero-cli-transfer==1.0.1
# Above packages are current to importer env as of Jan 6, 2025

# sudo su svc-import
# conda activate omeroutils
# python /opt/jax-omeroutils-check/check_imports_ctome02ld.py --dir /nfs/dropbox/dropbox
# python /opt/jax-omeroutils-check/check_imports_ctome02ld.py --dir /nfs/dropbox/dropbox/<folder> -v

def find_md_file(import_directory):
    """Finds the xlsx file for importing OMERO metadata.
    Adapted from jax-omeroutils.intake without logging.

    This function will look at each file in the top level of the given
    ``import_directory``. Note that this function only looks for a file
    and will not check for the validity of the file (i.e., whether the type
    matches the extention) or whether the metadata itself satisfies all
    requirements.

    Parameters
    ----------
    import_directory : str or pathlike object
        The directory in which to look for a metadata file.

    Returns
    -------
    md_filepath : ``pathlib.Path`` object
       Path to the "valid-looking" spreadsheet file. If no appropriate file
       is found, returns `None`.

    Notes
    -----
    If multiple files are found, this function will log an error and return
    `None`.

    Example
    -------
    >>> fp = import_directory('/dropbox/djme_20200101')
    >>> print(f'Import metadata file found at: {fp}')
    Import metadata file found at: dropbox/djme_20200101/import_me.xlsx
    """
    import_directory = pathlib.Path(import_directory)
    md_files = []
    for f in import_directory.iterdir():
        if f.suffix.endswith('xlsx'):
            md_files.append(f)
    if len(md_files) == 0:
        raise ValueError('No spreadsheet exists')
    elif len(md_files) > 1:
        raise ValueError('More than one spreadsheet prevents import')
    else:
        md_filepath = md_files[0]
    return md_filepath

def load_md_from_file(md_filepath, sheet_name=0):
    """Load metadata from file into ``pandas.DataFrame`` object.
    Adapted from jax-omeroutils.intake without logging.

    Parameters
    ----------
    md_filepath : str, pathlike object, or None
        Path to file to attempt to load.
    sheet_name : str, int, list or None
        Passed to ``pd.read_excel``.

    Returns
    -------
    md : pandas.DataFrame
        Metadata for subsequent OMERO import steps.

    Notes
    -----
    This function will usually be taking its input from ``find_md_file``. Since
    that function will return `None` if no single metadata file is found, this
    function can take `None` as input, returning `None`.
    """
    if md_filepath is None:
        return None
    md_filepath = pathlib.Path(md_filepath)
    if not md_filepath.exists():
        raise FileNotFoundError(f'No such file: {md_filepath}')
    if md_filepath.suffix != '.xlsx':
        raise ValueError('File suffix must be xlsx')

    try:
        md_header = pd.read_excel(md_filepath,
                                  sheet_name=sheet_name,
                                  nrows=4,
                                  index_col=0,
                                  header=None,
                                  dtype=str,
                                  engine="openpyxl")
    except ValueError:
        raise ValueError(f"Worksheet {sheet_name} does not exist.")
    md = pd.read_excel(md_filepath,
                       sheet_name=sheet_name,
                       skiprows=range(4),
                       dtype=str,
                       engine="openpyxl")

    # fixing rogue lines, rogue columns, rogue leading/trailing spaces
    # in any string column
    md = md.map(lambda x: x.strip() if isinstance(x, str) else x)
    if 'project' in md.columns:
        md = md.dropna(subset=['filename', 'project', 'dataset'])\
             .dropna(axis='columns', how='all')
        if md.empty:
            raise ValueError('Spreadsheet needs filename, project and dataset')
    elif 'screen' in md.columns:
        md = md.dropna(subset=['filename', 'screen'])\
             .dropna(axis='columns', how='all')
        if md.empty:
            raise ValueError('Spreadsheet needs filename and screen')

    # protect against extra spaces on 'omero user' and 'omero group'
    md_header.index = md_header.index.str.strip()
    # protect against empty user and group
    md_header = md_header.fillna('')
    md_json = {}
    try:
        # added 'strip' to protect against leading/trailing spaces
        md_json['omero_user'] = md_header.loc['OMERO user:', 1].strip()
        md_json['omero_group'] = md_header.loc['OMERO group:', 1].strip()
        md_json['file_metadata'] = md.to_dict(orient='records')
    except KeyError:
        raise KeyError("User and group fields are non-existent or malformed.")
    return (md, md_json['omero_user'], md_json['omero_group'])

def check_log(log_path, verbose=True):
    """Count number of success and failure lines in a given log file"""
    success_filenames = []
    unknown_lines = []
    with open(log_path) as f:
        for line in f.readlines():
            line = line.strip()
            success_regex = re.search("Success moving file (.*) to the server. It will be imported.", line)
            missing_regex = re.search("Target does not exist: (.*) This file is in your spreadsheet but not in your folder, and will not be imported.", line)
            if success_regex:
                filename = pathlib.Path(success_regex.group(1)).name
                success_filenames.append(filename)
            elif missing_regex:
                pass # Skip for now
            elif line == "Cannot write import.json, no valid import targets. Skipping empty import.":
                pass # Summary line, don't care
            else:
                unknown_lines.append(line)
    if success_filenames and verbose:
        print("{} images from spreadsheet moved to hyperfile on {}".format(len(success_filenames), pathlib.Path(log_path).stem))
    return (success_filenames, unknown_lines)

def check_logs(import_directory, image_filenames):
    """loop through all logs in directory and call check_log"""
    images_dict = dict.fromkeys(image_filenames, 0)
    extra_images = []
    unknown_lines_all = []
    import_directory = pathlib.Path(import_directory)
    num_logs = 0
    for f in sorted(import_directory.iterdir()):
        if f.suffix.endswith('log'):
            num_logs += 1
            success_filenames, unknown_lines = check_log(f, image_filenames)
            for filename in success_filenames:
                if filename in images_dict:
                    images_dict[filename] = images_dict[filename] + 1
                else:
                    extra_images.append(filename)
            unknown_lines_all = unknown_lines_all + unknown_lines
    return num_logs, images_dict, extra_images, unknown_lines_all

def prettyprint_check_logs(import_directory, image_filenames, verbose=False):
    num_logs, images_dict, extra_images, unknown_lines_all = check_logs(import_directory, image_filenames)
    failed_image_count = 0
    if num_logs == 0:
        print("WAIT: There are no logs yet")
        return False
    for filename in image_filenames:
        if images_dict[filename] != 1:
            if verbose:
                print("BAD: {} was moved {} times".format(filename, images_dict[filename]))
            failed_image_count += 1
    unknown_lines_count = Counter(unknown_lines_all)
    for unknown_line in unknown_lines_count.keys():
        print("WARNING: \"{}\" received {} times".format(unknown_line, unknown_lines_count[unknown_line]))
    extra_image_extensions = set()
    for extra_image in extra_images:
        extra_split = extra_image.split(".")
        if len(extra_split) > 1:
            extra_image_extensions.add(extra_split[-1])
        else:
            extra_image_extensions.add("None")
    if extra_images:
        print("WARNING: {} extra images moved to hyperfile with extensions {}".format(len(extra_images),", ".join(extra_image_extensions)))
    if failed_image_count == 0:
        print("GOOD: All images from spreadsheet moved to hyperfile")
        return True
    else:
        print("BAD: {} images from spreadsheet were not moved to hyperfile".format(failed_image_count))
        return False

def get_image_names(conn, project_name, dataset_name):
    q = conn.getQueryService()
    params = Parameters()
    params.map = {"project": rstring(project_name),
                  "dataset": rstring(dataset_name)}
    results = q.projection(
            "SELECT i.name FROM Project p"
            " JOIN p.datasetLinks pdl"
            " JOIN pdl.child d"
            " JOIN d.imageLinks dil"
            " JOIN dil.child i"
            " WHERE p.name=:project"
            " AND d.name=:dataset",
            params,
            conn.SERVICE_OPTS
            )
    results_list = [r[0].val for r in results]
    return results_list

def check_omero(md_df, omero_group):
    all_zero = True
    image_counts = []
    conn = ezomero.connect(group=omero_group, host=OMERO_HOST, port=OMERO_PORT, user=OMERO_USER, password=OMERO_PASS, secure=True)
    if conn == None:
        print("BAD: OMERO login didn't work, likely incorrect group name")
        return([0]*md_df.shape[0])
    for index, row in md_df.iterrows():
        images_in_dataset = get_image_names(conn, row["project"], row["dataset"])
        if len(images_in_dataset) != 0: all_zero = False
        this_image_count = 0
        for imagename in images_in_dataset:
            if imagename.startswith(row["filename"]):
                this_image_count += 1
        image_counts.append(this_image_count)
    if all_zero == True:
        print("WARNING: all HQL queries returned no values - please check OMERO credentials and connect using an admin account")
    conn.close()
    return image_counts

def prettyprint_check_omero(md_df, omero_group, verbose=False):
    image_counts = check_omero(md_df, omero_group)
    unique_image_counts = sorted(set(image_counts))
    string_positive_counts = ", ".join([str(x) for x in unique_image_counts if x!=0])
    if 0 not in unique_image_counts:
        print("GOOD: All images found in OMERO, images per file: {}".format(string_positive_counts))
        return True
    else:
        num_fail = sum([x==0 for x in image_counts])
        print("BAD: {} images not found in OMERO".format(num_fail))
        if num_fail != len(image_counts):
            print("\t however, {} images found in OMERO, images per file: {}".format(len(image_counts)-num_fail,string_positive_counts))
        if verbose:
            for i in range(len(image_counts)):
                if not image_counts[i]:
                    print("BAD: {} not found in OMERO project {}, dataset {}".format(md_df["filename"][i], 
                                                                                    md_df["project"][i],
                                                                                    md_df["dataset"][i]))
        return False
    
def check_remaining_images(import_directory, verbose=False):
    import_directory = pathlib.Path(import_directory)
    remaining_images = []
    remaining_directories = []
    for remaining_path in import_directory.rglob("*"):
        if remaining_path.is_dir():
            remaining_directories.append(remaining_path)
        else:
            if not remaining_path.suffix.endswith(("log","xlsx","csv")) \
                and remaining_path.name != ".DS_Store" \
                and remaining_path.name != "metadata.json" \
                and remaining_path.name != "Thumbs.db":
                remaining_images.append(remaining_path.name)
    if remaining_directories:
        if verbose:
            print("WARNING: The following remaining directories were discovered:")
            for dirpath in remaining_directories:
                print("\t"+dirpath.name)
        else:
            print("WARNING: Discovered {} remaining directories".format(len(remaining_directories)))
    if remaining_images:
        if verbose:
            print("BAD: The following remaining images were discovered:")
            for imgname in remaining_images:
                print("\t"+imgname)
        else:
            print("BAD: Discovered {} remaining images".format(len(remaining_images)))
        return False
    return True

def check_directory(import_directory, verbose=False, print_md=False):
    """Loop through all files in folder, checking for log files, remaining image files, and Excel spreadsheet"""
    try:
        md_df, omero_user, omero_group = load_md_from_file(find_md_file(import_directory), sheet_name="Submission Form")
    except Exception as e:
        print(e)
        return None

    image_filenames = md_df["filename"].tolist()
    image_extensions = set()
    for filename in image_filenames:
        filename_split = filename.split(".")
        if len(filename_split) > 1:
            image_extensions.add(filename_split[-1])
        else:
            image_extensions.add("None")
    print("OMERO group: {}, OMERO user: {}".format(omero_group, omero_user))
    print("Found {} images with extensions {} in spreadsheet".format(len(image_filenames), ", ".join(image_extensions)))
    if print_md:
        column_names = [x for x in ["filename","dataset","project"] if x in md_df.columns]
        print(", ".join(column_names))
        for i in range(md_df.shape[0]):
            print(", ".join(md_df.iloc[i][column_names].tolist()))
    logs_good = prettyprint_check_logs(import_directory, image_filenames, verbose=verbose)
    omero_good = prettyprint_check_omero(md_df, omero_group, verbose=verbose)
    remaining_good = check_remaining_images(import_directory, verbose=verbose)
    return logs_good, omero_good, remaining_good

def check_all_directories(dropbox_path, verbose=False):
    """Loop through all import directories"""
    for dirname in sorted(os.listdir(dropbox_path)):
        dirpath = os.path.join(dropbox_path, dirname)
        if os.path.isdir(dirpath):
            print("Checking import directory "+dirname)
            check_directory(dirpath, verbose=verbose)
            print("")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check OMERO import directory logs')
    parser.add_argument('--dir', help='Directory path to check, if ends in "dropbox" will loop through subdirectories', required=True)
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    dirpath = pathlib.Path(args.dir)
    if not dirpath.exists():
        raise FileNotFoundError("Provided directory path to check does not exist. Make sure you map the volume first.")
    if dirpath.name == "dropbox":
        check_all_directories(args.dir, args.verbose)
    else:
        check_directory(args.dir, args.verbose, args.verbose)