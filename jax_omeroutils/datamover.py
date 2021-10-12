"""
This module is for "safely" moving OMERO import data from the submission folder
to its staging location on the server for "in-place" imports. Safety comes from
md5 checks on source and destination copy.
"""


import hashlib
import json
import logging
import shutil
import os
from pathlib import Path

DIR_PERM = 0o755
FILE_PERM = 0o644


def calculate_md5(file_path):
    """Return an md5 digest of a file.
    """
    hasher = hashlib.md5()
    BLOCKSIZE = 65536
    with open(file_path, "rb") as f:
        buf = f.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BLOCKSIZE)
    return hasher.hexdigest()


def file_mover(file_path, destination_dir, tries=3):
    """Safely move a file to a destination directory. Will retry if initial
    attempts result in mismatching md5 digests.
    """
    logger = logging.getLogger('intake')
    ersatz_file = destination_dir / 'test.tiff'
    if file_path.exists():
        for i in range(tries):
            os.makedirs(os.path.dirname(ersatz_file), mode=DIR_PERM, exist_ok=True)
            if destination_dir.exists():
                shutil.copy(file_path, destination_dir)
                dest_file = destination_dir / file_path.name
                if calculate_md5(file_path) == calculate_md5(dest_file):
                    os.remove(file_path)
                    return str(dest_file)
                else:
                    fp = str(file_path)
                    err = f"checksum failed after copy attempt {i + 1} for {fp}"
                    logger.error(err)
                    os.remove(dest_file)
    logger.error(f"Unable to copy {str(file_path)}")
    return None


class DataMover:
    """Class for moving files based on import.json

    If functions/classes from ``intake.py`` are used to manage an import,
    this class can be used to move data based on information in the resulting
    ``import.json``.

    Parameter
    ---------
    import_json_path : pathlike object
        Path to ``import.json``.

    Attributes
    ----------
    import_path : ``pathlib.Path`` object
        Path to the directory containing the data to be moved. Populated from
        ``import.json``.
    server_path : ``pathlib.Path`` object
        Path to the destination directory. Populated from ``import.json``.
    import_targets : list of dicts
        Contains metadata (filename, critically) for each file to be imported
        into OMERO and hence moved.
    """

    def __init__(self, import_json_path, fileset_list_path):
        self.logger = logging.getLogger('datamover')
        self.import_json_path = Path(import_json_path)
        self.fileset_list_path = Path(fileset_list_path)

        if not self.import_json_path.exists():
            raise FileNotFoundError('import.json not found')
        else:
            with open(import_json_path, 'r') as f:
                self.import_json = json.load(f)
        if not self.fileset_list_path.exists():
            raise FileNotFoundError('list of files not found')
        else:
            with open(fileset_list_path, 'r') as f_list:
                self.fileset_list = f_list.readlines()
        self.import_path = Path(self.import_json['import_path'])
        self.server_path = Path(self.import_json['server_path'])
        self.import_targets = self.import_json['import_targets']

    def move_data(self):
        # Prepare destination
        self.server_path.mkdir(mode=DIR_PERM, parents=True, exist_ok=True)

        # Move import targets first
        for target in self.import_targets:
            src_fp = self.import_path / target['filename']
            subfolder = target['filename'].rsplit('/',1)
            if len(subfolder) > 1:
                subfolder_path = self.server_path / subfolder[0]
            else:
                subfolder_path = self.server_path
            file = str(target['filename'])
            result = file_mover(src_fp, subfolder_path)
            if result is not None:
                print(f'Main file moved to {result}')
                self.logger.debug(f'Success moving file {file} to '+
                                  f'the server. It will be imported.')
                os.chmod(result, FILE_PERM)

        for target in self.fileset_list:
            src_fp = target.strip()
            subfolder_file = src_fp.split(str(self.import_path))[-1]
            src_fp = Path(src_fp)
            if src_fp.suffix == '.log' or src_fp.suffix == '.xlsx':
                continue
            subfolder = subfolder_file.rsplit('/',1)
            if len(subfolder) > 1:
                subfolder_path = self.server_path / subfolder[0].lstrip('/')
            else:
                subfolder_path = self.server_path
            #need to get the file subfolder structure here and
            #append to server_path
            result = file_mover(src_fp, subfolder_path)
            if result is not None:
                print(f'Auxiliary file moved to {result}')
                os.chmod(result, FILE_PERM)

        # Move import.json
        result = file_mover(self.import_json_path, self.server_path)
        if result:
            os.chmod(result, FILE_PERM)
        else: 
            for line in self.fileset_list:
                line.rstrip()
                print(line, "is it import.json?", line.endswith('import.json'))
                if line.endswith('import.json'):
                    result = line
        return f'Ready for import at:{result}'

    def set_logging(self, log_directory, timestamp):
        logfile = Path(self.import_path) / Path(f'{timestamp}.log')
        server_logfile = Path(log_directory) / Path(f'{timestamp}.log')
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        sh = logging.FileHandler(server_logfile)
        sh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        self.logger.addHandler(fh)
        self.logger.addHandler(sh)
        self.logger.addHandler(ch)
