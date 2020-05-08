import logging
import os
import pathlib
import pandas as pd
from functools import partial
from jax_omeroutils.filewriter import write_files_tsv, write_import_md_json
from jax_omeroutils.filewriter import write_import_yml

# metadata filetype handling #
##############################

MD_VALID_TYPES = {'xlsx': pd.read_excel,
                  'xls': pd.read_excel,
                  'tsv': partial(pd.read_csv, sep='\t')}


# Function definitions #
########################


def find_md_file(import_directory):
    """Finds a "valid-looking" spreadsheet file for importing OMERO metadata.

    This function will look at each file in the top level of the given
    ``import_directory``. It specifically looks for file extensions in
    ``MD_VALID_TYPES``, which tells subsequent steps of imports how to
    load the metadata file. Note that this function only looks for a file
    and will not check for the validity of the file (i.e., whether the type
    matches the extention) or whether the metadata itself satisfies all
    requirements.

    Parameters
    ----------
    import_directory : str
        The directory in which to look for a metadata file.

    Returns
    -------
    md_filepath : str
       Path to the "valid-looking" spreadsheet file. If no appropriate file
       is found, returns `None`.

    Notes
    -----
    If multiple files are found, this function will log an error and return
    `None`.

    Note that this function currently uses ``os.path`` for dealing with paths.
    We should eventually switch to all pathlib.

    Example
    -------
    >>> fp = import_directory('/dropbox/dropbox/djme_20200101')
    >>> print(f'Import metadata file found at: {fp}')
    /dropbox/dropbox/djme_20200101/import_me.xlsx
    """
    allowed_ftypes = tuple(MD_VALID_TYPES.keys())
    md_files = []
    for f in os.listdir(import_directory):
        if f.lower().endswith(allowed_ftypes):
            md_files.append(f)
    if len(md_files) == 0:
        logging.error('No valid metadata file found')
        md_filepath = None
    elif len(md_files) > 1:
        logging.error('>1 metadata files found, can not process')
        md_filepath = None
    else:
        md_filename = md_files[0]
        md_filepath = os.path.join(import_directory, md_filename)
    return md_filepath


def load_md_from_file(md_filepath):
    """Load metadata from file into ``pandas.DataFrame`` object.

    Parameters
    ----------
    md_filepath : str or None
        Path to file to attempt to load.

    Returns
    -------
    md : pandas.DataFrame
        Metadata for subsequent OMERO import steps.

    Notes
    -----
    This function will usually be taking its input from ``find_md_file``. Since
    that function will return `None` if no single metadata file is found, this
    function can take `None` as input, returning `None`.

    Examples
    --------
    >>> md = load_md_from_file(find_md_file('/dropbox/dropbox/djme_20200101'))
    >>> print(md.iloc[:, :3])
         filename      project      dataset
    0  image0.tif  testproject  testdataset
    1  image1.tif  testproject  testdataset
    2  image2.tif  testproject  testdataset
    3  image3.tif  testproject  testdataset
    4  image4.tif  testproject  testdataset
    """

    allowed_ftypes = MD_VALID_TYPES.keys()
    if md_filepath is None:
        return None
    md_filepath = pathlib.Path(md_filepath)
    ftype = md_filepath.suffix.strip('.')
    if ftype not in allowed_ftypes:
        raise ValueError(f'Metadata file type {ftype} is invalid')
    else:
        reader = MD_VALID_TYPES[ftype]
    return reader(md_filepath)


# Class definitions #
#####################


class ImportBatch:
    """Class for a batch of import files + metadata.

    Parameters
    ----------
    basepath : str or pathlike object
        Path to the directory containing the images to be imported. Files will
        be written to this directory. If supplying string, do not include
        trailing slash!
    user : str
        Shortname of OMERO user who will own the images.
    group : str
        Group name of OMERO user who will own the images.
    import_type : {'flat', 'plate'}, optional
        Method to use for handling metadata. See attribute details below.
        Defaults to 'flat'.

    Attributes
    ----------
    basepath : str
        See description in Parameters section.
    import_type : {'flat', 'plate'}
        Method to use for handling metadata.

        flat: Most imports will use the "flat" method, where the directory
        containing files for import has a flat structure, and each file has
        one row in the import metadata file.

        plate: [NOT CURRENTLY IMPLEMENTED] Imports of HCS data (plate scans)
        will have complex file structures that do not work with the "flat"
        approach. For example, each "well" may comprise multiple files. There
        may be a directory structure to the files. And different wells will
        obviously have different metadata, even though the whole plate would be
        one import "target" in the OMERO sense. We will need to write methods
        to handle these data in the future.
    user : str
        See description in Parameters section.
    group : str
        See description in Parameters section.
    md : pandas.DataFrame
        Imported metadata, as output from ``load_md_from_file``.
    valid_batch : boolean
        Flag to set if import batch passes all checks for validity
        (`self.validate_batch`)
    target_path : str or None
        Path to directory where images will reside on OMERO server. This path
        will be used to set up the bulk import files for OMERO and must be
        correct, or import will fail. Defaults to `None` on initialization and
        set with `self.set_target_path`.

    Notes
    -----
    Object initialization should fail if there is no valid metadata returned
    from ``load_md_from_file``.

    Need to refactor to fully utilize pathlib.

    Examples
    --------
    >>> batch = ImportBatch('/dropbox/dropbox/djme_202000101',
    ...                     'djme',
    ...                     'Research_IT',
    ...                     import_type='flat')
    >>> batch.validate_batch()
    True
    >>> batch.set_target('/hyperfile/omero/Research_IT/djme_20200101')
    """

    def __init__(self, basepath, user, group, import_type='flat'):
        self.basepath = str(basepath)
        self.import_type = import_type
        self.user = user  # shortname (for OMERO)
        self.group = group  # OMERO group
        self.md = load_md_from_file(find_md_file(basepath))
        self.valid_batch = False
        self.target_path = None

    def validate_batch(self):
        """Validates import batch based on `self.import_type`

        Returns
        -------
        valid : boolean
            The value assigned to `self.valid_batch` by the method.

        Notes
        -----
        Currently, only the "flat" import type is implemented, so this method
        simply wraps `self._validate_flat`
        """
        if self.md is None:
            logging.error('Cannot validate ImportBatch: no metadata found')
            return False

        if self.import_type == 'flat':
            self._validate_flat()
            return self.valid_batch
        else:
            return False

    def set_target_path(self, fp):
        """Sets `self.target_path`.

        This method does not return anything.

        Parameters
        ----------
        fp : str or pathlike object
           Path to directory where images will reside on OMERO server. This
           path will be used to set up the bulk import files for OMERO and
           must be correct, or import will fail.
        """
        self.target_path = str(fp)

    def write_files(self):
        """Writes import.yml, files.tsv, and import_md.json

        This function will only attempt to write files if `self.valid_batch`
        is True and `self.target_path` has been set.

        This function does not return anything, but will log the path to the
        files being written (level=INFO).
        """
        if self.valid_batch is not True:
            logging.error('Cannot write files: ImportBatch not validated')
            return

        if self.target_path is None:
            logging.error('target_path must be set to create import files')
            return

        files_tsv_fp = write_files_tsv(self.md, self.basepath,
                                       self.target_path)
        logging.info(f'Writing {files_tsv_fp}')
        import_yml_fp = write_import_yml(self.basepath, self.target_path)
        logging.info(f'Writing {import_yml_fp}')
        import_md_json_fp = write_import_md_json(self.md, self.basepath,
                                                 self.user, self.group)
        logging.info(f'Writing {import_md_json_fp}')
        print(f'Success! Files written to {self.target_path}')

    def _validate_flat(self):
        """Use the "flat" approach for validating the import batch.

        This function will do the following checks:
         - Do the columns 'filename', 'project', and 'dataset' exist?
         - Are there any fields with missing data?
         - Are there files that don't have metadata entries?
         - Are there metadata entries that don't have files?
         - Are there any duplicate filenames in the metadata?

        This function will set `self.valid_batch` depending on the outcome of
        these checks.

        Notes
        -----
        Each row of the metadata file is expected to point to one unique
        image file.
        """
        self.valid_batch = True

        # Check for necessary columns
        if 'filename' not in self.md.columns:
            logging.error('Metadata file missing filename column')
            self.valid_batch = False
            return
        if 'dataset' not in self.md.columns:
            logging.error('Metadata file missing dataset column')
            self.valid_batch = False
        if 'project' not in self.md.columns:
            logging.error('Metadata file missing project column')
            self.valid_batch = False

        # Check for missing fields
        if self.md.isnull().values.any():
            logging.error('Spreadsheet missing values')
            self.valid_batch = False

        # Check for file mismatching
        file_list_dir = os.listdir(self.basepath)
        file_list_md = self.md.filename.values
        for f in file_list_dir:
            if f.endswith(tuple(MD_VALID_TYPES.keys())):
                file_list_dir.remove(f)

        uniq_files_dir = set(file_list_dir) - set(file_list_md)
        uniq_files_md = set(file_list_md) - set(file_list_dir)
        if len(uniq_files_dir) > 0:
            logging.error('Some files unaccounted for:'
                          f'{list(uniq_files_dir)}')
            self.valid_batch = False
        if len(uniq_files_md) > 0:
            logging.error('Some metadata entries missing files:'
                          f'{list(uniq_files_md)}')
            self.valid_batch = False

        # Check for duplicate filenames in metadata
        if len(set(file_list_md)) < len(file_list_md):
            logging.error('Duplicate filenames in metadata')
            self.valid_batch = False
