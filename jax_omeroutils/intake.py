"""
This module is for processing directories containing OMERO-bound images and
the metadata (submission) form accompanying those images. Note that the module
assumes a submission form in which metadata is supplied on a file-by-file basis
and is not suitable for handling, e.g., HCS plate imports.
"""


import logging
import pathlib
import json
import pandas as pd
from datetime import datetime
from importlib import import_module
from ezomero import get_user_id
from omero.cli import CLI
ImportControl = import_module("omero.plugins.import").ImportControl

# This variable indicates the path in which new OMERO submissions will be
# staged for "in-place" imports
BASE_SERVER_PATH = pathlib.Path('/hyperfile/omero/autoimport/')


# Function definitions #
########################
def find_md_file(import_directory):
    """Finds the xlsx file for importing OMERO metadata.

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
    logger = logging.getLogger('intake')
    import_directory = pathlib.Path(import_directory)
    md_files = []
    for f in import_directory.iterdir():
        if f.suffix.endswith('xlsx'):
            md_files.append(f)
    if len(md_files) == 0:
        logger.error('No valid metadata file found - have you added an \
                     Excel spreadsheet to your files?')
        md_filepath = None
    elif len(md_files) > 1:
        logger.error('>1 metadata files found, can not process. Is your \
                     spreadsheet open? Please close it!')
        md_filepath = None
    else:
        md_filepath = md_files[0]
    return md_filepath


def load_md_from_file(md_filepath, sheet_name=0):
    """Load metadata from file into ``pandas.DataFrame`` object.

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
    logger = logging.getLogger('intake')
    if md_filepath is None:
        return None
    md_filepath = pathlib.Path(md_filepath)
    if not md_filepath.exists():
        logger.error(f'Cannot find file {md_filepath} - have you moved it?')
        raise FileNotFoundError(f'No such file: {md_filepath}')
    if md_filepath.suffix != '.xlsx':
        logger.error('Only spreadsheets with xlsx extensions are accepted. \
                      Please use our template for submission!')
        raise ValueError('File suffix must be xlsx')

    try:
        md_header = pd.read_excel(md_filepath,
                                  sheet_name=sheet_name,
                                  nrows=4,
                                  index_col=0,
                                  header=None,
                                  dtype=str,
                                  engine="openpyxl")
    except KeyError:
        logger.error("Your spreadsheet does not have a Submission Form sheet - \
                     please use our template for submission!")
        raise KeyError(f"Worksheet {sheet_name} does not exist.")
    md = pd.read_excel(md_filepath,
                       sheet_name=sheet_name,
                       skiprows=range(4),
                       dtype=str,
                       engine="openpyxl")

    # fixing rogue lines, rogue columns, rogue leading/trailing spaces
    # in any string column
    md = md.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    md = md.dropna(subset=['filename', 'project', 'dataset'])\
           .dropna(axis='columns', how='all')

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
        logger.error("Your spreadsheet does not have 'OMERO user:' or \
                     'OMERO group:' fields, or you have added your username/\
                     group on the same cell as those fields! Please add them \
                     on column B.")
        raise KeyError(f"User and group fields are non-existent or malformed.")
    return(md_json)

# Class definitions #
#####################


class ImportBatch:
    """Class for a batch of import files + metadata.

    ImportBatch in this case means a collection of individual image files.
    The assumption is that each file equals one row in the xlsx metadata form.
    This class should not be used for situations such as HCS plate imports, as
    the assumptions about how to handle the images and metadata will not hold.

    Parameters
    ----------
    import_path : str or pathlike object
        Path to the directory containing the images to be imported. Files will
        be written to this directory.
    conn : ``omero.gateway.BlitzGateway`` object
        Active OMERO connection. Needed for validation steps and to load
        user email address.

    Attributes
    ----------
    import_path : ``pathlib.Path`` object
        See description in Parameters section.
    user : str or None
        Shortname of OMERO user who will own the images.
    group : str or None
        Group that will own the images.
    user_email : str or None
        Email address for user, as pulled from OMERO.
    md : dict or None
        Loaded metadata, as output from ``load_md_from_file``.
    valid_md : boolean
        Flag to set if import form passes all checks for validity
        (`self.validate_import_md`)
    server_path : ``pathlib.Path`` object or None
        Path to directory where images will reside on OMERO server. Defaults to
        `None` on initialization and set with `self.set_target_path`.
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection to be used with this import batch. Necessary for
        validating user/group and grabbing email address from OMERO.
    import_target_list : list of ImportTarget objects
        Populated by ``self.load_targets``.
    """

    def __init__(self, conn, import_path):
        self.import_path = pathlib.Path(import_path)
        self.user = None  # shortname (for OMERO)
        self.group = None  # OMERO group
        self.user_email = None  # user email address pulled from OMERO
        self.md = None
        self.valid_md = False
        self.server_path = None  # where images will live on server
        self.conn = conn  # OMERO connection
        self.import_target_list = []  # List of ImportTarget objects
        self.logger = logging.getLogger('intake')

    def set_logging(self, log_directory, timestamp):
        logfile = pathlib.Path(self.import_path) / \
            pathlib.Path(f'{timestamp}.log')
        server_logfile = pathlib.Path(log_directory) / \
            pathlib.Path(f'{timestamp}.log')
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

    def load_md(self, sheet_name="Submission Form"):
        """Populate self.md

        If no metadata form is found, self.md will remain None. Nothing is
        returned.
        """
        self.md = load_md_from_file(find_md_file(self.import_path),
                                    sheet_name=sheet_name)

    def validate_user_group(self, user=None, group=None):
        """Validate omero user and group

        If user is a valid member of a valid group, ``self.user``,
        ``self.group``, and ``self.user_email`` will be set.

        Returns
        -------
        valid : boolean
            True if user is valid, group is valid, and user is a member of
            the group.
        """
        if user is None:
            user = self.md['omero_user']

        if group is None:
            group = self.md['omero_group']

        # Check group
        for g in self.conn.listGroups():
            if g.getName() == group:
                group_summary = g.groupSummary()
                self.group = group

                # Check user is a part of group
                userlist = group_summary[0]
                userlist.extend(group_summary[1])
                userlist = [u.getName() for u in userlist]
                if user not in userlist:
                    self.logger.error(f'User {user} is not in group {group} \
                                      and/or user {user} does not exist. \
                                      Please double-check the spelling and \
                                      note that usernames and group \
                                      names are case sensitive!')
                    raise ValueError('User non-existent or not in group.')
                else:
                    self.user = user
                    userid = get_user_id(self.conn, self.user)
                    user_obj = self.conn.getObject('Experimenter', userid)
                    self.user_email = user_obj._obj.email._val
                    return True
        self.logger.error(f'Group {group} was not found. Please double-check \
                          the spelling and note that usernames and group \
                          names are case sensitive!')
        raise ValueError('group not found.')

    def set_server_path(self):
        """Set ``self.server_path`` based on group, user, and import date.
        """
        group_directory = self.group.lower().replace(' ', '_')
        group_directory = pathlib.Path(group_directory)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        batch_directory = pathlib.Path(f'{self.user}_{timestamp}')

        self.server_path = BASE_SERVER_PATH / group_directory / batch_directory

    def validate_import_md(self):
        """Check whether the supplied metadata is valid.

        This function will do the following checks:
         - Do the columns 'filename', 'project', and 'dataset' exist?
         - Are there any fields in the above columns with missing data?
         - Are there any duplicate filenames in the metadata?

        This function will set `self.valid_md` depending on the outcome of
        these checks. Will return True or False, accordingly.

        Notes
        -----
        Each row of the metadata file is expected to point to one unique
        OMERO import target.
        """
        self.valid_md = True
        for filemd in self.md['file_metadata']:
            if 'filename' not in filemd.keys():
                self.logger.error('Column \'filename\' is missing in your \
                                  spreadsheet!')
                self.valid_md = False
                return False
            elif (str(filemd['filename']) == '' or
                  str(filemd['filename']) == 'nan'):
                self.logger.error('You have an empty filename in your \
                                  spreadsheet. Please double-check!')
                self.valid_md = False
                return False

            if 'dataset' not in filemd.keys():
                self.logger.error('Column \'dataset\' is missing in \
                                  your spreadsheet!')
                self.valid_md = False
                return False
            elif (str(filemd['dataset']) == '' or
                  str(filemd['dataset']) == 'nan'):
                self.logger.error('You have an empty dataset name in \
                                  your spreadsheet. Please double-check!')
                self.valid_md = False
                return False

            if 'project' not in filemd.keys():
                self.logger.error('Column \'project\' is missing in \
                                  your spreadsheet!')
                self.valid_md = False
                return False
            elif (str(filemd['project']) == '' or
                  str(filemd['project']) == 'nan'):
                self.logger.error('You have an empty project name in \
                                  your spreadsheet. Please double-check!')
                self.valid_md = False
                return False

        # Check for duplicate filenames in metadata
        file_list_md = [f['filename'] for f in self.md['file_metadata']]
        if len(set(file_list_md)) < len(file_list_md):
            self.logger.error('Spreadsheet contains duplicate filenames. \
                              Please double-check!')
            self.valid_md = False
            return False

        return True

    def load_targets(self):
        """Populate ``self.import_target_list`` with valid ImportTarget objects
        """
        for md_entry in self.md['file_metadata']:
            imp_target = ImportTarget(self.import_path, md_entry)
            if imp_target.exists:
                imp_target.validate_target()
            else:
                err = f'Target does not exist: {imp_target.path_to_target}. \
                      This file is in your spreadsheet but not in your \
                      folder, and will not be imported.'
                self.logger.error(err)
            if imp_target.valid_target is True:
                self.import_target_list.append(imp_target)
            elif imp_target.valid_target is False:
                err = ('Target can not be imported'
                       f' by OMERO: {imp_target.path_to_target}. File might be \
                       corrupted or invalid. Skipping.')
                self.logger.error(err)

    def write_json(self):
        """Write out metadata file for further processing
        """
        mandatory = [self.user, self.group, self.user_email,
                     self.md, self.server_path]
        if None in mandatory:
            self.logger.error("Cannot write import.json, missing or wrong \
                              information in one of the following items: \
                              username, group, metadata spreadsheet")
            return False
        elif self.valid_md is False:
            self.logger.error("Cannot write import.json, metadata \
                              spreadsheet contains invalid values.")
            return False
        elif len(self.import_target_list) == 0:
            self.logger.error("Cannot write import.json, no valid \
                              import targets. Skipping empty import.")
            return False
        else:
            import_json = {}
            import_json['user'] = self.user
            import_json['group'] = self.group
            import_json['user_email'] = self.user_email
            import_json['user_supplied_md'] = self.md
            import_json['server_path'] = str(self.server_path)
            import_json['import_path'] = str(self.import_path)
            import_json['import_targets'] = []
            for target in self.import_target_list:
                import_json['import_targets'].append(target.target_md)
            with open(self.import_path / 'import.json', 'w') as fp:
                json.dump(import_json, fp)
            return True


class ImportTarget:
    """Class to handle a file to be imported

    This should correspond to a row from the metadata form, as processed by
    ``load_md_from_file``.

    Parameters
    ----------
    import_path : pathlib.Path object
        Path to directory containing the file to be processed. Generally, this
        will come from ``ImportBatch.import_path``.
    md_entry : dict
        Metadata for a particular file, generally supplied by an import
        metadata form as processed by ``load_md_from_file``. In that case,
        a single ``md_entry`` corresponds to an item under
        ``md['file_metadata']``.

    Attributes
    ----------
    target_md : dict
        Populated from ``md_entry`` parameter.
    path_to_target : ``pathlib.Path`` object
        Path to the file represented by the ImportTarget
    exists : boolean
        True if the file at ``self.path_to_target`` exists.
    valid_target : boolean
        Set by ``self.validate_target``
    """

    def __init__(self, import_path, md_entry):
        self.target_md = md_entry
        self.path_to_target = import_path / self.target_md['filename']
        self.exists = self.path_to_target.exists()
        self.valid_target = None

    def validate_target(self):
        """Check whether an import target can be imported by OMERO

        This used the OMERO CLI, but since it is just checking whether
        the file can be interpreted by BioFormats, there is no connection
        required.
        """

        cli = CLI()
        cli.register('import', ImportControl, '_')
        cli.invoke(['import', '-f', str(self.path_to_target)])

        if cli.rv == 0:
            self.valid_target = True
        else:
            self.valid_target = False
