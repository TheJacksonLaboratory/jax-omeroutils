import pandas as pd
import csv
import json
import pathlib
from datetime import datetime


def write_files_tsv(md, import_path, target_path, import_type='flat'):
    """Write files.tsv for OMERO bulk import.

    Parameters
    ----------
    md : pandas.DataFrame
        Metadata provided with images for OMERO import. At a
        minimum, this data frame must contain the following columns - filename,
        project, dataset.
    import_path : str or pathlike object
        Path to the directory containing the images to be imported. Files will
        be written to this directory. Do not include trailing slash!
    target_path : str or pathlike object
        Path to directory where images will reside on OMERO server. This path
        will be used to set up the bulk import files for OMERO and must be
        correct, or import will fail. Do not include trailing slash!
    import_type : {'flat', 'plate'}, optional
        Type of image import. See ``omeroutils.intake.ImportBatch`` for more
        details. Only `flat` is currently supported.

    Returns
    -------
    files_tsv_path : ``pathlib.Path`` object
        Path to files.tsv, written by the function.

    Notes
    -----
    This function should be refactored to use pathlib.

    Example
    -------
    >>> fp = write_files_tsv(dataframe,
    ...                      '/dropbox/dropbox/djme_20200101',
    ...                      '/hyperfile/omero/Research_IT/djme_20200101',
    ...                      import_type='flat')
    >>> print(f'New file written to: {fp}')
    New file written to: /dropbox/dropbox/djme_20200101/files.tsv
    """

    target_path = pathlib.Path(target_path)
    import_path = pathlib.Path(import_path)

    if import_type == 'flat':
        md = md  # If we implement other import types (e.g., plates), filter md
    else:
        raise ValueError('Currently only \'flat\' import types implemented')

    target_project = "Project:name:" + md.project.astype(str) + "/"
    target_dataset = "Dataset:name:" + md.dataset.astype(str)
    tsv_target = target_project + target_dataset
    filepath = str(target_path) + "/" + md.filename
    df = pd.DataFrame({"target": tsv_target, "path": filepath})
    files_tsv = df.to_csv(sep='\t', header=False,
                          index=False, quoting=csv.QUOTE_NONE)
    files_tsv_path = import_path / 'files.tsv'
    with open(files_tsv_path, 'w') as f:
        f.write(files_tsv)
    return files_tsv_path


def write_import_yml(import_path, target_path):
    """Write import.yml for OMERO bulk import.

    Parameters
    ----------
    import_path : str or pathlike object
        Path to the directory containing the images to be imported. Files will
        be written to this directory. Do not include trailing slash!
    target_path : str
        Path to directory where images will reside on OMERO server. This path
        will be used to set up the bulk import files for OMERO and must be
        correct, or import will fail. Do not include trailing slash!

    Returns
    -------
    import_yml_path : ``pathlib.Path`` object
        Path to import.yml, written by the function.

    Notes
    -----
    This function should be refactored to use pathlib.

    Example
    -------
    >>> fp = write_import_yml('/dropbox/dropbox/djme_20200101',
    ...                       '/hyperfile/omero/Research_IT/djme_20200101')
    >>> print(f'New file written to: {fp}')
    New file written to: /dropbox/dropbox/djme_20200101/import.yml
    """
    import_path = pathlib.Path(import_path)
    import_yml_path = import_path / 'import.yml'
    with open(import_yml_path, 'a') as f:
        f.write('---\n')
        f.write(f'path: "{target_path}/files.tsv"\n')
        f.write('include: /hyperfile/omero/import_base.yml\n')
        f.write('columns:\n')
        f.write(' - target\n')
        f.write(' - path\n')
    return import_yml_path


def write_import_md_json(md, import_path, user, group):
    """Write import_md.json for OMERO bulk import.

    Parameters
    ----------
    md : pandas.DataFrame
        Metadata provided with images for OMERO import. At a
        minimum, this data frame must contain the following columns - filename,
        project, dataset.
    import_path : str or pathlike object
        Path to the directory containing the images to be imported. Files will
        be written to this directory. Do not include trailing slash!
    user : str
        Shortname of OMERO user who will own the images.
    group : str
        Group name of OMERO user who will own the images.

    Returns
    -------
    import_md_path : str
        Path to import_md.json, written by the function.

    Example
    -------
    >>> fp = write_import_md_json(dataframe,
    ...                           '/dropbox/dropbox/djme_20200101',
    ...                           user,
    ...                           group)
    >>> print(f'New file written to: {fp}')
    New file written to: /dropbox/dropbox/djme_20200101/import_md.json
    """
    import_path = pathlib.Path(import_path)
    import_md_path = import_path / 'import_md.json'
    md_json = json.loads(md.to_json(orient='table'))
    for x in md_json['data']:
        _ = x.pop('index', None)  # Look for pandas option to avoid this
        _ = x.pop('OMERO_group', None)  # No longer using this field
    md_json.pop('schema')  # pandas junk
    md_json['OMERO_group'] = group
    md_json['md_process_date'] = str(datetime.now())
    md_json['import_batch_path'] = str(import_path)
    md_json['user_shortname'] = user
    with open(import_md_path, 'w') as mdfile:
        json.dump(md_json, mdfile)
    return import_md_path
