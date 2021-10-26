"""
This module is for managing OMERO imports, making use of the OMERO CLI,
which can be called from a Python script. Note that this code requires
a properly structured import.json file, which is produced during data
intake (using the intake.py module).
"""


import logging
from ezomero import post_dataset, post_project
from ezomero import get_image_ids, link_images_to_dataset
from ezomero import post_screen, link_plates_to_screen
from importlib import import_module
from omero.cli import CLI
from omero.plugins.sessions import SessionsControl
from omero.rtypes import rstring
from omero.sys import Parameters
from omero.gateway import MapAnnotationWrapper
from pathlib import Path
ImportControl = import_module("omero.plugins.import").ImportControl

# Constants
CURRENT_MD_NS = 'jax.org/omeroutils/user_submitted/v0'


# Functions
def set_or_create_project(conn, project_name):
    """Create a new Project unless one already exists with that name.

    Parameter
    ---------
    conn : ``omero.gateway.BlitzGateway`` object.
        OMERO connection.
    project_name : str
        The name of the Project needed. If there is no Project with a matching
        name in the group specified in ``conn``, a new Project will be created.

    Returns
    -------
    project_id : int
        The id of the Project that was either found or created.
    """
    ps = conn.getObjects('Project', attributes={'name': project_name})
    ps = list(ps)
    if len(ps) == 0:
        project_id = post_project(conn, project_name)
        print(f'Created new Project:{project_id}')
    else:
        project_id = ps[0].getId()
    return project_id


def set_or_create_dataset(conn, project_id, dataset_name):
    """Create a new Dataset unless one already exists with that name/Project.

    Parameter
    ---------
    conn : ``omero.gateway.BlitzGateway`` object.
        OMERO connection.
    project_id : int
        Id of Project in which to find/create Dataset.
    dataset_name : str
        The name of the Dataset needed. If there is no Dataset with a matching
        name in the group specified in ``conn``, in the Project specified with
        ``project_id``, a new Dataset will be created accordingly.

    Returns
    -------
    dataset_id : int
        The id of the Dataset that was either found or created.
    """
    ds = conn.getObjects('Dataset',
                         attributes={'name': dataset_name},
                         opts={'project': project_id})
    ds = list(ds)
    if len(ds) == 0:
        dataset_id = post_dataset(conn, dataset_name, project_id=project_id)
        print(f'Created new Dataset:{dataset_id}')
    else:
        dataset_id = ds[0].getId()
    return dataset_id


def set_or_create_screen(conn, screen_name):
    """Create a new Screen unless one already exists with that name.

    Parameter
    ---------
    conn : ``omero.gateway.BlitzGateway`` object.
        OMERO connection.
    screen_name : str
        The name of the Screen needed. If there is no Screen with a matching
        name in the group specified in ``conn``, a new Screen will be created.

    Returns
    -------
    screen_id : int
        The id of the Project that was either found or created.
    """
    ss = conn.getObjects('Screen', attributes={'name': screen_name})
    ss = list(ss)
    if len(ss) == 0:
        screen_id = post_screen(conn, screen_name)
        print(f'Created new Screen:{screen_id}')
    else:
        screen_id = ss[0].getId()
    return screen_id


def multi_post_map_annotation(conn, object_type, object_ids, kv_dict, ns):
    """Create a single new MapAnnotation and link to multiple images.
    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    object_type : str
       OMERO object type, passed to ``BlitzGateway.getObjects``
    object_ids : int or list of ints
        IDs of objects to which the new MapAnnotation will be linked.
    kv_dict : dict
        key-value pairs that will be included in the MapAnnotation
    ns : str
        Namespace for the MapAnnotation
    Notes
    -----
    All keys and values are converted to strings before saving in OMERO.
    Returns
    -------
    map_ann_id : int
        IDs of newly created MapAnnotation
    Examples
    --------
    >>> ns = 'jax.org/jax/example/namespace'
    >>> d = {'species': 'human',
             'occupation': 'time traveler'
             'first name': 'Kyle',
             'surname': 'Reese'}
    >>> multi_post_map_annotation(conn, "Image", [23,56,78], d, ns)
    234
    """
    if type(object_ids) not in [list, int]:
        raise TypeError('object_ids must be list or integer')
    if type(object_ids) is not list:
        object_ids = [object_ids]

    if len(object_ids) == 0:
        raise ValueError('object_ids must contain one or more items')

    if type(kv_dict) is not dict:
        raise TypeError('kv_dict must be of type `dict`')

    kv_pairs = []
    for k, v in kv_dict.items():
        k = str(k)
        v = str(v)
        kv_pairs.append([k, v])

    map_ann = MapAnnotationWrapper(conn)
    map_ann.setNs(str(ns))
    map_ann.setValue(kv_pairs)
    map_ann.save()
    for o in conn.getObjects(object_type, object_ids):
        o.linkAnnotation(map_ann)
    return map_ann.getId()


# Class definitions
class Importer:
    """Class for managing OMERO imports using OMERO CLI.

    Metadata from ``import.json`` (item in 'import_targets') is required for
    assigning to Project/Dataset and adding MapAnnotations.

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object.
        OMERO connection.
    file_path : pathlike object
        Path to the file to imported into OMERO.
    import_md : dict
        Contains metadata required for import and annotation. Generally, at
        item from ``import.json`` ('import_targets').

    Attributes
    ----------
    conn : ``omero.gateway.BlitzGateway`` object.
        From parameter given at initialization.
    file_path : ``pathlib.Path`` object
        From parameter given at initialization.
    md : dict
        From ``import_md`` parameter given at initialization.
    session_uuid : str
        UUID for OMERO session represented by ``self.conn``. Supplied to
        OMERO CLI for connection purposes.
    filename : str
        Filename of file to be imported. Populated from ``self.md``.
    project : str
        Name of Project to contain the image. Populated from ``self.md``.
    dataset : str
        Name of Dataset to contain the image. Poplulated from ``self.md``.
    imported : boolean
        Flag indicating import status.
    image_ids : list of ints
        The Ids of the images in OMERO. Populated after a file is imported.
        This list may contain one or more images derived from a single file.
    """

    def __init__(self, conn, file_path, import_md):
        self.conn = conn
        self.file_path = Path(file_path)
        self.md = import_md
        self.session_uuid = conn.getSession().getUuid().val
        self.filename = self.md.pop('filename')
        if 'project' in self.md.keys():
            self.project = self.md.pop('project')
        else:
            self.project = None
        if 'dataset' in self.md.keys():
            self.dataset = self.md.pop('dataset')
        else:
            self.dataset = None
        if 'screen' in self.md.keys():
            self.screen = self.md.pop('screen')
        else:
            self.screen = None
        self.imported = False
        self.image_ids = None
        self.plate_ids = None

    def get_image_ids(self):
        """Get the Ids of imported images.

        Note that this will not find images if they have not been imported.
        Also, while image_ids are returned, this method also sets
        ``self.image_ids``.

        Returns
        -------
        image_ids : list of ints
            Ids of images imported from the specified client path, which
            itself is derived from ``self.file_path`` and ``self.filename``.

        """
        if self.imported is not True:
            logging.error(f'File {self.file_path} has not been imported')
            return None
        else:
            q = self.conn.getQueryService()
            params = Parameters()
            path_query = str(self.file_path).strip('/')
            params.map = {"cpath": rstring(path_query)}
            results = q.projection(
                "SELECT i.id FROM Image i"
                " JOIN i.fileset fs"
                " JOIN fs.usedFiles u"
                " WHERE u.clientPath=:cpath",
                params,
                self.conn.SERVICE_OPTS
                )
            self.image_ids = [r[0].val for r in results]
            return self.image_ids
    
    def get_plate_ids(self):
        """Get the Ids of imported plates.

        Note that this will not find plates if they have not been imported.
        Also, while plate_ids are returned, this method also sets
        ``self.plate_ids``.

        Returns
        -------
        plate_ids : list of ints
            Ids of plates imported from the specified client path, which
            itself is derived from ``self.file_path`` and ``self.filename``.

        """
        if self.imported is not True:
            logging.error(f'File {self.file_path} has not been imported')
            return None
        else:
            q = self.conn.getQueryService()
            params = Parameters()
            path_query = str(self.file_path).strip('/')
            params.map = {"cpath": rstring(path_query)}
            results = q.projection(
                "SELECT DISTINCT p.id FROM Plate p"
                " JOIN p.plateAcquisitions pa"
                " JOIN pa.wellSample ws"
                " JOIN ws.image i"
                " JOIN i.fileset fs"
                " JOIN fs.usedFiles u"
                " WHERE u.clientPath=:cpath",
                params,
                self.conn.SERVICE_OPTS
                )
            self.plate_ids = [r[0].val for r in results]
            return self.plate_ids

    def annotate_images(self):
        """Post map annotation (``self.md``) to images ``self.image_ids``.

        Returns
        -------
        map_ann_id : int
            The Id of the MapAnnotation that was created.
        """
        if len(self.image_ids) == 0:
            logging.error('No image ids to annotate')
            return None
        else:
            map_ann_id = multi_post_map_annotation(self.conn, "Image",
                                                   self.image_ids, self.md,
                                                   CURRENT_MD_NS)
            return map_ann_id

    def annotate_plates(self):
        """Post map annotation (``self.md``) to plates ``self.plate_ids``.

        Returns
        -------
        map_ann_id : int
            The Id of the MapAnnotation that was created.
        """
        if len(self.plate_ids) == 0:
            logging.error('No plate ids to annotate')
            return None
        else:
            map_ann_id = multi_post_map_annotation(self.conn, "Plate",
                                                   self.plate_ids, self.md,
                                                   CURRENT_MD_NS)
            return map_ann_id

    def organize_images(self):
        """Move images to ``self.project``/``self.dataset``.

        Returns
        -------
        image_moved : boolean
            True if images were found and moved, else False.
        """
        if len(self.image_ids) == 0:
            logging.error('No image ids to organize')
            return False
        orphans = get_image_ids(self.conn)
        for im_id in self.image_ids:
            if im_id not in orphans:
                logging.error(f'Image:{im_id} not an orphan')
            else:
                project_id = set_or_create_project(self.conn, self.project)
                dataset_id = set_or_create_dataset(self.conn,
                                                   project_id,
                                                   self.dataset)
                link_images_to_dataset(self.conn, [im_id], dataset_id)
                print(f'Moved Image:{im_id} to Dataset:{dataset_id}')
        return True

    def organize_plates(self):
        """Move plates to ``self.screen``.

        Returns
        -------
        plate_moved : boolean
            True if plates were found and moved, else False.
        """
        if len(self.plate_ids) == 0:
            logging.error('No plate ids to organize')
            return False
        for pl_id in self.plate_ids:
            screen_id = set_or_create_screen(self.conn, self.screen)
            link_plates_to_screen(self.conn, [pl_id], screen_id)
            print(f'Moved Plate:{pl_id} to Screen:{screen_id}')
        return True

    def import_ln_s(self, host, port):
        """Import file using the ``--transfer=ln_s`` option.

        Parameters
        ----------
        host : str
            Hostname of OMERO server in which images will be imported.
        port : int
            Port used to connect to OMERO.server.

        Returns
        -------
        import_status : boolean
            True if OMERO import returns a 0 exit status, else False.
        """
        cli = CLI()
        cli.register('import', ImportControl, '_')
        cli.register('sessions', SessionsControl, '_')

        cli.invoke(['import',
                    '-k', self.conn.getSession().getUuid().val,
                    '-s', host,
                    '-p', str(port),
                    '--transfer', 'ln_s',
                    str(self.file_path)])

        if cli.rv == 0:
            self.imported = True
            print(f'Imported {self.file_path}')
            return True
        else:
            logging.error(f'Import of {self.file_path} has failed!')
            return False
