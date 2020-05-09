from omero.gateway import MapAnnotationWrapper
from omero.model import MapAnnotationI


# posts
def post_map_annotation(conn, object_type, object_ids, kv_dict, ns):
    """Create new MapAnnotation and link to images

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
    >>> post_map_annotation(conn, "Image", [23,56,78], d, ns)
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


# gets
def get_image_ids(conn, project=None, dataset=None):
    """return a list of image ids based on project and dataset

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    project : str or int, optional
        Name or ID of Project in which to return image_ids.
    dataset : str or int, optional
        Name or ID of Dataset in which to return image_ids.

    Returns
    -------
    im_ids : list of ints
        List of image IDs contained in the given Project/Dataset.

    Notes
    -----
    User and group information comes from the `conn` object. Be sure to use
    ``conn.SERVICE_OPTS.setOmeroGroup`` to specify group prior to passing
    the `conn` object to this function.

    If no Project or Dataset is given, orphaned images are returned.

    If only Project is given, images will be returned that belong to all
    Datasets for that Project.

    If Project/Dataset names are given, note that multiple different
    Projects/Datasets with the same name can exist in a given group. In this
    case, all image IDs belonging to all matching Projects/Datasets will be
    returned.

    Examples
    --------
    Return orphaned images:
    >>> orphans = get_image_ids(conn)

    Return IDs of all images from Project named "Good stuff":
    >>> good_stuff_ims = get_image_ids(conn, project="Good stuff")

    Return IDs of all images from Dataset names "Bonus" in Project with ID 448:
    >>> bonus_ims = get_image_ids(conn, project=448, dataset="Bonus")
    """

    if project is not None:
        if type(project) not in [str, int]:
            raise TypeError('project must be integer or string')

        if type(project) is str:
            ps = conn.getObjects('project', attributes={'name': project})
            project = [p.getId() for p in ps]

    if dataset is not None:
        if type(dataset) not in [str, int]:
            raise TypeError('dataset must be integer or string')

        if type(dataset) is str:
            ds = conn.getObjects('Dataset', attributes={'name': dataset})
            dataset = [d.getId() for d in ds]

    if type(project) is not list:
        project = [project]
    if type(dataset) is not list:
        dataset = [dataset]

    im_ids = []
    if (project is None) and (dataset is None):
        for im in conn.listOrphans('Image'):
            im_ids.append(im.getId())
    else:
        for d in dataset:
            for p in project:
                opts = {'project': p, 'dataset': d}
                if opts['project'] is None:
                    opts.pop('project')
                if opts['dataset'] is None:
                    opts.pop('dataset')
                ims = conn.getObjects('Image', opts=opts)
                im_ids.extend([im.getId() for im in ims])
    return im_ids


def get_map_annotation_ids(conn, object_type, object_id, ns=None):
    """Get IDs of map annotations associated with an object

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    object_type : str
        OMERO object type, passed to ``BlitzGateway.getObject``
    object_id : int
        ID of object of ``object_type``.
    ns : str
        Namespace with which to filter results

    Returns
    -------
    map_ann_ids : list of ints

    Examples
    --------
    Return IDs of all map annotations belonging to an image:
    >>> map_ann_ids = get_map_annotation_ids(conn, 'Image', 42)

    Return IDs of map annotations with namespace "test" linked to a Dataset:
    >>> map_ann_ids = get_map_annotation_ids(conn, 'Datset', 16, ns='test')
    """

    target_object = conn.getObject(object_type, object_id)
    map_ann_ids = []
    for ann in target_object.listAnnotations(ns):
        if ann.OMERO_TYPE is MapAnnotationI:
            map_ann_ids.append(ann.getId())
    return map_ann_ids


def get_map_annotation(conn, map_ann_id):
    """Get the value of a map annotation object

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    map_ann_id : int
        ID of map annotation to get.

    Returns
    -------
    kv_dict : dict
        The value of the specified map annotation object, as a Python dict.

    Example
    -------
    >>> ma_dict = get_map_annotation(conn, 62)
    >>> print(ma_dict)
    {'testkey': 'testvalue', 'testkey2': 'testvalue2'}
    """
    kv_pairs = conn.getObject('MapAnnotation', map_ann_id).getValue()
    kv_dict = {}
    for k, v in kv_pairs:
        kv_dict[k] = v
    return kv_dict


def get_group_id(conn, group_name):
    """Get ID of a group based on group name.

    Must be an exact match. Case sensitive.

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    group_name : str
        Name of the group for which an ID is to be returned.

    Returns
    -------
    group_id : int
        ID of the OMERO group. Returns `None` if group cannot be found.

    Examples
    --------
    >>> get_group_id(conn, "Research IT")
    304
    """
    if type(group_name) is not str:
        raise TypeError('OMERO group name must be a string')

    for g in conn.listGroups():
        if g.getName() == group_name:
            return g.getId()
    return None


# puts
def put_map_annotation(conn, map_ann_id, kv_dict, ns=None):
    """Update an existing map annotation with new values (kv pairs)

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    map_ann_id : int
        ID of map annotation whose values (kv pairs) will be replaced.
    kv_dict : dict
        New values (kv pairs) for the MapAnnotation.
    ns : str
        New namespace for the MapAnnotation. If left as None, the old
        namespace will be used.

    Returns
    -------
    Returns None.

    Examples
    --------
    Change only the values of an existing map annotation:
    >>> new_values = {'testkey': 'testvalue', 'testkey2': 'testvalue2'}
    >>> put_map_annotation(conn, 15, new_values)

    Change both the values and namespace of an existing map annotation:
    >>> put_map_annotation(conn, 16, new_values, 'test_v2')
    """
    map_ann = conn.getObject('MapAnnotation', map_ann_id)

    if ns is None:
        ns = map_ann.getNs()
    map_ann.setNs(ns)

    kv_pairs = []
    for k, v in kv_dict.items():
        k = str(k)
        v = str(v)
        kv_pairs.append([k, v])
    map_ann.setValue(kv_pairs)
    map_ann.save()
    return None


# filters
def image_has_imported_filename(conn, im_id, imported_filename):
    """Ask whether an image is associated with a particular image file.

    Sometimes we know the filename of an image that has been imported into
    OMERO but not necessarily the image ID. This is frequently the case when
    we want to annotate a recently imported image. This funciton will help
    to filter a list of image IDs to only those associated with a particular
    filename in ImportedImageFiles.

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    im_id : int
        ID of OMERO image.
    imported_filename : str
        The full filename (with extension) of the file whose OMERO image
        we are looking for. NOT the path of the image.

    Returns
    -------
    answer : boolean
        Answer to the question of whether the given image has an associated
        ImportedImageFile of the given name.

    Notes
    -----
    This function should be used as a filter on an image list that has been
    already narrowed down as much as possible. Note that many different images
    in OMERO may share the same filename (e.g., image.tif).

    Examples
    --------
    >>> im_ids = get_image_ids(conn, project="My Proj", dataset="Nice Pics")
    >>> im_ids = [im_id for im_id in im_ids
    ...           if image_has_imported_filename(conn, im_id, "feb_2020.tif")]
    """
    im = conn.getObject('Image', im_id)
    imp_files = im.getImportedImageFiles()
    imp_filenames = [impf.getName() for impf in imp_files]
    if imported_filename in imp_filenames:
        return True
    else:
        return False


# prints
def print_map_annotation(conn, map_ann_id):
    """Print some information and value of a map annotation.

    """
    map_ann = conn.getObject('MapAnnotation', map_ann_id)
    print(f'Map Annotation: {map_ann_id}')
    print(f'Namespace: {map_ann.getNs()}')
    print('Key-Value Pairs:')
    for k, v in map_ann.getValue():
        print(f'\t{k}:\t{v}')
