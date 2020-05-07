from omero.gateway import MapAnnotationWrapper


# posts
def post_map_annotation(conn, image_ids, kv_dict, ns):
    """create new map annotation and attach to images

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    image_ids : int or list of ints
        Image IDs where MapAnnotations will be posted.
    kv_dict : dict
        key-value pairs that will be included in the MapAnnotation
    ns : str
        Namespace for the MapAnnotation

    Returns
    -------
    map_ann_ids : list of ints
        IDs of newly created MapAnnotations

    Examples
    --------
    >>> ns = 'jax.org/jax/example/namespace'
    >>> d = {'species': 'human',
             'occupation': 'time traveler'
             'first name': 'Kyle',
             'surname': 'Reese'}
    >>> post_map_annotation(conn, [23,56,78], d, ns)
    [234, 235, 236]
    """
    if type(image_ids) not in [list, int]:
        raise TypeError('image_ids must be list or integer')
    if type(image_ids) is not list:
        image_ids = [image_ids]

    if len(image_ids) == 0:
        raise ValueError('image_ids must contain one or more items')

    if type(kv_dict) is not dict:
        raise TypeError('kv_dict must be of type `dict`')

    kv_pairs = []
    for k, v in kv_dict.items():
        k = str(k)
        v = str(v)
        kv_pairs.append([k, v])

    map_ann_ids = []
    for image in conn.getObjects('Image', image_ids):
        map_ann = MapAnnotationWrapper(conn)
        map_ann.setNs(str(ns))
        map_ann.setValue(kv_pairs)
        map_ann.save()
        image.linkAnnotation(map_ann)
        map_ann_ids.append(map_ann.getId())

    return map_ann_ids


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

        if type(project) is int:
            ps = conn.getObjects('Project', [project])
        elif type(project) is str:
            ps = conn.getObjects('project', attributes={'name': project})

    if dataset is not None:
        if type(dataset) not in [str, int]:
            raise TypeError('dataset must be integer or string')

        if type(dataset) is int:
            ds = conn.getObjects('Dataset', [dataset])
        elif type(dataset) is str:
            ds = conn.getObjects('Dataset', attributes={'name': dataset})

    im_ids = []
    if (project is None) and (dataset is None):
        for im in conn.listOrphans('Image'):
            im_ids.append(im.getId())
    elif project is None:
        for d in ds:
            for im in d.listChildren():
                im_ids.append(im.getId())
    elif dataset is None:
        for p in ps:
            for d in p.listChildren():
                for im in d.listChildren():
                    im_ids.append(im.getId())
    else:
        d_ids = [d.getId() for d in ds]
        for p in ps:
            for d in p.listChildren():
                if d.getId() in d_ids:
                    for im in d.listChildren():
                        im_ids.append(im.getId())
    return im_ids


def get_group_id(conn, group_name):
    """Get ID of a group based on group name.

    Parameters
    ----------
    conn : ``omero.gateway.BlitzGateway`` object
        OMERO connection.
    group_name : str
        Name of the group for which an ID is to be returned.

    Returns
    -------
    group_id : int
        ID of the OMERO group.

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
