import pytest
import numpy as np
from datetime import datetime
from jax_omeroutils import ezomero
from omero.gateway import BlitzGateway

# we can change this later
@pytest.fixture(scope="session")
def omero_params():
    user = 'root'
    password = 'omero'
    host = 'localhost'
    port = 6064
    secure = True
    return(user, password, host, port, secure)


@pytest.fixture(scope='session')
def conn(omero_params):
    user, password, host, port, secure = omero_params
    conn = BlitzGateway(user, password, host=host, port=port, secure=secure)
    conn.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def image_fixture():
    test_image = np.zeros((200, 200, 20, 3, 1), dtype=np.uint8)
    test_image[0:100, 0:100, 0:10, 0, :] = 255
    test_image[0:100, 0:100, 11:20, 1, :] = 255
    test_image[101:200, 101:200, :, 2, :] = 255
    return test_image


@pytest.fixture(scope='session')
def timestamp():
    return f'{datetime.now():%Y%m%d%H%M%S}'


@pytest.fixture(scope='session')
def project_structure(conn, timestamp, image_fixture):
    """
    Project              Dataset           Image
    -------              -------           -----
    main_proj   ---->    main_ds    ---->   im0
                ---->    sec_ds     ---->   im1
                ---->    dupe1_ds(1)---->   im2

    sec_proj    ---->    dupe1_ds(2)---->   im3

    dupe_proj(1)---->    dupe2_ds(1)---->   im4

    dupe_proj(2)---->    dupe2_ds(2)---->   im5
    """
    main_proj_name = "main_proj_" + timestamp
    main_proj_id = ezomero.post_project(conn, main_proj_name)

    sec_proj_name = "sec_proj_" + timestamp
    sec_proj_id = ezomero.post_project(conn, sec_proj_name)

    dupe_proj_name = "dupe_proj_" + timestamp
    dupe_proj_id1 = ezomero.post_project(conn, dupe_proj_name)
    dupe_proj_id2 = ezomero.post_project(conn, dupe_proj_name)

    main_ds_name = "main_ds_" + timestamp
    main_ds_id = ezomero.post_dataset(conn, main_ds_name,
                                      project_id=main_proj_id)

    sec_ds_name = "sec_ds_" + timestamp
    sec_ds_id = ezomero.post_dataset(conn, sec_ds_name,
                                     project_id=main_proj_id)

    dupe1_ds_name = "dupe1_ds_" + timestamp
    dupe1_ds_id1 = ezomero.post_dataset(conn, dupe1_ds_name,
                                        project_id=main_proj_id)
    dupe1_ds_id2 = ezomero.post_dataset(conn, dupe1_ds_name,
                                        project_id=sec_proj_id)

    dupe2_ds_name = "dupe2_ds_" + timestamp
    dupe2_ds_id1 = ezomero.post_dataset(conn, dupe2_ds_name,
                                        project_id=dupe_proj_id1)
    dupe2_ds_id2 = ezomero.post_dataset(conn, dupe2_ds_name,
                                        project_id=dupe_proj_id2)

    image_ids = []
    for i, dsid in enumerate([main_ds_id,
                              sec_ds_id,
                              dupe1_ds_id1,
                              dupe1_ds_id2,
                              dupe2_ds_id1,
                              dupe2_ds_id2]):
        im_name = f'im{i}_' + timestamp
        im_id = ezomero.post_image(conn, image_fixture, im_name,
                                   dataset_id=dsid)
        image_ids.append(im_id)

    return({'main_proj': main_proj_id,
            'sec_proj': sec_ds_id,
            'dupe_proj': [dupe_proj_id1,
                          dupe_proj_id2],
            'main_ds': main_ds_id,
            'sec_ds': sec_ds_id,
            'dupe1_ds': [dupe1_ds_id1,
                         dupe1_ds_id2],
            'dupe2_ds': [dupe2_ds_id1,
                         dupe2_ds_id2],
            'image_ids': image_ids})


def test_omero_connection(conn, omero_params):
    assert conn.getUser().getName() == omero_params[0]

# test posts
# def test_post_map_annotation(conn):
#     pid = ezomero.post_project(conn, "Map Ann proj")
#     map_ann_id = ezomero.post_map_annotation(conn, )


def test_post_dataset(conn, project_structure, timestamp):
    # Orphaned dataset, with descripion
    ds_test_name = 'test_post_dataset_' + timestamp
    did = ezomero.post_dataset(conn, ds_test_name, description='New test')
    assert conn.getObject("Dataset", did).getName() == ds_test_name
    assert conn.getObject("Dataset", did).getDescription() == "New test"

    # Dataset in project, no description
    ds_test_name2 = 'test_post_dataset2_' + timestamp
    pid = project_structure['main_proj']
    did2 = ezomero.post_dataset(conn, ds_test_name2, project_id=pid)
    ds = conn.getObjects("Dataset", opts={'project': pid})
    ds_names = [d.getName() for d in ds]
    assert ds_test_name2 in ds_names
    conn.deleteObjects("Dataset", [did, did2], deleteAnns=True,
                       deleteChildren=True, wait=True)


def test_post_image(conn, project_structure, timestamp, image_fixture):
    # Post image in dataset
    image_name = 'test_post_image_' + timestamp
    im_id = ezomero.post_image(conn, image_fixture, image_name,
                               description='This is an image',
                               dataset_id=project_structure["main_ds"])
    assert conn.getObject("Image", im_id).getName() == image_name

    # Post orphaned image
    im_id2 = ezomero.post_image(conn, image_fixture, image_name)
    assert conn.getObject("Image", im_id2).getName() == image_name
    conn.deleteObjects("Image", [im_id, im_id2], deleteAnns=True,
                       deleteChildren=True, wait=True)


def test_post_project(conn, timestamp):
    # No description
    new_proj = "test_post_project_" + timestamp
    pid = ezomero.post_project(conn, new_proj)
    assert conn.getObject("Project", pid).getName() == new_proj

    # With description
    new_proj2 = "test_post_project2_" + timestamp
    desc = "Now with a description"
    pid2 = ezomero.post_project(conn, new_proj2, description=desc)
    assert conn.getObject("Project", pid2).getDescription() == desc
    conn.deleteObjects("Project", [pid, pid2], deleteAnns=True,
                       deleteChildren=True, wait=True)


def test_post_project_type(conn):
    with pytest.raises(TypeError):
        _ = ezomero.post_project(conn, 123)
    with pytest.raises(TypeError):
        _ = ezomero.post_project(conn, '123', description=1245)


# test gets

def test_get_image_ids(conn, project_structure):
    # Based on Project ID
    image_ids = project_structure["image_ids"]
    main_proj_id = project_structure['main_proj']
    im_ids = ezomero.get_image_ids(conn, project=main_proj_id)
    assert all([i in im_ids for i in image_ids[:3]])
    assert not any([i in im_ids for i in image_ids[3:]])

    # Based on dataset ID
    main_ds_id = project_structure['main_ds']
    im_ids = ezomero.get_image_ids(conn, dataset=main_ds_id)
    assert im_ids[0] == image_ids[0]
    assert len(im_ids) == 1

    # Based on unique dataset name
    ds_name = conn.getObject('Dataset', project_structure['sec_ds']).getName()
    im_ids = ezomero.get_image_ids(conn, dataset=ds_name)
    assert im_ids[0] == image_ids[1]
    assert len(im_ids) == 1

    # Based on duplicate dataset name
    ds_list = conn.getObjects('Dataset', project_structure['dupe1_ds'])
    ds_name = list(ds_list)[0].getName()
    im_ids = ezomero.get_image_ids(conn, dataset=ds_name)
    assert all([i in im_ids for i in image_ids[2:4]])
    bad_list = [image_ids[0], image_ids[1], image_ids[4], image_ids[5]]
    assert not any([i in im_ids for i in bad_list])

    # Based on unique project name, duplicate dataset name
    pj = conn.getObject('Project', project_structure['main_proj'])
    pj_name = pj.getName()
    im_ids = ezomero.get_image_ids(conn, project=pj_name, dataset=ds_name)
    assert im_ids[0] == image_ids[2]
    assert len(im_ids) == 1

    # Based on duplicate project and dataset names
    ds_list = conn.getObjects('Dataset', project_structure['dupe2_ds'])
    pj_list = conn.getObjects('Project', project_structure['dupe_proj'])
    ds_name = list(ds_list)[0].getName()
    pj_name = list(pj_list)[0].getName()
    im_ids = ezomero.get_image_ids(conn, project=pj_name, dataset=ds_name)
    assert all([i in im_ids for i in image_ids[4:]])
    assert not any([i in im_ids for i in image_ids[:4]])

    # Return nothing on bad input
    im_ids = ezomero.get_image_ids(conn, project="ajhasfkjhg")
    im_ids2 = ezomero.get_image_ids(conn, dataset=999999)
    assert len(im_ids) == 0
    assert len(im_ids2) == 0
