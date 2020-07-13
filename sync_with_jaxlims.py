import argparse
import logging
import json
from getpass import getpass
from omero.gateway import BlitzGateway
from jax_omeroutils import intake
from jax_omeroutils.ezomero import get_image_ids
from jax_omeroutils.ezomero import post_project, post_dataset
from jax_omeroutils.ezomero import post_map_annotation
from jax_omeroutils.ezomero import filter_by_filename
from jax_omeroutils.ezomero import link_images_to_dataset

CURRENT_MD_NS = 'jax.org/omeroutils/jaxlims/v0'


def main(md_filepath, user_name, group, admin_user, server, port):

    # create connection and establish context
    password = getpass(f'Enter password for {admin_user}: ')
    su_conn = BlitzGateway(admin_user, password, host=server, port=port)
    su_conn.connect()
    conn = su_conn.suConn(user_name, group, 600000)
    su_conn.close()
    orphan_ids = get_image_ids(conn)

    # load and prepare metadata
    md = intake.load_md_from_file(md_filepath)

    if 'filename' not in md.columns:
        logging.error('Metadata file missing filename column')
        return
    if 'dataset' not in md.columns:
        logging.error('Metadata file missing dataset column')
        return
    if 'project' not in md.columns:
        logging.error('Metadata file missing project column')
        return

    md_json = json.loads(md.to_json(orient='table', index=False))

    # loop over metadata, move and annotate matching images
    processed_filenames = []
    for row in md_json['data']:
        row.pop('OMERO_group', None)  # No longer using this field
        project_name = str(row.pop('project'))
        dataset_name = str(row.pop('dataset'))
        filename = row.pop('filename')
        if filename not in processed_filenames:
            image_ids = filter_by_filename(conn, orphan_ids, filename)
            if len(image_ids) > 0:
                # move image into place, create projects/datasets as necessary
                project_id = set_or_create_project(conn, project_name)
                dataset_id = set_or_create_dataset(conn,
                                                   project_id,
                                                   dataset_name)
                link_images_to_dataset(conn, image_ids, dataset_id)
                print(f'Moved images:{image_ids} to dataset:{dataset_id}')

                # map annotations
                ns = CURRENT_MD_NS
                map_ann_id = post_map_annotation(conn,
                                                 "Image",
                                                 image_ids,
                                                 row,
                                                 ns)
                print(f'Created annotation:{map_ann_id}'
                      f' and linked to images:{image_ids}')
                processed_filenames.append(filename)

            else:
                print(f'Image with filename:{filename} not found in orphans')
        else:
            print(f'Already processed images with filename:{filename}')

    conn.close()
    print('Complete!')


def set_or_create_project(conn, project_name):
    ps = conn.getObjects('Project', attributes={'name': project_name})
    ps = list(ps)
    if len(ps) == 0:
        project_id = post_project(conn, project_name)
        print(f'Created new Project:{project_id}')
    else:
        project_id = ps[0].getId()
    return project_id


def set_or_create_dataset(conn, project_id, dataset_name):
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


if __name__ == "__main__":
    description = ("Use metadata from jaxLIMS to organize orphaned files."
                   " Metadata is provided as tsv. Please contact Dave Mellert"
                   " and Mike McFarland for more details.")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('md', type=str, help='Path to jaxlims metadata')
    parser.add_argument('-u', '--user',
                        type=str,
                        help='OMERO user who owns the images (REQUIRED)',
                        required=True)
    parser.add_argument('-g', '--group',
                        type=str,
                        help='Group in which to find orphans (REQUIRED)',
                        required=True)
    parser.add_argument('--sudo',
                        type=str,
                        help='OMERO admin user for login (REQUIRED)',
                        required=True)
    parser.add_argument('-s', '--server',
                        type=str,
                        help='OMERO server hostname (default = localhost)',
                        default='localhost')
    parser.add_argument('-p', '--port',
                        type=int,
                        help='OMERO server port (default = 4064)',
                        default=4064)
    args = parser.parse_args()
    main(args.md, args.user, args.group, args.sudo, args.server, args.port)
