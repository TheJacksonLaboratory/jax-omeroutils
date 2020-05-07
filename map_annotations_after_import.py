import argparse
import json
import logging
import pathlib
from getpass import getpass
from omero.gateway import BlitzGateway
from omeroutils.ezomero import get_group_id, get_image_ids
from omeroutils.ezomero import post_map_annotation, image_has_imported_filename

CURRENT_MD_NS = 'jax.org/omeroutils/user_submitted/v0'


def main(md_path, admin_user, server, port):

    # load metadata
    md_path = pathlib.Path(md_path)
    if not md_path.is_file():
        logging.error('file does not exist, check md argument')
        return

    try:
        with open(md_path, 'r') as f:
            md = json.load(f)
    except json.JSONDecodeError as err:
        logging.error(f'Unable to load json: {err}')
        return
    annotation_md = md['data']

    # set up connection
    password = getpass(f'Enter password for {admin_user}')
    conn = BlitzGateway(admin_user, password, host=server, port=port)
    conn.connect()
    group_id = get_group_id(conn, md['OMERO_group'])
    conn.SERVICE_OPTS.setOmeroGroup(group_id)

    # loop through metadata and annotate
    print('New Annotations:')
    for md in annotation_md:
        project = str(md.pop('project'))
        dataset = str(md.pop('dataset'))
        filename = md.pop('filename')
        ns = CURRENT_MD_NS
        im_ids = get_image_ids(conn, project=project, dataset=dataset)
        im_ids = [im_id for im_id in im_ids
                  if image_has_imported_filename(conn, im_id, filename)]
        if len(im_ids) == 0:
            print(f"Cannot annotate {project} / {dataset} / {filename}"
                  " because it can not be found")
        else:
            map_ann_ids = post_map_annotation(conn, im_ids, md, ns)
            print(map_ann_ids)
    conn.close()
    print('Complete!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Use import_md.json output'
                                     'from prepare_import_batch.py to add map'
                                     'annotations to OMERO images.')
    parser.add_argument('md', type=str, help='Path to import_md.json')
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
    main(args.md, args.sudo, args.server, args.port)
