import argparse
from getpass import getpass
from omero.gateway import BlitzGateway
from ezomero import get_image_ids, get_group_id, get_image


def main(group, admin_user, server, port):
    password = getpass(f'Enter password for {admin_user}: ')
    conn = BlitzGateway(admin_user, password, host=server, port=port)
    conn.connect()
    group_id = get_group_id(conn, group)
    conn.SERVICE_OPTS.setOmeroGroup(group_id)
    image_ids = get_image_ids(conn)
    with open("omeroImages2Import.tsv", 'w') as f:
        f.write('omero_id\timage_file\timage_name\n')
        for im_id in image_ids:
            im_o, _ = get_image(conn, im_id, no_pixels=True)
            imp_files = im_o.getImportedImageFiles()
            imp_filename = list(imp_files)[0].getName()
            image_name = im_o.getName()
            out_string = f'{im_id}\t{imp_filename}\t{image_name}\n'
            f.write(out_string)
    conn.close()


if __name__ == "__main__":
    description = ("Output a tsv of orphaned images for a particular group. "
                   "Columns: omero_id, image_file, image_name")
    parser = argparse.ArgumentParser(description=description)
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
    main(args.group, args.sudo, args.server, args.port)
