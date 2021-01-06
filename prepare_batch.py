"""
NOTE: This needs to be run as the service account that owns the directory
in which the image data will reside on the server. This should be separate
from the service account that runs OMERO, for safety.
"""

import argparse
from jax_omeroutils.intake import ImportBatch
from jax_omeroutils.config import OMERO_USER, OMERO_PASS
from jax_omeroutils.config import OMERO_HOST, OMERO_PORT
from jax_omeroutils.datamover import DataMover
from omero.gateway import BlitzGateway
from pathlib import Path


def main(import_batch_directory):

    # Validate import and write import.json
    conn = BlitzGateway(OMERO_USER,
                        OMERO_PASS,
                        host=OMERO_HOST,
                        port=OMERO_PORT)
    conn.connect()
    batch = ImportBatch(conn, import_batch_directory)
    batch.load_md()
    batch.validate_import_md()
    batch.validate_user_group()
    batch.set_server_path()
    batch.load_targets()
    batch.write_json()
    conn.close()

    # Move files into place
    mover = DataMover(import_batch_directory / 'import.json')
    message = mover.move_data()
    print(message)
    return


if __name__ == "__main__":
    description = 'Prepare a batch for OMERO import'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('import_batch_directory',
                        type=str,
                        help='Full path of directory containing images to'
                             ' import and single metadata file')
    args = parser.parse_args()

    main(Path(args.import_batch_directory))
