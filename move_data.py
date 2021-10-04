import argparse
from jax_omeroutils.datamover import DataMover
from pathlib import Path

def main(import_json, fileset_list):
# Move files into place
    if Path(import_json).exists():
        mover = DataMover(import_json, fileset_list)
        message = mover.move_data()
        print(message)
    return



if __name__ == "__main__":
    description = 'Move data to server ahead of OMERO import'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('import_json',
                        type=str,
                        help='Full path of destination for the import.json'
                             ' file server-side')
    parser.add_argument('fileset_list',
                        type=str,
                        help='Text file with list of files that need'
                             ' to be transfered')
    args = parser.parse_args()

    main(Path(args.import_json),Path(args.fileset_list))