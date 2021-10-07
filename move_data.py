import argparse
from datetime import datetime
from jax_omeroutils.datamover import DataMover
from pathlib import Path

def main(import_batch_directory, fileset_list, log_directory, timestamp):
# Move files into place
    if Path(import_batch_directory / 'import.json').exists():
        mover = DataMover(import_batch_directory / 'import.json', fileset_list)
        mover.set_logging(log_directory, timestamp)
        message = mover.move_data()
        print(message)
    return



if __name__ == "__main__":
    description = 'Move data to server ahead of OMERO import'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('import_batch_directory',
                        type=str,
                        help='Full path of directory containing images to'
                             ' import and single metadata file')
    parser.add_argument('fileset_list',
                        type=str,
                        help='Text file with list of files that need'
                             ' to be transfered')
    parser.add_argument('log_directory',
                        type=str,
                        help='Directory for the log files')
    parser.add_argument('timestamp',
                        type=str,
                        required=False,
                        default=datetime.now().strftime('%Y%m%d_%H%M%S'),
                        help='Timestamp for the log files')
    args = parser.parse_args()

    main(Path(args.import_batch_directory),Path(args.fileset_list),Path(args.log_directory),
         args.timestamp)