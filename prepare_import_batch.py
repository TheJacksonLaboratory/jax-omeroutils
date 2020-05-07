import argparse
from pathlib import Path
from omeroutils import intake


def main(basepath, targetpath, user, group):
    batch = intake.ImportBatch(basepath, user, group)
    batch.validate_batch()
    targetpath = str(Path(targetpath))  # sanitize; need to switch to pathlib!
    batch.set_target_path(targetpath)
    batch.write_files()
    print(f'Success! Files written to {targetpath}')
    return


if __name__ == "__main__":
    description = 'Prepare an import batch by creating import files'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('import_batch_directory',
                        type=str,
                        help='Full path of directory containing images to'
                             ' import and single metadata file')
    parser.add_argument('target_directory',
                        type=str,
                        help='Full path of directory where images will'
                             ' reside on server')
    parser.add_argument('-u', '--user',
                        type=str,
                        help='OMERO user who will own the images (REQUIRED)',
                        required=True)
    parser.add_argument('-g', '--group',
                        type=str,
                        help='OMERO group who will own the images (REQUIRED)',
                        required=True)
    args = parser.parse_args()

    main(args.import_batch_directory, args.target_directory,
         args.user, args.group)
