# /// script
# requires-python = ">=3.11"
# dependencies = []
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
# platforms = ["osx-arm64", "linux-64"]
#
# [tool.pixi.dependencies]
# omero-py = "*"
# python = "3.12.*"
# ///

import argparse
import getpass
import sys
import os
from pathlib import PurePosixPath

from omero.gateway import BlitzGateway


def omero_object(value):
    obj_type, _, obj_id = value.partition(":")
    if obj_type not in ("Project", "Dataset") or not obj_id.isdigit():
        raise argparse.ArgumentTypeError("must be Project:<id> or Dataset:<id>")
    return obj_type, int(obj_id)


parser = argparse.ArgumentParser(
    description="Write a Globus transfer batch file for the original files of "
                "in-place imported OMERO images. The OMERO server is read from "
                "the OMERO_HOST environment variable (or prompted for).",
    epilog="e.g.  pixi run --script make_globus_batch.py Dataset:123 /data/omero/lab1 "
           "/shares/my-share globus_batch.txt",
)
parser.add_argument("target", type=omero_object, metavar="<Project|Dataset>:<id>",
                    help="OMERO object, same syntax as the OMERO CLI (e.g. Dataset:123)")
parser.add_argument("src_root", type=PurePosixPath,
                    help="common parent folder of the source files, as seen on the "
                         "machine running the Globus source endpoint")
parser.add_argument("dest_root", type=PurePosixPath,
                    help="folder on the destination Globus collection to copy into")
parser.add_argument("out_file", help="batch file to write")
parser.add_argument("base_dir", nargs="?", type=PurePosixPath,
                    help="only needed if images were imported with relative paths")
parser.add_argument("--flatten", action="store_true",
                    help="put every file directly in dest_root instead of recreating "
                         "the folder structure below src_root; files whose names "
                         "collide are skipped with a warning")
args = parser.parse_args()
(obj_type, obj_id), src_root, dest_root = args.target, args.src_root, args.dest_root
out_file, base_dir, flatten = args.out_file, args.base_dir, args.flatten

# Transfer types that leave the original file in place (the OMERO repo only
# holds a link to it). Copy/move transfers put the data inside the OMERO repo.
IN_PLACE = (
    "ome.formats.importer.transfers.SymlinkFileTransfer",
    "ome.formats.importer.transfers.HardlinkFileTransfer",
)


def q(path):
    # quote paths containing spaces for the globus batch format
    return f'"{path}"' if " " in str(path) else str(path)


def warn(msg):
    print(f"WARNING: {msg}", file=sys.stderr)


omero_user = input("OMERO user: ")
omero_host = os.environ.get("OMERO_HOST") or input("OMERO host: ")
conn = BlitzGateway(
    username=omero_user,
    passwd=getpass.getpass(),
    host=omero_host,
    port=4064,
)
if not conn.connect():
    sys.exit(
        f"Could not connect to OMERO at {omero_host}:4064 as "
        f"'{omero_user}'. Check your "
        "username, password, and host (and that the server is reachable)."
    )
conn.SERVICE_OPTS.setOmeroGroup(-1)  # search across all groups you can read

obj = conn.getObject(obj_type, obj_id)
if obj is None:
    sys.exit(f"{obj_type} {obj_id} not found or not readable")

if obj_type == "Project":
    images = (img for ds in obj.listChildren() for img in ds.listChildren())
else:
    images = obj.listChildren()

paths = set()
skipped = 0
seen_filesets = {}  # fileset id -> list of source paths, or None if skipped
for img in images:
    fileset = img._obj.fileset  # unloaded Fileset; None for pre-FS images
    fs_id = fileset.id.val if fileset is not None else None
    if fs_id is not None and fs_id in seen_filesets:
        continue  # images from the same fileset share the same files

    transfer = img.getInplaceImport()
    if transfer not in IN_PLACE:
        how = transfer.rsplit(".", 1)[-1] if transfer else "not an in-place import"
        warn(f"image {img.getId()} ({img.getName()}) was not imported in place "
                f"({how}); skipped")
        skipped += 1
        if fs_id is not None:
            seen_filesets[fs_id] = None
        continue

    # client_paths are the original file locations given at import time.
    # (server_paths would point at the links inside the OMERO managed repo.)
    files = img.getImportedImageFilePaths()["client_paths"]
    if fs_id is not None:
        seen_filesets[fs_id] = files
    paths.update(files)
conn.close()

# Everything in this loop is string manipulation only (PurePosixPath never
# touches the filesystem), so it does not matter whether the source files
# exist or are mounted on the machine running this script.
written = 0
used_dests = {}  # flattened dest path -> source path (to detect name collisions)
with open(out_file, "w") as f:
    for p in sorted(paths):  # sorted so the output is the same on every run
        p = PurePosixPath(p)
        # make relative import paths absolute by prefixing base_dir
        if not p.is_absolute() and base_dir is not None:
            p = base_dir / p
        # Globus needs absolute source paths; skip anything still relative
        if not p.is_absolute():
            warn(f"{p} is a relative path (use the base_dir argument); skipped")
            skipped += 1
            continue
        # strip src_root off the front to get the folder layout to recreate
        # under dest_root (e.g. /src_root/a/b.tif -> a/b.tif)
        try:
            rel = p.relative_to(src_root)
        except ValueError:  # path is not under src_root
            warn(f"{p} is not under {src_root}; skipped")
            skipped += 1
            continue
        dest = dest_root / p.name if flatten else dest_root / rel
        if flatten:
            if dest in used_dests:
                warn(f"{p} would overwrite {used_dests[dest]} at {dest} when "
                     "flattened; skipped")
                skipped += 1
                continue
            used_dests[dest] = p
        f.write(f"{q(p)} {q(dest)}\n")
        written += 1
print(f"wrote {written} files to {out_file}; {skipped} skipped (see warnings)")