import getpass
import sys
import os
from pathlib import PurePosixPath

from omero.gateway import BlitzGateway

# usage: python make_globus_batch.py <Project|Dataset>:<id> <src_root> <dest_root> <out_file> [<base_dir>]
#   <Project|Dataset>:<id>: OMERO object, same syntax as the OMERO CLI (e.g. Dataset:123)
#   src_root: common parent folder of the source files, as seen on the machine
#             running the Globus source endpoint
#   dest_root: folder on the destination Globus collection to copy into
#   base_dir: only needed if images were imported with relative paths
# The OMERO server is read from the OMERO_HOST environment variable (or prompted for).
# e.g.   python make_globus_batch.py Dataset:123 /data/omero/lab1 \
#            /shares/my-share globus_batch.txt
target, src_root, dest_root, out_file = sys.argv[1:5]
obj_type, _, obj_id = target.partition(":")
if obj_type not in ("Project", "Dataset") or not obj_id.isdigit():
    sys.exit("first argument must be Project:<id> or Dataset:<id>")
base_dir = PurePosixPath(sys.argv[5]) if len(sys.argv) > 5 else None
src_root, dest_root = PurePosixPath(src_root), PurePosixPath(dest_root)

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


conn = BlitzGateway(
    username=input("OMERO user: "),
    passwd=getpass.getpass(),
    host=os.environ.get("OMERO_HOST") or input("OMERO host: "),
    port=4064,
)
conn.connect()
conn.SERVICE_OPTS.setOmeroGroup(-1)  # search across all groups you can read

obj = conn.getObject(obj_type, int(obj_id))
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
        f.write(f"{q(p)} {q(dest_root / rel)}\n")
print(f"wrote {len(paths)} files to {out_file}; {skipped} skipped (see warnings)")