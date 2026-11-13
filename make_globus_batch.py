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
import os
import sys
from pathlib import PurePosixPath

from omero.gateway import BlitzGateway
from omero.constants.namespaces import NSFILETRANSFER
from omero.sys import ParametersI

INTERNAL_CHUNK = 1000  # image/fileset ids per HQL query

# Transfer types that leave the original file in place (the OMERO repo only
# holds a link to it). Copy/move transfers put the data inside the OMERO repo.
IN_PLACE = (
    "ome.formats.importer.transfers.SymlinkFileTransfer",
    "ome.formats.importer.transfers.HardlinkFileTransfer",
)

# Rough Globus throughput, from one recent transfer (23 GB in 1.5 min). Real
# speed varies a lot with endpoints, network and file count, so treat the
# printed estimate as a ballpark only.
GLOBUS_BYTES_PER_SECOND = 23e9 / 90

OBJECT_TYPES = ("Project", "Dataset", "Image")


def parse_object(text):
    """'Image:123' etc. -> ('Image', 123), or None if it is not that syntax."""
    obj_type, _, obj_id = text.partition(":")
    if obj_type in OBJECT_TYPES and obj_id.isdigit():
        return obj_type, int(obj_id)
    return None


def target(value):
    """A Project/Dataset/Image:<id>, or a text file of such lines."""
    parsed = parse_object(value)
    if parsed:
        return parsed
    if os.path.isfile(value):
        return "File", value
    raise argparse.ArgumentTypeError(
        "must be Project:<id>, Dataset:<id>, Image:<id>, or the path of an existing "
        "text file of such lines")


def read_objects(path):
    """OMERO objects listed in a text file, one per line, e.g. Image:123.

    Each line must be Image:<id>, Dataset:<id> or Project:<id>. Blank lines
    and anything after a '#' are ignored.
    """
    objects = []
    with open(path) as f:
        for line_number, line in enumerate(f, 1):
            text = line.split("#", 1)[0].strip()
            if not text:
                continue
            parsed = parse_object(text)
            if parsed is None:
                sys.exit(f"{path} line {line_number}: expected Image:<id>, "
                         f"Dataset:<id> or Project:<id>, got {text!r}")
            if parsed not in objects:
                objects.append(parsed)
    return objects


def q(path):
    # quote paths containing spaces for the globus batch format
    return f'"{path}"' if " " in str(path) else str(path)


def warn(msg):
    print(f"WARNING: {msg}", file=sys.stderr)


def human_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1000:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} TB"


def human_time(seconds):
    if seconds < 10:
        return f"{seconds:.1f} seconds"
    if seconds < 90:
        return f"{seconds:.0f} seconds"
    if seconds < 5400:
        return f"{seconds / 60:.1f} minutes"
    return f"{seconds / 3600:.1f} hours"


def connect(host=None):
    omero_user = input("OMERO user: ")
    omero_host = host or os.environ.get("OMERO_HOST") or input("OMERO host: ")
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
    return conn


def image_ids_in(conn, obj_type, obj_id):
    """Ids of every image in a project or dataset (or just the image itself)."""
    if obj_type == "Image":
        # not checked one by one (a file can list hundreds); images that do not
        # exist are reported by the batched lookup in lookup_images instead
        return [obj_id]
    if conn.getObject(obj_type, obj_id) is None:
        sys.exit(f"{obj_type} {obj_id} not found or not readable")
    if obj_type == "Project":
        hql = ("select distinct i.id from Project p join p.datasetLinks dl "
               "join dl.child d join d.imageLinks il join il.child i where p.id = :id")
    else:
        hql = ("select distinct i.id from Dataset d join d.imageLinks il "
               "join il.child i where d.id = :id")
    rows = conn.getQueryService().projection(
        hql, ParametersI().addId(obj_id), conn.SERVICE_OPTS)
    return [row[0].val for row in rows]


def lookup_images(conn, image_ids):
    """image id -> (name, fileset id, transfer type) for readable images.

    HQL queries (one per chunk of ids) replace per-image gateway calls, which
    cost several server round trips per image. Left joins keep images with no
    fileset (pre-5.0 imports) and filesets with no file-transfer annotation
    (copy/move uploads); both come back with transfer type None.
    """
    query_service = conn.getQueryService()
    images = {}
    for start in range(0, len(image_ids), INTERNAL_CHUNK):
        params = ParametersI().addIds(image_ids[start:start + INTERNAL_CHUNK])
        params.addString("ns", NSFILETRANSFER)
        rows = query_service.projection(
            "select i.id, i.name, i.fileset.id, a.textValue from Image i "
            "left join i.fileset fs left join fs.annotationLinks al "
            "left join al.child a with a.ns = :ns where i.id in (:ids)",
            params, conn.SERVICE_OPTS)
        for image_id, name, fs_id, transfer in rows:
            images[image_id.val] = (name.val, fs_id.val if fs_id else None,
                                    transfer.val if transfer else None)
    return images


def in_place_files(conn, fileset_ids):
    """client path -> size in bytes for the original files of these filesets.

    client paths are the original file locations given at import time.
    (server paths would point at the links inside the OMERO managed repo.)
    """
    query_service = conn.getQueryService()
    sizes = {}
    fileset_ids = sorted(fileset_ids)
    for start in range(0, len(fileset_ids), INTERNAL_CHUNK):
        for client_path, size in query_service.projection(
            "select fse.clientPath, fse.originalFile.size from FilesetEntry fse "
            "where fse.fileset.id in (:ids)",
            ParametersI().addIds(fileset_ids[start:start + INTERNAL_CHUNK]),
            conn.SERVICE_OPTS,
        ):
            sizes[client_path.val] = size.val if size else 0
    return sizes


def write_batch(sizes, out_file, src_root, dest_root, base_dir, flatten):
    """Write the globus batch file; returns (files written, bytes, skipped)."""
    # Everything here is string manipulation only (PurePosixPath never
    # touches the filesystem), so it does not matter whether the source files
    # exist or are mounted on the machine running this script.
    written = written_bytes = skipped = 0
    used_dests = {}  # flattened dest path -> source path (to detect name collisions)
    with open(out_file, "w") as f:
        for raw_path in sorted(sizes):  # sorted so the output is the same on every run
            p = PurePosixPath(raw_path)
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
            written_bytes += sizes[raw_path]
    return written, written_bytes, skipped


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Write a Globus transfer batch file for the original files of "
                    "in-place imported OMERO images, from a project, dataset or "
                    "image, or from a text file listing several such objects "
                    "(Image:, Dataset: or Project: lines). The OMERO server is "
                    "taken from --omero-host, else the OMERO_HOST environment "
                    "variable, else prompted for.",
        epilog="e.g.  pixi run --script make_globus_batch.py Dataset:123 "
               "/data/omero/lab1 /shares/my-share globus_batch.txt   (or give "
               "objects.txt instead of Dataset:123)",
    )
    parser.add_argument("target", type=target,
                        metavar="<Project|Dataset|Image>:<id> | <objects file>",
                        help="OMERO object, same syntax as the OMERO CLI "
                             "(e.g. Dataset:123), or a text file with one such object "
                             "per line, e.g. Image:123 (blank lines and # comments are "
                             "ignored)")
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
    parser.add_argument("--omero-host",
                        help="OMERO server to connect to (default: the OMERO_HOST "
                             "environment variable, else prompted for)")
    args = parser.parse_args(argv)
    (obj_type, obj_id), src_root, dest_root = args.target, args.src_root, args.dest_root

    if obj_type == "File":  # obj_id is then the path of a file of objects
        objects = read_objects(obj_id)
        print(f"read {len(objects)} objects from {obj_id}")
    else:
        objects = [(obj_type, obj_id)]

    conn = connect(args.omero_host)

    # 1. image ids of everything requested
    ids = set()  # a set: objects can overlap (an image can be in several datasets)
    for kind, number in objects:
        found = image_ids_in(conn, kind, number)
        if kind != "Image":
            print(f"{kind} {number} has {len(found)} images")
        ids.update(found)
    image_ids = sorted(ids)
    if len(objects) > 1:
        print(f"{len(image_ids)} distinct images in total")

    # 2. which of those were imported in place, and where are the files
    images = lookup_images(conn, image_ids)
    not_found = [i for i in image_ids if i not in images]  # can be many: summarized
    if not_found:
        shown = ", ".join(map(str, not_found[:10])) + (", ..." if len(not_found) > 10 else "")
        warn(f"{len(not_found)} images not found or not readable by this user; "
             f"skipped (image ids: {shown})")
    skipped = len(not_found)

    in_place_filesets = set()
    seen_skipped = set()  # warn once per skipped fileset (its images share one cause)
    for image_id, (name, fs_id, transfer) in sorted(images.items()):
        if transfer in IN_PLACE:
            in_place_filesets.add(fs_id)
            continue
        if fs_id is not None:
            if fs_id in seen_skipped:
                continue  # images from the same fileset share the same files
            seen_skipped.add(fs_id)
        how = transfer.rsplit(".", 1)[-1] if transfer else "not an in-place import"
        warn(f"image {image_id} ({name}) was not imported in place ({how}); skipped")
        skipped += 1
    sizes = in_place_files(conn, in_place_filesets)
    conn.close()

    # 3. batch file
    written, written_bytes, path_skipped = write_batch(
        sizes, args.out_file, src_root, dest_root, args.base_dir, args.flatten)
    skipped += path_skipped
    print(f"wrote {written} files to {args.out_file}; {skipped} skipped (see warnings)")
    print(f"total size {human_size(written_bytes)}; estimated Globus transfer time "
          f"~{human_time(written_bytes / GLOBUS_BYTES_PER_SECOND)} "
          "(very rough: depends on endpoints, network and file count)")


if __name__ == "__main__":
    main()
