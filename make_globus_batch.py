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
# requests = "*"  # only used with --public
# ///

import argparse
import csv
import getpass
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import PurePosixPath

import requests  # only used with --public
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
    omero_user = input("Internal OMERO user: ")
    omero_host = host or os.environ.get("OMERO_HOST") or input("Internal OMERO host: ")
    conn = BlitzGateway(
        username=omero_user,
        passwd=getpass.getpass("Internal OMERO password: "),
        host=omero_host,
        port=4064,
    )
    if not conn.connect():
        sys.exit(
            f"Could not connect to the internal OMERO at {omero_host}:4064 as "
            f"'{omero_user}'. Check your "
            "username, password, and host (and that the server is reachable)."
        )
    conn.SERVICE_OPTS.setOmeroGroup(-1)  # search across all groups you can read
    return conn


# ---- PUBLIC OMERO ONLY (--public) --------------------------------------------
# Images on the public OMERO (no login needed) were copied from the internal
# OMERO with omero-cli-transfer, which records the internal image id in a map
# annotation. Everything below turns public objects into internal image ids;
# the rest of the script then treats both modes the same.

DEFAULT_PUBLIC_URL = "https://images.jax.org"
ORIGIN_NS = "openmicroscopy.org/cli/transfer"
ORIGIN_KEY = "origin_image_id"
PUBLIC_PAGE_SIZE = 500   # maxLimit of the public JSON API
PUBLIC_ANNOTATION_CHUNK = 100  # image ids per annotation request (500 is too long a URL)
PUBLIC_THREADS = 8


def public_get(session, url, params=None):
    """Public mode only: GET a JSON document; None if it is a 404."""
    try:
        response = session.get(url, params=params, timeout=120)
    except requests.RequestException as e:
        sys.exit(f"Could not reach the public OMERO at {url}: {e}")
    if response.status_code == 404:
        return None
    if not response.ok:
        sys.exit(f"Public OMERO returned HTTP {response.status_code} for {url}")
    return response.json()


def public_list(session, url):
    """Public mode only: all items of a paginated JSON API listing."""
    items = []
    while True:
        page = public_get(session, url, {"limit": PUBLIC_PAGE_SIZE, "offset": len(items)})
        items += page["data"]
        if not page["data"] or len(items) >= page["meta"]["totalCount"]:
            return items


def public_image_ids(session, public_url, obj_type, obj_id):
    """Public mode only: public ids of every image in a public project or dataset."""
    api = f"{public_url}/api/v0/m"
    if public_get(session, f"{api}/{obj_type.lower()}s/{obj_id}/") is None:
        sys.exit(f"{obj_type} {obj_id} not found or not readable in the public OMERO")
    if obj_type == "Project":
        dataset_ids = [d["@id"] for d in public_list(session, f"{api}/projects/{obj_id}/datasets/")]
    else:
        dataset_ids = [obj_id]
    with ThreadPoolExecutor(PUBLIC_THREADS) as pool:
        listings = pool.map(
            lambda d: public_list(session, f"{api}/datasets/{d}/images/"), dataset_ids)
        return [image["@id"] for images in listings for image in images]


def public_to_internal_ids(session, public_url, public_ids):
    """Public mode only: map public image id -> internal image id.

    Read from the origin_image_id annotation. Images without exactly one
    numeric value (or that do not exist / are not public) are left out.
    """
    def fetch(chunk):
        # webclient endpoint: the JSON API has no annotation listing. Filtering
        # by namespace on the server keeps the response small.
        result = public_get(
            session, f"{public_url}/webclient/api/annotations/",
            [("type", "map"), ("ns", ORIGIN_NS)] + [("image", i) for i in chunk])
        found = {}
        for annotation in result["annotations"]:
            values = dict(annotation["values"])
            if ORIGIN_KEY in values:
                found.setdefault(annotation["link"]["parent"]["id"], set()).add(values[ORIGIN_KEY])
        return found

    chunks = [public_ids[i:i + PUBLIC_ANNOTATION_CHUNK]
              for i in range(0, len(public_ids), PUBLIC_ANNOTATION_CHUNK)]
    origins = {}
    with ThreadPoolExecutor(PUBLIC_THREADS) as pool:
        for found in pool.map(fetch, chunks):
            origins.update(found)

    mapping = {p: int(next(iter(v))) for p, v in origins.items()
               if len(v) == 1 and next(iter(v)).isdigit()}
    unmapped = [p for p in public_ids if p not in mapping]
    if unmapped:
        shown = ", ".join(map(str, unmapped[:10])) + (", ..." if len(unmapped) > 10 else "")
        warn(f"{len(unmapped)} public images have no usable {ORIGIN_KEY} in {ORIGIN_NS} "
             f"(missing, ambiguous, non-numeric, or image not public); skipped "
             f"(public image ids: {shown})")
    return mapping


# ---- both modes ----------------------------------------------------------------

def image_ids_in(conn, obj_type, obj_id):
    """Ids of every image in a project or dataset."""
    if conn.getObject(obj_type, obj_id) is None:
        sys.exit(f"{obj_type} {obj_id} not found or not readable in the internal OMERO")
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
    """(client path -> size in bytes, fileset id -> its client paths).

    client paths are the original file locations given at import time.
    (server paths would point at the links inside the OMERO managed repo.)
    """
    query_service = conn.getQueryService()
    sizes, files_of = {}, {}
    fileset_ids = sorted(fileset_ids)
    for start in range(0, len(fileset_ids), INTERNAL_CHUNK):
        for client_path, size, fs_id in query_service.projection(
            "select fse.clientPath, fse.originalFile.size, fse.fileset.id "
            "from FilesetEntry fse where fse.fileset.id in (:ids)",
            ParametersI().addIds(fileset_ids[start:start + INTERNAL_CHUNK]),
            conn.SERVICE_OPTS,
        ):
            sizes[client_path.val] = size.val if size else 0
            files_of.setdefault(fs_id.val, set()).add(client_path.val)
    return sizes, files_of


def write_batch(files_of, sizes, out_file, src_root, dest_root, base_dir, flatten):
    """Write the globus batch file.

    A fileset is transferred whole or not at all: if any of its files would be
    skipped (relative path, outside src_root, or a name collision when
    flattening), none of its files are written. Filesets are taken in id order,
    so on a collision the lower fileset id wins.

    Returns ({client path: dest} written, bytes written, {fileset id: why skipped}).
    """
    # Everything here is string manipulation only (PurePosixPath never
    # touches the filesystem), so it does not matter whether the source files
    # exist or are mounted on the machine running this script.
    scheduled = {}  # client path -> (source path, dest) of the files to transfer
    used_dests = {}  # dest -> source path, to detect name collisions
    skipped_filesets = {}  # fileset id -> why none of its files are transferred
    for fs_id in sorted(files_of):
        planned, planned_dests = {}, {}  # this fileset's files, before committing
        for raw_path in sorted(files_of[fs_id]):
            p = PurePosixPath(raw_path)
            # make relative import paths absolute by prefixing base_dir
            if not p.is_absolute() and base_dir is not None:
                p = base_dir / p
            reason = dest = None
            if not p.is_absolute():  # Globus needs absolute source paths
                reason = "a file has a relative path (use the base_dir argument)"
                warn(f"{p} is a relative path (use the base_dir argument); "
                     f"fileset {fs_id} skipped")
            elif not p.is_relative_to(src_root):
                reason = "a file is not under src_root"
                warn(f"{p} is not under {src_root}; fileset {fs_id} skipped")
            else:
                # strip src_root off the front to get the folder layout to recreate
                # under dest_root (e.g. /src_root/a/b.tif -> a/b.tif)
                dest = dest_root / p.name if flatten else dest_root / p.relative_to(src_root)
                other = used_dests.get(dest, planned_dests.get(dest, p))
                if other != p:  # the same file already scheduled is fine
                    reason = "a file has the same name as another file when flattened"
                    warn(f"{p} would overwrite {other} at {dest} when flattened; "
                         f"fileset {fs_id} skipped")
            if reason:
                skipped_filesets[fs_id] = reason
                break
            planned[raw_path], planned_dests[dest] = (p, dest), p
        else:  # every file is fine: commit the whole fileset
            scheduled.update(planned)
            used_dests.update(planned_dests)
    written_bytes = 0
    seen = set()  # a file shared by several filesets is written once
    with open(out_file, "w") as f:
        for raw_path in sorted(scheduled):  # sorted so the output is the same on every run
            p, dest = scheduled[raw_path]
            if p not in seen:
                seen.add(p)
                f.write(f"{q(p)} {q(dest)}\n")
                written_bytes += sizes[raw_path]
    return {raw: dest for raw, (_, dest) in scheduled.items()}, written_bytes, skipped_filesets


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Write a Globus transfer batch file for the original files of "
                    "in-place imported OMERO images, from a project, dataset or "
                    "image, or from a text file listing several such objects "
                    "(Image:, Dataset: or Project: lines). The internal OMERO server is "
                    "taken from --omero-host, else the OMERO_HOST environment "
                    "variable, else prompted for. With --public the ids are those "
                    "of the PUBLIC OMERO instead, and each image is translated to "
                    f"its internal id via its {ORIGIN_KEY} annotation.",
        epilog="e.g.  pixi run --script make_globus_batch.py Dataset:123 "
               "/data/omero/lab1 /shares/my-share globus_batch.txt   (or give "
               "objects.txt instead of Dataset:123; add --public for public ids)",
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
                             "collide cause their whole fileset to be skipped, with a "
                             "warning")
    parser.add_argument("--report", metavar="FILE",
                        help="also write a TSV report (no source paths or internal ids, "
                             "so it can be shared) with columns image_ids, "
                             "destination_path, status: a row per file of each "
                             "transferred fileset, and one row, with the reason in "
                             "status, for anything skipped. image_ids are the requested "
                             "images of the fileset (public ids with --public), "
                             "separated by ';'")
    parser.add_argument("--omero-host",
                        help="internal OMERO server to connect to (default: the "
                             "OMERO_HOST environment variable, else prompted for)")
    parser.add_argument("--public", action="store_true",
                        help="the ids are on the PUBLIC OMERO (no login needed), not "
                             "the internal one")
    parser.add_argument("--public-url",
                        help=f"public OMERO web address (implies --public; "
                             f"default {DEFAULT_PUBLIC_URL})")
    args = parser.parse_args(argv)
    (obj_type, obj_id), src_root, dest_root = args.target, args.src_root, args.dest_root
    public = args.public or args.public_url is not None
    public_url = (args.public_url or DEFAULT_PUBLIC_URL).rstrip("/")

    print("Running with --public: expecting ids from the PUBLIC OMERO "
          f"({public_url}), translated to internal ids via {ORIGIN_KEY}" if public else
          "Running without --public: expecting ids from the INTERNAL OMERO")

    if obj_type == "File":  # obj_id is then the path of a file of objects
        objects = read_objects(obj_id)
        print(f"read {len(objects)} objects from {obj_id}")
    else:
        objects = [(obj_type, obj_id)]

    conn = connect(args.omero_host)

    # 1. image ids of everything requested
    session = requests.Session()  # public mode only
    ids = set()  # a set: objects can overlap (an image can be in several datasets)
    for kind, number in objects:
        if kind == "Image":
            found = [number]  # not checked one by one (a file can list hundreds);
            # missing images are reported by the batched lookups below instead
        elif public:
            found = public_image_ids(session, public_url, kind, number)
        else:
            found = image_ids_in(conn, kind, number)
        if kind != "Image":
            print(f"{kind} {number} has {len(set(found))} images")
        ids.update(found)
    if len(objects) > 1:
        print(f"{len(ids)} distinct images in total")
    unmapped = []  # public ids with no internal id
    original_id = {i: [i] for i in ids}  # internal image id -> its ids in the input's terms
    if public:  # translate public ids to the internal ids used from here on
        mapping = public_to_internal_ids(session, public_url, sorted(ids))
        unmapped = sorted(ids - mapping.keys())
        original_id = {}
        for public_id, internal_id in sorted(mapping.items()):
            original_id.setdefault(internal_id, []).append(public_id)  # sorted: lowest first
    image_ids = sorted(original_id)

    # 2. which of those were imported in place, and where are the files
    images = lookup_images(conn, image_ids)
    not_found = [i for i in image_ids if i not in images]  # can be many: summarized
    if not_found:
        shown = ", ".join(map(str, not_found[:10])) + (", ..." if len(not_found) > 10 else "")
        warn(f"{len(not_found)} images not found or not readable by this user in the internal OMERO; "
             f"skipped (image ids: {shown})")

    in_place_filesets = set()
    not_in_place = {}  # fileset id -> why; warn once per fileset (its images share one cause)
    for image_id, (name, fs_id, transfer) in sorted(images.items()):
        if transfer in IN_PLACE:
            in_place_filesets.add(fs_id)
            continue
        how = transfer.rsplit(".", 1)[-1] if transfer else "not an in-place import"
        if fs_id is not None:
            if fs_id in not_in_place:
                continue  # images from the same fileset share the same files
            not_in_place[fs_id] = how
        warn(f"image {image_id} ({name}) was not imported in place ({how}); skipped")
    sizes, files_of = in_place_files(conn, in_place_filesets)
    conn.close()

    # 3. batch file
    dests, written_bytes, skipped_filesets = write_batch(
        files_of, sizes, args.out_file, src_root, dest_root, args.base_dir, args.flatten)
    skipped = len(unmapped) + len(not_found) + len(not_in_place) + len(skipped_filesets) + sum(
        fs_id is None for _, fs_id, _ in images.values())
    print(f"wrote {len(dests)} files to {args.out_file}; {skipped} skipped (see warnings)")
    if args.report:  # joins every requested fileset with every file; nothing is left out
        rows = []  # (image ids, destination_path, status); no internal paths or ids

        def row(images, dest="", status="transferred"):
            rows.append((sorted(images), str(dest), status))

        for public_id in unmapped:
            row([public_id], status="skipped: no usable origin_image_id")
        for image_id in not_found:
            row(original_id[image_id], status="skipped: image not found or not readable")
        requested_filesets = {}  # fileset id -> its requested image ids (in the input's terms)
        for image_id, (_, fs_id, _) in images.items():
            if fs_id is None:
                row(original_id[image_id], status="skipped: no fileset (not an in-place import)")
            else:
                requested_filesets.setdefault(fs_id, []).extend(original_id[image_id])
        for fs_id, fs_images in requested_filesets.items():
            if fs_id in not_in_place:
                row(fs_images, status=f"skipped: not imported in place ({not_in_place[fs_id]})")
            elif fs_id in skipped_filesets:  # one row for the whole fileset
                row(fs_images, status="skipped: fileset not transferred because "
                                  + skipped_filesets[fs_id])
            elif fs_id not in files_of:
                row(fs_images, status="skipped: no files found for fileset")
            else:  # transferred whole: a row per file
                for path in sorted(files_of[fs_id]):
                    row(fs_images, dests[path])
        with open(args.report, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["image_ids", "destination_path", "status"])
            for ids, dest, status in sorted(rows, key=lambda r: (r[0][0], r[1], r[2])):
                writer.writerow([";".join(map(str, ids)), dest, status])  # first id sorts rows
        print(f"wrote {len(rows)} rows to {args.report}")
    print(f"total size {human_size(written_bytes)}; estimated Globus transfer time "
          f"~{human_time(written_bytes / GLOBUS_BYTES_PER_SECOND)} "
          "(very rough: depends on endpoints, network and file count)")


if __name__ == "__main__":
    main()
