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
# requests = "*"
# ///

import argparse
import getpass
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import PurePosixPath

import requests
from omero.gateway import BlitzGateway
from omero.constants.namespaces import NSFILETRANSFER
from omero.sys import ParametersI

# Images on the public OMERO were copied from the internal OMERO with
# omero-cli-transfer, which records the internal image id in a map annotation.
ORIGIN_NS = "openmicroscopy.org/cli/transfer"
ORIGIN_KEY = "origin_image_id"

DEFAULT_PUBLIC_URL = "https://images.jax.org"
PAGE_SIZE = 500          # maxLimit of the public JSON API
ANNOTATION_CHUNK = 100   # image ids per annotation request (500 is too long a URL)
INTERNAL_CHUNK = 1000    # image ids per internal HQL query
THREADS = 8

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
    """A public Project/Dataset/Image:<id>, or a text file of such lines."""
    parsed = parse_object(value)
    if parsed:
        return parsed
    if os.path.isfile(value):
        return "File", value
    raise argparse.ArgumentTypeError(
        "must be Project:<id>, Dataset:<id>, Image:<id>, or the path of an existing "
        "text file of such lines")


def read_objects(path):
    """Public objects listed in a text file, one per line, e.g. Image:123.

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


# ---- public OMERO (no authentication) ---------------------------------------

def public_get(session, url, params=None):
    """GET a JSON document from the public server; None if it is a 404."""
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
    """All items of a paginated JSON API listing."""
    items = []
    while True:
        page = public_get(session, url, {"limit": PAGE_SIZE, "offset": len(items)})
        items += page["data"]
        if not page["data"] or len(items) >= page["meta"]["totalCount"]:
            return items


def public_image_ids(session, public_url, obj_type, obj_id):
    """Ids of every image in a public project or dataset (or just the image)."""
    api = f"{public_url}/api/v0/m"
    if obj_type == "Image":
        # not checked one by one (a file can list hundreds); images that do not
        # exist are reported by the batched annotation lookup instead
        return [obj_id]
    kind = f"{obj_type.lower()}s"
    if public_get(session, f"{api}/{kind}/{obj_id}/") is None:
        sys.exit(f"{obj_type} {obj_id} not found on {public_url}")
    if obj_type == "Project":
        dataset_ids = [d["@id"] for d in public_list(session, f"{api}/projects/{obj_id}/datasets/")]
    else:
        dataset_ids = [obj_id]
    with ThreadPoolExecutor(THREADS) as pool:
        listings = pool.map(
            lambda d: public_list(session, f"{api}/datasets/{d}/images/"), dataset_ids)
        # a set: an image can be in more than one dataset
        return sorted({image["@id"] for images in listings for image in images})


def origin_image_ids(session, public_url, public_ids):
    """Map public image id -> internal image id, from the transfer annotation."""
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

    chunks = [public_ids[i:i + ANNOTATION_CHUNK]
              for i in range(0, len(public_ids), ANNOTATION_CHUNK)]
    origins = {}
    with ThreadPoolExecutor(THREADS) as pool:
        for found in pool.map(fetch, chunks):
            origins.update(found)

    mapping = {}
    missing = []
    for public_id in public_ids:
        values = origins.get(public_id)
        if not values:
            missing.append(public_id)  # can be thousands, so summarized below
            continue
        if len(values) > 1:
            warn(f"public image {public_id} has several {ORIGIN_KEY} values "
                 f"{sorted(values)}; skipped")
            continue
        value = next(iter(values))
        if not value.isdigit():
            warn(f"public image {public_id} has a non-numeric {ORIGIN_KEY} "
                 f"({value!r}); skipped")
            continue
        mapping[public_id] = int(value)
    if missing:
        shown = ", ".join(map(str, missing[:10])) + (", ..." if len(missing) > 10 else "")
        warn(f"{len(missing)} public images have no {ORIGIN_KEY} annotation in "
             f"{ORIGIN_NS} (or do not exist / are not public); skipped "
             f"(public image ids: {shown})")
    return mapping


# ---- internal OMERO -----------------------------------------------------------

def connect_internal():
    omero_user = input("Internal OMERO user: ")
    omero_host = os.environ.get("OMERO_HOST") or input("Internal OMERO host: ")
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


def internal_images(conn, image_ids):
    """image id -> (name, fileset id, transfer type) for readable images.

    One HQL query per chunk of ids, instead of several gateway round trips per
    image. Left joins keep images with no fileset (pre-5.0 imports) and
    filesets with no file-transfer annotation (copy/move uploads); both come
    back with transfer type None.
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
                    "in-place imported images, starting from a project or dataset on "
                    "the PUBLIC OMERO (no login needed), or from a text file listing "
                    "several such objects (Image:, Dataset: or Project: lines). "
                    "Each public image's "
                    f"'{ORIGIN_KEY}' annotation (namespace {ORIGIN_NS}) gives the "
                    "image id on the internal OMERO, which is then looked up there. "
                    "The internal OMERO server is read from the OMERO_HOST "
                    "environment variable (or prompted for).",
        epilog="e.g.  pixi run --script make_globus_batch_public.py Dataset:4251 "
               "/data/omero/lab1 /shares/my-share globus_batch.txt   (or give "
               "objects.txt instead of Dataset:4251)",
    )
    parser.add_argument("target", type=target,
                        metavar="<Project|Dataset|Image>:<id> | <objects file>",
                        help="object on the PUBLIC OMERO, same syntax as the OMERO CLI "
                             "(e.g. Dataset:4251), or a text file with one such object "
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
    parser.add_argument("--public-url", default=DEFAULT_PUBLIC_URL,
                        help=f"public OMERO web address (default {DEFAULT_PUBLIC_URL})")
    args = parser.parse_args(argv)
    (obj_type, obj_id), src_root, dest_root = args.target, args.src_root, args.dest_root
    public_url = args.public_url.rstrip("/")

    # 1. public OMERO: image ids in the project/dataset -> internal image ids
    session = requests.Session()
    if obj_type == "File":  # obj_id is then the path of a file of objects
        objects = read_objects(obj_id)
        print(f"read {len(objects)} objects from {obj_id}")
    else:
        objects = [(obj_type, obj_id)]
    ids = set()  # a set: objects can overlap (an image can be in several datasets)
    for kind, number in objects:
        found = public_image_ids(session, public_url, kind, number)
        if kind != "Image":
            print(f"public {kind} {number} has {len(found)} images")
        ids.update(found)
    public_ids = sorted(ids)
    if len(objects) > 1:
        print(f"{len(public_ids)} distinct public images in total")
    origin_for = origin_image_ids(session, public_url, public_ids)
    skipped = len(public_ids) - len(origin_for)
    internal_ids = sorted(set(origin_for.values()))
    public_for = {internal: public for public, internal in sorted(origin_for.items())}
    print(f"{len(internal_ids)} of them map to internal OMERO images")
    if not internal_ids:
        sys.exit("Nothing to transfer: no public image has an internal image id.")

    # 2. internal OMERO: which of those were imported in place, and where are the files
    conn = connect_internal()
    images = internal_images(conn, internal_ids)
    not_found = [i for i in internal_ids if i not in images]  # can be many: summarized
    if not_found:
        shown = ", ".join(f"{i} (public {public_for[i]})" for i in not_found[:5])
        warn(f"{len(not_found)} internal images not found or not readable by this "
             f"user; skipped (e.g. {shown}{', ...' if len(not_found) > 5 else ''})")
        skipped += len(not_found)

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
        warn(f"internal image {image_id} ({name}) was not imported in place "
             f"({how}); skipped")
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
