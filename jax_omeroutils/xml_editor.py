import copy
from os import sep
from collections import defaultdict
from ome_types import to_xml
from ome_types.model import Project, Screen, Dataset, MapAnnotation
from ome_types.model import DatasetRef, AnnotationRef, ImageRef
from ome_types.model.screen import PlateRef
from ome_types.model import CommentAnnotation, Map
from ome_types.model.map import M
import xml.etree.cElementTree as ETree


CURRENT_MD_NS = 'jax.org/omeroutils/user_submitted/v0'


def create_proj(**kwargs):
    proj = Project(**kwargs)
    return proj


def create_screen(**kwargs):
    scr = Screen(**kwargs)
    return scr


def create_dataset_and_ref(**kwargs):
    ds = Dataset(**kwargs)
    ds_ref = DatasetRef(id=ds.id)
    return ds, ds_ref


def create_kv_and_ref(**kwargs):
    kv = MapAnnotation(**kwargs)
    kvref = AnnotationRef(id=kv.id)
    return kv, kvref


def add_projects_datasets(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = imp_json['user_supplied_md']['file_metadata'][0].keys()
    if ('project' not in columns) or ('dataset' not in columns):
        return ome
    else:
        md = imp_json['user_supplied_md']['file_metadata']
        proj_ds = defaultdict(set)
        for i in md:
            proj_ds[i['project']].add(i['dataset'])
        proj_count = 1
        ds_count = 1
        for project in proj_ds.keys():
            id = f"Project:{proj_count}"
            proj_count += 1
            name = project
            proj = create_proj(id=id, name=name)
            for dataset in proj_ds[project]:
                id = f"Dataset:{ds_count}"
                ds_count += 1
                ds, ds_ref = create_dataset_and_ref(id=id, name=dataset)
                newome.datasets.append(ds)
                proj.dataset_ref.append(ds_ref)
            newome.projects.append(proj)
    return newome


def add_screens(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = imp_json['user_supplied_md']['file_metadata'][0].keys()
    if 'screen' not in columns:
        return ome
    else:
        md = imp_json['user_supplied_md']['file_metadata']
        scr_count = 1
        screens = [i['screen'] for i in md]
        for scr in list(set(screens)):
            id = f"Screen:{scr_count}"
            scr_count += 1
            scr_obj = create_screen(id=id, name=scr)
            newome.screens.append(scr_obj)
    return newome


def add_annotations(ome, imp_json):
    columns = list(imp_json['user_supplied_md']['file_metadata'][0].keys())
    if 'project' in columns:
        newome = add_annotations_images(ome, imp_json)
        return newome
    elif 'screen' in columns:
        newome = add_annotations_plates(ome, imp_json)
        return newome
    return ome


def add_annotations_images(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = list(imp_json['user_supplied_md']['file_metadata'][0].keys())
    if 'project' in columns:
        columns.remove('project')
    if 'dataset' in columns:
        columns.remove('dataset')
    md = imp_json['user_supplied_md']['file_metadata']
    max_ann = 0
    for ann in newome.structured_annotations:
        clean_id = int(ann.id.split(":")[-1])
        if clean_id > max_ann:
            max_ann = clean_id
    ann_count = max_ann + 1
    for line in md:
        filename = line['filename']
        ann_dict = {i: line[i] for i in columns}
        ann_dict.pop('filename')
        ann_dict = {k: v for k, v in ann_dict.items() if isinstance(v, str)}
        img_ids = get_image_ids(filename, ome)
        for img in newome.images:
            if img.id in img_ids:
                mmap = []
                for _key, _value in ann_dict.items():
                    if _value:
                        mmap.append(M(k=_key, value=str(_value)))
                    else:
                        mmap.append(M(k=_key, value=''))
                kv, annref = create_kv_and_ref(
                    id=f"Annotation:{ann_count}",
                    namespace=CURRENT_MD_NS,
                    value=Map(ms=mmap))
                ann_count += 1
                newome.structured_annotations.append(kv)
                img.annotation_ref.append(annref)
    return newome


def add_annotations_plates(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = list(imp_json['user_supplied_md']['file_metadata'][0].keys())
    if 'screen' in columns:
        columns.remove('screen')
    md = imp_json['user_supplied_md']['file_metadata']
    max_ann = 0
    for ann in newome.structured_annotations:
        clean_id = int(ann.id.split(":")[-1])
        if clean_id > max_ann:
            max_ann = clean_id
    ann_count = max_ann + 1
    for line in md:
        filename = line['filename']
        ann_dict = {i: line[i] for i in columns}
        ann_dict.pop('filename')
        ann_dict = {k: v for k, v in ann_dict.items() if isinstance(v, str)}
        pl_ids = get_plate_ids(filename, ome)
        for pl in newome.plates:
            if pl.id in pl_ids:
                mmap = []
                for _key, _value in ann_dict.items():
                    if _value:
                        mmap.append(M(k=_key, value=str(_value)))
                    else:
                        mmap.append(M(k=_key, value=''))
                kv, annref = create_kv_and_ref(
                    id=f"Annotation:{ann_count}",
                    namespace=CURRENT_MD_NS,
                    value=Map(ms=mmap))
                ann_count += 1
                newome.structured_annotations.append(kv)
                pl.annotation_ref.append(annref)
    return newome


def move_objects(ome, imp_json):
    columns = list(imp_json['user_supplied_md']['file_metadata'][0].keys())
    if 'project' in columns:
        newome = move_images(ome, imp_json)
        return newome
    elif 'screen' in columns:
        newome = move_plates(ome, imp_json)
        return newome
    return ome


def move_images(ome, imp_json):
    newome = copy.deepcopy(ome)
    md = imp_json['user_supplied_md']['file_metadata']
    for line in md:
        dsname = line['dataset']
        projname = line['project']
        right_ds = []
        for proj in newome.projects:
            if projname == proj.name:
                for dsref in proj.dataset_refs:
                    for ds in newome.datasets:
                        if dsref.id == ds.id and ds.name == dsname:
                            right_ds.append(ds.id)
        images = []
        filename = line['filename']
        ids = get_image_ids(filename, newome)
        for imgid in ids:
            imgref = ImageRef(id=imgid)
            for ds in newome.datasets:
                    if ds.id in right_ds:
                        ds.image_ref.append(imgref)
    return newome


def get_image_ids(filename, ome):
    ann_ids = []
    ids = []
    for an_loop in ome.structured_annotations:
        tree = ETree.fromstring(to_xml(an_loop.value,
                                        canonicalize=True))
        for el in tree:
            if el.tag.rpartition('}')[2] == "CLITransferServerPath":
                for el2 in el:
                    if el2.tag.rpartition('}')[2] == "Path":
                        fpath = el2.text
                        if fpath == filename:
                            ann_ids.append(an_loop.id)
    for img in ome.images:
        for annref in img.annotation_refs:
            if annref.id in ann_ids:
                ids.append(img.id)
    return ids


def get_plate_ids(filename, ome):
    ann_ids = []
    ids = []
    for an_loop in ome.structured_annotations:
        tree = ETree.fromstring(to_xml(an_loop.value,
                                        canonicalize=True))
        for el in tree:
            if el.tag.rpartition('}')[2] == "CLITransferServerPath":
                for el2 in el:
                    if el2.tag.rpartition('}')[2] == "Path":
                        fpath = el2.text
                        if fpath == filename:
                            ann_ids.append(an_loop.id)
    for pl in ome.plates:
        for annref in pl.annotation_refs:
            if annref.id in ann_ids:
                ids.append(pl.id)
    return ids


def move_plates(ome, imp_json):
    newome = copy.deepcopy(ome)
    md = imp_json['user_supplied_md']['file_metadata']
    for line in md:
        scrname = line['screen']
        right_scr = []
        for scr in newome.screens:
            if scrname == scr.name:
                right_scr.append(scr.id)
        plates = []
        filename = line['filename']
        ids = get_plate_ids(filename, newome)
        for plid in ids:
            plref = PlateRef(id=plid)
            for scr in newome.screens:
                    if scr.id in right_scr:
                        scr.plate_refs.append(plref)
    return newome
