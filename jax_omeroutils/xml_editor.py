import copy
from collections import defaultdict
from ome_types.model import Project, Screen, Dataset, MapAnnotation
from ome_types.model import DatasetRef, AnnotationRef


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
    if 'project' not in columns:
        return ome
    else:
        md = imp_json['user_supplied_md']['file_metadata']
        proj_ds = defaultdict(list)
        for i in md:
            proj_ds[i['project']].append(i['dataset'])
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
        print(columns)
    return newome


def add_annotations(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = list(imp_json['user_supplied_md']['file_metadata'][0].keys())
    if 'project' in columns:
        columns.remove('project')
    if 'dataset' in columns:
        columns.remove('dataset')
    if 'screen' in columns:
        columns.remove('screen')
    print(columns)
    return newome
