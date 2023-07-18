import copy


def add_projects_datasets(ome, imp_json):
    newome = copy.deepcopy(ome)
    columns = imp_json['user_supplied_md']['file_metadata'][0].keys()
    if 'project' not in columns:
        return ome
    else:
        print(columns)
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
    columns = imp_json['user_supplied_md']['file_metadata'][0].keys()
    if 'project' in columns:
        columns.remove('project')
    if 'dataset' in columns:
        columns.remove('dataset')
    if 'screen' in columns:
        columns.remove('screen')
    return newome
