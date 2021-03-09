import json
import pytest
from pathlib import Path
from jax_omeroutils import intake
from math import nan
from copy import deepcopy


@pytest.fixture(scope='session')
def md_dict():
    md = {}
    md['omero_user'] = 'testuser'
    md['omero_group'] = 'testgroup'
    md['file_metadata'] = []
    for i in range(5):
        md['file_metadata'].append({'filename': f'name{i}',
                                    'project': 'proj',
                                    'dataset': 'ds'})
    return md


# load_md
def test_load_md_invalid_ftypes(tmp_path):
    open(tmp_path / 'md2.xls.5', 'a').close()
    with pytest.raises(ValueError):
        intake.load_md_from_file(tmp_path / 'md2.xls.5')


def test_load_md_nofile(tmp_path):
    with pytest.raises(FileNotFoundError):
        intake.load_md_from_file(tmp_path / 'md.xlsx')


def test_load_md_fp_isNone():
    assert intake.load_md_from_file(None) is None


def test_load_md_good():
    # This uses an actual file for testing
    md_path = Path('./jax_omeroutils/tests/data/OMERO_submission_form.xlsx')
    md = intake.load_md_from_file(md_path, sheet_name="Example Form")
    assert md['omero_user'] == 'djme'
    assert md['omero_group'] == 'Research IT'
    assert md['file_metadata'][0]['filename'] == 'my_image.tif'


# find_md
def test_find_md_good(tmp_path):
    open(tmp_path / 'md.xlsx', 'a').close()
    md = intake.find_md_file(tmp_path)
    assert md == tmp_path / 'md.xlsx'


def test_find_md_no_md(tmp_path):
    md = intake.find_md_file(tmp_path)
    assert md is None


def test_find_md_too_many(tmp_path):
    open(tmp_path / 'md.xlsx', 'a').close()
    open(tmp_path / 'md2.xlsx', 'a').close()
    md = intake.find_md_file(tmp_path)
    assert md is None


# ImportBatch
def test_ImportBatch_load_md(conn):
    import_path = Path('./jax_omeroutils/tests/data')
    batch = intake.ImportBatch(conn, import_path)
    assert batch.md is None
    batch.load_md(sheet_name="Example Form")
    assert batch.md['omero_user'] == 'djme'
    assert batch.md['omero_group'] == 'Research IT'
    assert batch.md['file_metadata'][0]['filename'] == 'my_image.tif'


def test_ImportBatch_validate_ug(conn, users_groups):
    batch = intake.ImportBatch(conn, 'test')
    check = batch.validate_user_group(user='test_user1', group='test_group_1')
    assert check
    assert batch.user == 'test_user1'
    assert batch.group == 'test_group_1'
    assert batch.user_email == 'useremail@jax.org'
    with pytest.raises(ValueError):
        batch.validate_user_group(user='test_user1', group='bad_group')
    with pytest.raises(ValueError):
        batch.validate_user_group(user='bad_user', group='test_group_1')


def test_set_server_path():
    batch = intake.ImportBatch('test', 'test')
    batch.user = 'user'
    batch.group = 'Group Name'
    batch.set_server_path()
    group_path = Path('group_name')
    base = intake.BASE_SERVER_PATH
    assert batch.server_path.parents[0] == base / group_path
    assert batch.server_path.name.split('_')[0] == batch.user


def test_validate_import_md_good(md_dict):
    batch = intake.ImportBatch('test', 'test')
    batch.md = deepcopy(md_dict)
    assert batch.validate_import_md()


def test_validate_import_md_filename_errors(md_dict):
    # Check filename errors
    batch = intake.ImportBatch('test', 'test')
    batch.md = deepcopy(md_dict)
    batch.md['file_metadata'][0]['filename'] = nan
    assert not batch.validate_import_md()
    batch.md['file_metadata'][0].pop('filename')
    assert not batch.validate_import_md()


def test_validate_import_md_project_errors(md_dict):
    # Check project error
    batch = intake.ImportBatch('test', 'test')
    batch.md = deepcopy(md_dict)
    batch.md['file_metadata'][0]['project'] = nan
    assert not batch.validate_import_md()
    batch.md['file_metadata'][0].pop('project')
    assert not batch.validate_import_md()


def test_validate_import_md_dataset_errors(md_dict):
    # Check dataset error
    batch = intake.ImportBatch('test', 'test')
    batch.md = deepcopy(md_dict)
    batch.md['file_metadata'][0]['dataset'] = nan
    assert not batch.validate_import_md()
    batch.md['file_metadata'][0].pop('dataset')
    assert not batch.validate_import_md()


def test_validate_import_md_duplicate_fn(md_dict):
    batch = intake.ImportBatch('test', 'test')
    batch.md = deepcopy(md_dict)
    batch.md['file_metadata'].append(batch.md['file_metadata'][0])
    assert not batch.validate_import_md()


def test_load_targets(md_dict):
    batch = intake.ImportBatch('noconn', './jax_omeroutils/tests/data/')
    batch.md = deepcopy(md_dict)
    batch.md['file_metadata'][0]['filename'] = 'valid_test.tif'
    batch.md['file_metadata'][1]['filename'] = 'invalid_test.tif'
    assert len(batch.import_target_list) == 0
    batch.load_targets()
    assert len(batch.import_target_list) == 1


def test_write_json(md_dict, tmp_path):
    batch = intake.ImportBatch('noconn', tmp_path)
    batch.md = deepcopy(md_dict)
    batch.user = 'testuser'
    batch.group = 'testgroup'
    batch.user_email = 'email@email.com'
    batch.server_path = '/server_path'
    batch.valid_md = True
    batch.import_target_list = []
    for md_entry in batch.md['file_metadata']:
        import_target = intake.ImportTarget(tmp_path, md_entry)
        batch.import_target_list.append(import_target)
    batch.write_json()
    with open(tmp_path / 'import.json', 'r') as fp:
        test_json = json.load(fp)
    assert test_json['user'] == 'testuser'
    assert test_json['import_targets'][0]['project'] == 'proj'


# ImportTarget
def test_import_target():
    md = {}
    md['filename'] = 'valid_test.tif'
    imp_target = intake.ImportTarget(Path('./jax_omeroutils/tests/data/'), md)
    assert imp_target.exists
    imp_target.validate_target()
    assert imp_target.valid_target
