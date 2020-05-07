import os
import pytest
import pandas as pd
import prepare_import_batch
from jax_omeroutils import intake


# helper functions
def _tmp_builder(temp_dir, nfiles):

    # Make a bunch of "image files"
    for i in range(nfiles):
        newfile = temp_dir / f'image{i}.tif'
        open(newfile, 'a').close()

    # Make metadata file
    d = {'filename': [f'image{i}.tif' for i in range(5)],
         'project': ['testproject'] * 5,
         'dataset': ['testdataset'] * 5,
         'species': ['mouse', 'mouse', 'human', 'human', 'D. melanogaster'],
         'genotype': ['WT', 'C57Bl6', 'another', 'another', 'another'],
         'tissue_cell_type': ['a', 'b', 'c', 'd', 'e'],
         'antibodies_stains': ['1', '2', '3', '4', '5']
         }
    df = pd.DataFrame(data=d)
    fp = temp_dir / 'md.tsv'
    df.to_csv(fp, sep='\t')
    return temp_dir


# load_md
def test_load_md_invalid_ftypes():
    with pytest.raises(ValueError):
        intake.load_md_from_file('/path/to/junk.xls.5')


def test_load_md_fp_isNone():
    assert intake.load_md_from_file(None) is None


def test_load_md_good(tmp_path):
    md_path = _tmp_builder(tmp_path, 5) / 'md.tsv'
    md = intake.load_md_from_file(md_path)
    assert md.filename[0] == 'image0.tif'
    assert md.project[0] == 'testproject'
    assert md.genotype[1] == 'C57Bl6'


# find_md
def test_find_md_good(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    md = intake.find_md_file(d)
    assert md == str(d / 'md.tsv')


def test_find_md_no_md(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    os.remove(d / 'md.tsv')
    md = intake.find_md_file(d)
    assert md is None


def test_find_md_too_many(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    open(d / 'md.xls', 'a').close()
    md = intake.find_md_file(d)
    assert md is None


# ImportBatch
def test_ImportBatch_init(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    assert batch.user == 'djme'
    assert batch.md.filename[0] == 'image0.tif'


def test_ImportBatch_valid(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    valid = batch.validate_batch()
    assert valid is True
    assert batch.valid_batch is True


def test_ImportBatch_too_few_files(tmp_path):
    d = _tmp_builder(tmp_path, 3)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    valid = batch.validate_batch()
    assert valid is False
    assert batch.valid_batch is False


def test_ImportBatch_too_many_files(tmp_path):
    d = _tmp_builder(tmp_path, 7)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    batch.validate_batch()
    assert batch.valid_batch is False


def test_ImportBatch_missing_filename(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    del batch.md['filename']
    batch.validate_batch()
    assert batch.valid_batch is False


def test_ImportBatch_missing_other(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    del batch.md['dataset']
    del batch.md['project']
    batch.md.iloc[3, 3] = None
    batch.validate_batch()
    assert batch.valid_batch is False


def test_ImportBatch_no_metadata(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    os.remove(d / 'md.tsv')
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    batch.validate_batch()
    assert batch.valid_batch is False


def test_ImportBatch_write_files(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    batch = intake.ImportBatch(d, 'djme', 'research_it')
    batch.write_files()
    assert not os.path.exists(tmp_path / 'files.tsv')
    batch.validate_batch()
    batch.write_files()
    assert not os.path.exists(tmp_path / 'files.tsv')
    batch.set_target_path('/path/to/target/dir')
    batch.write_files()
    assert os.path.exists(tmp_path / 'files.tsv')
    assert os.path.exists(tmp_path / 'import_md.json')
    assert os.path.exists(tmp_path / 'import.yml')


# Test scripts
def test_prepare_import_batch_py(tmp_path):
    d = _tmp_builder(tmp_path, 5)
    prepare_import_batch.main(d, '/test/path', 'djme', 'rit')
    assert os.path.exists(tmp_path / 'files.tsv')
    assert os.path.exists(tmp_path / 'import_md.json')
    assert os.path.exists(tmp_path / 'import.yml')


def test_prepare_import_batch_py_wproblem(tmp_path):
    d = _tmp_builder(tmp_path, 4)
    prepare_import_batch.main(d, '/test/path', 'djme', 'rit')
    assert not os.path.exists(tmp_path / 'files.tsv')
