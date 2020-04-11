import json
import pandas as pd
from omeroutils import filewriter


def _make_md():
    d = {"filename": [f'test{n}.tif' for n in range(5)],
         "project": ["project 1", "project 2", "A", "B", "C"],
         "dataset": ["new data", "c", "MY-Project", "proj", "X"],
         'species': ['mouse', 'mouse', 'human', 'human', 'D. melanogaster'],
         'genotype': ['WT', 'C57Bl6', 'another', 'another', 'another'],
         'tissue_cell_type': ['a', 'b', 'c', 'd', 'e'],
         'antibodies_stains': ['1', '2', '3', '4', '5']}
    md = pd.DataFrame(d)
    return md


def test_write_files_tsv(tmp_path):
    md = _make_md()
    fp = filewriter.write_files_tsv(md, str(tmp_path), '/hyperfile/omero/test')
    fp2 = tmp_path / "files.tsv"
    assert fp == str(fp2)
    md2 = pd.read_csv(fp, sep='\t', header=None)
    assert md2.iloc[0, 0] == ('Project:name:project 1/'
                              'Dataset:name:new data')
    assert md2.iloc[0, 1] == '/hyperfile/omero/test/test0.tif'


def test_write_import_yml(tmp_path):
    fp = filewriter.write_import_yml(str(tmp_path), '/hyperfile/omero/test')
    with open(fp, 'r') as f:
        lines = f.readlines()
    assert lines[1] == 'path: \"/hyperfile/omero/test/files.tsv\"\n'
    assert fp == str(tmp_path / 'import.yml')


def test_write_import_md_json(tmp_path):
    md = _make_md()
    fp = filewriter.write_import_md_json(md, str(tmp_path),
                                         'admin', "Research IT")
    md_fp = tmp_path / 'import_md.json'
    assert str(md_fp) == fp
    with open(md_fp, 'r') as f:
        md2 = json.load(f)
    assert md2['OMERO_group'] == 'Research IT'
    assert len(md2['data']) == len(md['filename'])
    filenames = [x['filename'] for x in md2['data']]
    assert filenames == list(md['filename'])
