import json
import shutil
from jax_omeroutils import datamover
from pathlib import Path


def test_calculatemd5():
    known_hash = '2fee729b57b5c0f6fe34f3a63ee53964'
    test_data = Path('./jax_omeroutils/tests/data/valid_test.tif')
    assert known_hash == datamover.calculate_md5(test_data)


def test_file_mover(tmp_path):
    test_data = Path('./jax_omeroutils/tests/data/valid_test.tif')
    test_data_temp = tmp_path / 'testdir'
    test_data_temp.mkdir()
    shutil.copy2(test_data, test_data_temp)
    result = datamover.file_mover(test_data_temp / test_data.name, tmp_path)
    expected_path = str(tmp_path / test_data.name)
    assert result == expected_path


def test_DataMover(tmp_path):
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    dest_dir = tmp_path / 'dest'
    dest_dir.mkdir()
    test_data = Path('./jax_omeroutils/tests/data/valid_test.tif')
    test_data2 = Path('./jax_omeroutils/tests/data/invalid_test.tif')
    shutil.copy2(test_data, src_dir)
    shutil.copy2(test_data2, src_dir)

    import_json = {"import_path": str(src_dir),
                   "server_path": str(dest_dir),
                   "import_targets": [{"filename": "valid_test.tif"},
                                      {"filename": "invalid_test.tif"}]}
    json_fp = src_dir / 'import.json'
    with open(json_fp, 'w') as fp:
        json.dump(import_json, fp)
    mover = datamover.DataMover(json_fp)
    assert mover.import_json["import_path"] == str(src_dir)

    mover.move_data()
    assert (dest_dir / 'valid_test.tif').exists()
    assert (dest_dir / 'invalid_test.tif').exists()
    assert (dest_dir / 'import.json').exists()
