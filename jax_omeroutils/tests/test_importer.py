# from jax_omeroutils import importer
# from pathlib import Path


'''
These tests are on hold.
In-place imports need to be run as a user with write permissions to the
Managed Repository.
'''

# def test_importer(conn, timestamp, users_groups, omero_params):
#     host = omero_params[2]
#     port = omero_params[3]
#     group_info, user_info = users_groups
#     username = user_info[0][0]
#     group = group_info[0][0]
#     suconn = conn.suConn(username, group, ttl=600000)
#     import_md = {'filename': 'valid_test.tif',
#                  'project': f'importer_test{timestamp}',
#                  'dataset': f'importer_testds{timestamp}',
#                  'test_key1': 'test_value1',
#                  'test_key2': 'test_value2'}
#     filepath = Path('./jax_omeroutils/tests/data/valid_tests.tif')
#     imp_ctl = importer.Importer(suconn, filepath, import_md)
#     assert imp_ctl.import_ln_s(host, port)
