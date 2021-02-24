import os
import pytest
import ezomero
from omero.cli import CLI
from omero.gateway import BlitzGateway
from omero.plugins.sessions import SessionsControl
from omero.plugins.user import UserControl
from omero.plugins.group import GroupControl

# Settings for OMERO
DEFAULT_OMERO_USER = "root"
DEFAULT_OMERO_PASS = "omero"
DEFAULT_OMERO_HOST = "localhost"
DEFAULT_OMERO_PORT = 6064
DEFAULT_OMERO_SECURE = 1

# [group, permissions]
GROUPS_TO_CREATE = [['test_group_1', 'read-only'],
                    ['test_group_2', 'read-only']]

# [user, [groups to be added to], [groups to own]]
USERS_TO_CREATE = [
                   [
                    'test_user1',
                    ['test_group_1', 'test_group_2'],
                    ['test_group_1']
                   ],
                   [
                    'test_user2',
                    ['test_group_1', 'test_group_2'],
                    ['test_group_2']
                   ],
                   [
                    'test_user3',
                    ['test_group_2'],
                    []
                   ]
                  ]


def pytest_addoption(parser):
    parser.addoption("--omero-user", action="store",
        default=os.environ.get("OMERO_USER", DEFAULT_OMERO_USER))
    parser.addoption("--omero-pass", action="store",
        default=os.environ.get("OMERO_PASS", DEFAULT_OMERO_PASS))
    parser.addoption("--omero-host", action="store",
        default=os.environ.get("OMERO_HOST", DEFAULT_OMERO_HOST))
    parser.addoption("--omero-port", action="store", type=int,
        default=int(os.environ.get("OMERO_PORT", DEFAULT_OMERO_PORT)))
    parser.addoption("--omero-secure", action="store",
        default=bool(os.environ.get("OMERO_SECURE", DEFAULT_OMERO_SECURE)))


# we can change this later
@pytest.fixture(scope="session")
def omero_params(request):
    user = request.config.getoption("--omero-user")
    password = request.config.getoption("--omero-pass")
    host = request.config.getoption("--omero-host")
    port = request.config.getoption("--omero-port")
    secure = request.config.getoption("--omero-secure")
    return(user, password, host, port, secure)


@pytest.fixture(scope='session')
def users_groups(conn, omero_params):
    session_uuid = conn.getSession().getUuid().val
    user = omero_params[0]
    host = omero_params[2]
    port = str(omero_params[3])
    cli = CLI()
    cli.register('sessions', SessionsControl, 'test')
    cli.register('user', UserControl, 'test')
    cli.register('group', GroupControl, 'test')

    group_info = []
    for gname, gperms in GROUPS_TO_CREATE:
        cli.invoke(['group', 'add',
                    gname,
                    '--type', gperms,
                    '-k', session_uuid,
                    '-u', user,
                    '-s', host,
                    '-p', port])
        gid = ezomero.get_group_id(conn, gname)
        group_info.append([gname, gid])

    user_info = []
    for user, groups_add, groups_own in USERS_TO_CREATE:
        # make user while adding to first group
        cli.invoke(['user', 'add',
                    user,
                    'test',
                    'tester',
                    '--group-name', groups_add[0],
                    '-e', 'useremail@jax.org',
                    '-P', 'abc123',
                    '-k', session_uuid,
                    '-u', user,
                    '-s', host,
                    '-p', port])

        # add user to rest of groups
        if len(groups_add) > 1:
            for group in groups_add[1:]:
                cli.invoke(['group', 'adduser',
                            '--user-name', user,
                            '--name', group,
                            '-k', session_uuid,
                            '-u', user,
                            '-s', host,
                            '-p', port])

        # make user owner of listed groups
        if len(groups_own) > 0:
            for group in groups_own:
                cli.invoke(['group', 'adduser',
                            '--user-name', user,
                            '--name', group,
                            '--as-owner',
                            '-k', session_uuid,
                            '-u', user,
                            '-s', host,
                            '-p', port])
        uid = ezomero.get_user_id(conn, user)
        user_info.append([user, uid])

    return (group_info, user_info)


@pytest.fixture(scope='session')
def conn(omero_params):
    user, password, host, port, secure = omero_params
    conn = BlitzGateway(user, password, host=host, port=port, secure=secure)
    conn.connect()
    yield conn
    conn.close()
