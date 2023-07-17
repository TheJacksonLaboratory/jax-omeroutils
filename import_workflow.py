import os
import subprocess
import argparse
import pwd
import sys
import grp
import pathlib
from datetime import datetime
from jax_omeroutils.config import OMERO_USER, OMERO_PASS
from jax_omeroutils.config import OMERO_HOST, OMERO_PORT


def demote(user_uid, user_gid, homedir):
    def result():
        os.setgid(user_gid)
        os.setuid(user_uid)
        os.environ["HOME"] = homedir
    return result


def retrieve_json(stdoutval):
    if (not stdoutval):
        return None
    last_line = stdoutval.split('\n')[-2]
    json_path = last_line.split(':')[-1].strip()
    return json_path


def retrieve_fileset(stdoutval, target):
    lines = stdoutval.split('\n')
    files = [i for i in lines if ((not i.startswith('#')) and (i != ''))]
    filelist_path = pathlib.Path(target) / 'moved_files.txt'
    with open(filelist_path, 'w') as f:
        f.write("\n".join(files))
        f.close()
    return filelist_path

def edit_xml(target):
    return


def main(target, datauser, omerouser, logdir):

    # Data user info
    data_user_uid = pwd.getpwnam(datauser).pw_uid
    data_user_gid = grp.getgrnam('omeroadmin').gr_gid
    data_user_home = f"/home/{datauser}"

    # Omero user info
    omero_user_uid = pwd.getpwnam(omerouser).pw_uid
    omero_user_gid = data_user_gid
    omero_user_home = f"/home/{omerouser}"

    curr_folder = os.path.abspath(os.path.dirname(__file__))

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Run prepare_batch.py
    prepbatch = [sys.executable, curr_folder + '/prepare_batch.py',
                 target, logdir, '--timestamp', timestamp]
    process = subprocess.Popen(prepbatch,
                               preexec_fn=demote(data_user_uid,
                                                 data_user_gid,
                                                 data_user_home),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE
                               )
    stdoutval, stderrval = process.communicate()
    stdoutval, stderrval = stdoutval.decode('UTF-8'), stderrval.decode('UTF-8')
    print("stdout prep:", stdoutval)
    print("stderr prep:", stderrval)
    fileset_list = retrieve_fileset(stdoutval, target)

    # Run omero transfer prepare

    filelist = str(pathlib.Path(target) / 'filelist.txt')
    prepare = ['omero', '-s', OMERO_HOST, '-p', str(OMERO_PORT),
               '-u', OMERO_USER, '-w', OMERO_PASS,
               'transfer', 'prepare', '--filelist', filelist,]
    process = subprocess.Popen(prepare,
                               preexec_fn=demote(data_user_uid,
                                                 data_user_gid,
                                                 data_user_home),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE
                               )
    stdoutval, stderrval = process.communicate()
    stdoutval, stderrval = stdoutval.decode('UTF-8'), stderrval.decode('UTF-8')
    print("stdout prepare:", stdoutval)
    print("stderr prepare:", stderrval)

    edit_xml(target)

    # Move data

    datamove = [sys.executable, curr_folder + '/move_data.py',
                target, fileset_list, logdir, '--timestamp', timestamp]
    process = subprocess.Popen(datamove,
                               preexec_fn=demote(data_user_uid,
                                                 data_user_gid,
                                                 data_user_home),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE
                               )
    stdoutval, stderrval = process.communicate()
    stdoutval, stderrval = stdoutval.decode('UTF-8'), stderrval.decode('UTF-8')
    json_path = retrieve_json(stdoutval)
    print("stdout move:", stdoutval)
    print("stderr move:", stderrval)
    if json_path and pathlib.Path(json_path).exists():
        print(f'json path will be {json_path}')
        out_path = pathlib.Path(json_path).parent / (timestamp + ".out")
        err_path = pathlib.Path(json_path).parent / (timestamp + ".err")
        with open(out_path, 'w+') as fp:
            fp.write(stdoutval)
        fp.close()
        with open(err_path, 'w+') as fp:
            fp.write(stderrval)
        fp.close()

        # Run import_annotate_batch.py
        impbatch = [sys.executable, curr_folder + '/import_annotate_batch.py',
                    json_path]
        process = subprocess.Popen(impbatch,
                                   preexec_fn=demote(omero_user_uid,
                                                     omero_user_gid,
                                                     omero_user_home),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdoutval, stderrval = process.communicate()
        stdoutval, stderrval = stdoutval.decode('UTF-8'), \
            stderrval.decode('UTF-8')
        print("stdout import:", stdoutval)
        print("stderr import:", stderrval)
        with open(out_path, 'a') as fp:
            fp.write(stdoutval)
        fp.close()
        with open(err_path, 'a') as fp:
            fp.write(stderrval)
        fp.close()


if __name__ == '__main__':
    description = "One-command in-place importing sript"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('target',
                        type=str,
                        help='Target folder to be imported')
    parser.add_argument('--datauser',
                        type=str,
                        help='System username for the data user',
                        default='svc-omerodata')
    parser.add_argument('--omerouser',
                        type=str,
                        help='System username for the omero user',
                        default='svc-omero')
    parser.add_argument('--logdir',
                        type=str,
                        help='Directory for the log files',
                        default='/tmp/cron_logs')
    args = parser.parse_args()
    main(args.target, args.datauser, args.omerouser, args.logdir)
