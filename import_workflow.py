import os
import subprocess
import argparse
import pwd
import sys
import grp
import pathlib
import logging
from datetime import datetime


def demote(user_uid, user_gid, homedir):
    def result():
        os.setgid(user_gid)
        os.setuid(user_uid)
        os.environ["HOME"] = homedir
    return result


def retrieve_json(stdoutval):
    last_line = stdoutval.split('\n')[-2]
    if (last_line == "\n"):
        print("empty last line")
    json_path = last_line.split(':')[-1].strip()
    return json_path


def main(target, datauser, omerouser):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logfile = pathlib.Path(target) / pathlib.Path(f'{timestamp}.log')
    print("log file path:", logfile)
    logging.basicConfig(filename=logfile, level=logging.DEBUG)

    # Data user info
    data_user_uid = pwd.getpwnam(datauser).pw_uid
    data_user_gid = grp.getgrnam('omeroadmin').gr_gid
    data_user_home = f"/home/{datauser}"

    # Omero user info
    omero_user_uid = pwd.getpwnam(omerouser).pw_uid
    omero_user_gid = data_user_gid
    omero_user_home = f"/home/{omerouser}"

    curr_folder = os.path.abspath(os.path.dirname(__file__))

    # Run prepare_batch.py
    prepbatch = [sys.executable, curr_folder + '/prepare_batch.py', target]
    process = subprocess.Popen(prepbatch,
                               preexec_fn=demote(data_user_uid,
                                                 data_user_gid,
                                                 data_user_home),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdoutval, stderrval = process.communicate()
    stdoutval, stderrval = stdoutval.decode('UTF-8'), stderrval.decode('UTF-8')
    print("stdout:",stdoutval)
    print("stderr:",stderrval)
    json_path = retrieve_json(stdoutval)
    print(f'json path will be {json_path}')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = pathlib.Path(json_path).parent / timestamp / ".out"
    err_path = pathlib.Path(json_path).parent / timestamp / ".err"
    with open(out_path, 'w') as fp:
        fp.write(stdoutval)
    fp.close()
    with open(err_path, 'w') as fp:
        fp.write(stderrval)
    fp.close()

    # Run import_annotate_batch.py
    impbatch = [sys.executable, curr_folder + '/import_annotate_batch.py', json_path]
    process = subprocess.Popen(impbatch,
                               preexec_fn=demote(omero_user_uid,
                                                 omero_user_gid,
                                                 omero_user_home),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdoutval, stderrval = process.communicate()
    stdoutval, stderrval = stdoutval.decode('UTF-8'), stderrval.decode('UTF-8')
    print(stdoutval, stderrval)
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
    args = parser.parse_args()
    main(args.target, args.datauser, args.omerouser)
