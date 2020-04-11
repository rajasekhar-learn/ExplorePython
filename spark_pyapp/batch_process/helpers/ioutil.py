from urllib import request
import subprocess


def run_shell_cmd(args_list):
    """
    run shell commands
    """
    print('Running shell command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def download_data(url, location):
    request.urlretrieve(url, location)


def put_file_in_hdfs(local_path, hdfs_location):
    ret, out, err = run_shell_cmd(['hdfs', 'dfs', '-put', local_path, hdfs_location])
    print(ret, out, err)
    if ret:
        print('file does not exist')
        raise ValueError('invalid file object !!')
