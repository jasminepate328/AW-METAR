from utils.helper_functions import *

from paramiko import SSHClient, AutoAddPolicy

def main():
    args = parse_args()
    params = get_parameters(['dns', 'work_bucket'])

    submit_job(params['dns'], 'hadoop', args.key_pair, params['work_bucket'])

def submit_job(dns, username, key_pair, work_bucket, file):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    ssh.connect(hostname=dns, username=username, key_filename=key_pair)

    for i in ['opensky', 'noaa']:
        file_name = '{i}_convert_to_parquet.py'
        _, stdout, stderr = ssh.exec_command(
            command=f"""
                spark-submit --deploy-mode cluster --master yarn --conf 
                spark.yarn.submit.waitAppCompletion=true \
                    s3a://{work_bucket}/analyze/{file_name}"""
    )
        stdout_lines = ''
        
        while not stdout.channel.exit_status_ready():
            if stdout.channel.recv_ready():
                stdout_lines = stdout.readlines()
        logging.info(' '.join(map(str, stdout_lines)))

        ssh.close()
