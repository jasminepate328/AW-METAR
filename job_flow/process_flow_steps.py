from utils.helper_functions import *
import os 
import json

def main():
    args = parse_args()
    params = get_parameters()
    steps = get_steps(params, args.job_type)

    add_job_flow_steps(params['cluster_id'], steps)

def add_job_flow_steps(cluster_id, steps):
    try:
        emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=steps
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_steps(params, job_type):
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    file = open(f'{dir_path}/job_flow/{job_type}_steps.json', 'r')

    steps = json.load(file)
    new_steps = []

    for step in steps:
        step['HadoopJarStep']["Args"] = list(
            map(lambda st: str.replace(st, '{{ work_bucket }}',
            params['work_bucket']), step['HadoopJarStep']['Args']))
        new_steps.append(step)
    return new_steps

def start_crawler(crawler_name):
    try:
        glue_client.start_crawler(Name=crawler_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == '__main__':
    main()
