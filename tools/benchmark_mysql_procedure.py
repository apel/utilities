#!/bin/env python
"""
This script was from an experiment to test the JoinJobRecords MySQL procedure.

Run this script to progressively generate, load, and process records to
time-benchmark each step.

This script uses the scripts in mysql_bin to operate with variable privileges.
"""

import os
import sys
import shutil
import time
import datetime
import logging
import pathlib
import subprocess

import numpy as np

USAGE=f"""{sys.argv[0]} [procedure n]

procedure <n> is simply the procedure name followed by a number which allows
a basic level of versioning while keeping old procedures in the database.

This script uses the scripts in mysql_bin to operate with variable privileges.
"""

bin = 'mysql_bin'

# In place of a config file, change these as required.
config = {
        'experiment_dir': './tests',
        'log_file': './tests/log',
        'results_file': './results.csv', # append outputs to this file as well as each subdir in tests
        #'n_records_start': 100,
        #'n_records_stop': 1000,
        #'n_steps': 10,
        'n_repeat': 1, # repeat each step to improve time estimate
        'schedule': [10,100,200,500,1000], # <list>/linear/exponential/power10
        #'schedule': [10,100,200], # <list>/linear/exponential/power10
        'preload':True, # repeat schedule starting with preloaded values
        #'preload_schedule':[100, 1000, 10000, 50000, 100000],
        'preload_schedule':[100, 1000],
        #'preload_schedule':[100, 1000],
        #'preload_scale':[1, 2, 3, 4],
        'results_key': [ 'procedure_time',
                         'num_records_generated', 
                         'num_blahd_pre', 'num_blahd_post',
                         'num_event_pre', 'num_event_post',
                         'num_job_pre', 'num_job_post',
                         ],
        'delete_old_records':False, # After each run, dump the contents of the table.
        'call_procedure_n':1, # fix which procedure to call.
        'load_procedure':False, # load the procedure newly from the procedure file.
}

def get_datetime():
    return datetime.datetime.now()

def mkschedule():

    method = config['schedule']
    repeat = config['n_repeat']

    if type(method) is list:
        sch = np.array(method).repeat(repeat)
    else:
        start = config['n_records_start']

        if 'n_steps' in config.keys():
            n_steps = config['n_steps']

        if 'n_records_stop' in config.keys():
            stop = config['n_records_stop']
            rng = start - stop

        if method == 'exponential':
            sch = np.array([start+np.power(rng, i/(n_steps-1))-1 for i in np.arange(n_steps)], dtype=int).repeat(r)
        elif method == 'linear':
            sch = np.array([start+rng*(i)/(n_steps-1) for i in np.arange(n_steps)], dtype=int).repeat(r)
        elif method == 'power10':
            sch = np.array([start+np.power(10, i) for i in np.arange(n_steps)], dtype=int).repeat(r)

    if config['preload']:
        pre = np.array(config['preload_schedule'])
        n_p = np.size(pre)

        k = sch[np.newaxis, :].repeat(n_p, axis=0)
        
        if 'preload_scale' in config.keys():
            s = np.array(config['preload_scale'])
            k = k*s[:, np.newaxis]

        j = (pre[:, np.newaxis])
        sch = np.concatenate([j, k], axis=1)

    return sch.reshape(-1)

    
def init_results(results_file, session, key):
    if not pathlib.Path(results_file).exists():

        logging.info(f'Date: {get_datetime()}')
        logging.info(f'Writing results to: {results_file}')

        with open(results_file, 'w') as res:
            res.write(f'Session: {session}\n')
            res.write(','.join(key) + '\n')

def write_result(results_file, result):
    with open(results_file, 'a') as res:
        res.write(','.join([str(k) for k in result.values()]) + '\n')

def write_results(session, results_file, results):
    key = list(results[0].keys())
    init_results(results_file, session, key)

    for r in results:
        write_result(results_file, r)


def generate_records(n, dir):
    from gen_records import JoinJobRecordsGenerator
    jjrg = JoinJobRecordsGenerator(n, 1, dir)
    jjrg.write_messages('csv')

def copy_records_old(src):
    dest = '/var/lib/mysql/clientdb'

    for parent, dirs, files in os.walk(src, topdown=False):

        if not files:
            continue

        ffrom = os.path.join(parent, files[0])
        filename = os.path.basename(parent).split('-')[0] + '.csv'

        fto = os.path.join(dest, filename)

        logging.debug(f'Copy {ffrom} -> {fto}')

        shutil.copy(ffrom, fto)

def copy_records(src):
    subprocess.call([f'{bin}/call_transfer.sh', str(src), '/var/lib/mysql/clientdb'])

def delete_all_records():
    subprocess.call([f'{bin}/call_delete_data.sh', 'all'])

def delete_new_records():
    subprocess.call([f'{bin}/call_delete_data.sh', 'new'])

def load_records():
    subprocess.call([f'{bin}/call_load_data.sh'])

def call_procedure(n=''):
    subprocess.call([f'{bin}/call_procedure.sh', str(n)])

def load_procedure():
    subprocess.call([f'{bin}/load_procedure.sh'])

def count_records():
    return [int(i) for i in subprocess.check_output(['{bin}/call_count_records.sh']).decode().split('\n') if i.isnumeric()]


def run(n, dir='./'):

    logging.debug('Generating records.')
    generate_records(n, dir)

    logging.debug('Transferring records.')
    copy_records(dir)

    if config['delete_old_records']:
        logging.debug(f'Deleting all records.')
        delete_all_records()

    if config['load_procedure']:
        load_procedure()

    if config['call_procedure_n']:
        procedure = config['call_procedure_n']

    logging.debug('Loading records.')
    load_records()

    logging.debug(f'Counting records...')
    num_job_pre, num_blahd_pre, num_event_pre = count_records()

    logging.debug(f'Running procedure...')
    run_start = time.time()
    call_procedure(n=procedure)
    run_time = time.time() - run_start
    
    logging.debug(f'Counting records...')
    num_job_post, num_blahd_post, num_event_post = count_records()

    results = {
        'procedure_time': run_time,
        'num_records_generated': n,
        'num_blahd_pre': num_blahd_pre,
        'num_blahd_post': num_blahd_post,
        'num_event_pre': num_event_pre,
        'num_event_post': num_event_post,
        'num_job_pre': num_job_pre,
        'num_job_post': num_job_post,
    }

    with open(dir/'result', 'w') as f:
        [f.write(f'{k}: {v}\n') for k, v in results.items()]

    logging.info(f'time {run_time}')
    return results


def experiment(schedule, session_dir, results_file):

    results_list = []
    for i, n in enumerate(schedule):
        logging.info(f'Run: {i}')

        run_dir = session_dir / f'run.{i}'
        run_dir.mkdir(exist_ok=False)

        results = run(n, dir=run_dir)

        results_list.append(results)

        # Write result within session directory while running.
        write_result(results_file, results)

    return results_list


exp_dir = pathlib.Path(config['experiment_dir'])
session_dir = exp_dir / f'session.{len(os.listdir(exp_dir))}.{int(get_datetime().timestamp())}'

exp_dir.mkdir(exist_ok=True)
session_dir.mkdir(exist_ok=True)

logging.basicConfig(filename=config['log_file'], 
                    filemode='a',
                    encoding='utf-8',
                    level=logging.DEBUG)
                    #format="%(asctime)s:%(levelname)s:%(module)s:%(messages)s")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        config['call_procedure_n'] = sys.argv[1]

    results_file = session_dir / config['results_file']

    start_time = get_datetime()
    logging.info(f'Session: {start_time}')
    logging.info(f'Config: {config}')

    schedule = mkschedule()

    logging.info(f'Schedule: {list(schedule)}')

    results = experiment(schedule, session_dir, results_file)

    finish_time = get_datetime()
    session_time = finish_time.timestamp() - start_time.timestamp()
    logging.info(f'Session time: {session_time}')

    logging.info(f'Writing file.')

    if pathlib.Path(config['results_file']).exists():
        os.remove(config['results_file'])

    # Write final results to current directory
    write_results(session_dir, config['results_file'], results)

    logging.info(f'Finished: {finish_time}')
