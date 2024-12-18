import pyslurm

job = {'job_name': 'test_0', 'partition': 'cpu_nodes', 'command': 'hostname', 'ntasks': '16', 'nodes': 5, 'output': '/tmp/test_0_%j.out'}
pyslurm.job.submit_batch_job(job)