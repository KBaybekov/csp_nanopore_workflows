import unittest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.slurm import submit_slurm_job, is_slurm_job_running


class TestSlurmUtils(unittest.TestCase):

    @patch('pyslurm.job')
    def test_submit_slurm_job(self, mock_job):
        mock_job_instance = MagicMock()
        mock_job.return_value = mock_job_instance
        mock_job_instance.submit_batch_job.return_value = 1234

        result = submit_slurm_job('echo "Hello, World!"', 'test_job', 'cpu_nodes')
        self.assertEqual(result, 1234)

    @patch('pyslurm.job')
    def test_is_slurm_job_running(self, mock_job):
        mock_job_instance = MagicMock()
        mock_job.return_value = mock_job_instance
        mock_job_instance.find_id.return_value = {'job_state': 'RUNNING'}

        self.assertTrue(is_slurm_job_running('1234'))

        mock_job_instance.find_id.return_value = {'job_state': 'COMPLETED'}
        self.assertFalse(is_slurm_job_running('1234'))


if __name__ == '__main__':
    unittest.main()