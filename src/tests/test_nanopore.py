import unittest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.nanopore import get_fast5_dirs, convert_fast5_to_pod5, basecalling, aligning


class TestNanoporeUtils(unittest.TestCase):

    @patch('os.walk')
    def test_get_fast5_dirs(self, mock_walk):
        mock_walk.return_value = [
            ('/dir/sample1/fast5_pass', [], ['file1.fast5', 'file2.fast5']),
            ('/dir/sample2/fast5_pass', [], ['file3.fast5']),
        ]

        result = get_fast5_dirs('/dir')
        self.assertEqual(result, ['/dir/sample1/fast5_pass', '/dir/sample2/fast5_pass'])

        # Проверка исключения
        mock_walk.return_value = []
        with self.assertRaises(FileNotFoundError):
            get_fast5_dirs('/empty')

    @patch('utils.slurm.submit_slurm_job')
    def test_convert_fast5_to_pod5(self, mock_submit):
        mock_submit.side_effect = [1001, 1002]

        fast5_dirs = ['/dir/sample1/fast5_pass', '/dir/sample2/fast5_pass']
        result = convert_fast5_to_pod5(fast5_dirs, 'sample', '/output', '8', 16)

        self.assertEqual(result, [1001, 1002])
        self.assertEqual(mock_submit.call_count, 2)

    @patch('utils.slurm.submit_slurm_job')
    def test_basecalling(self, mock_submit):
        mock_submit.return_value = 1003

        job_id, ubam = basecalling('sample', '/input', '/output', '5mCG', 'model', [1001])
        self.assertEqual(job_id, 1003)
        self.assertEqual(ubam, '/output/sample/sample_5mCG.ubam')

        mock_submit.assert_called_once_with(
            "dorado basecaller --modified-bases 5mCG model /input/sample/*.pod5 > /output/sample/sample_5mCG.ubam",
            partition="gpu_nodes",
            nodes=1,
            job_name="basecall_sample_5mCG",
            dependency=[1001]
        )

    @patch('utils.slurm.submit_slurm_job')
    def test_aligning(self, mock_submit):
        mock_submit.return_value = 1004

        job_id, bam = aligning('sample', '/input/sample_5mCG.ubam', '/output', '5mCG', 'ref.fasta', '16', [1003])
        self.assertEqual(job_id, 1004)
        self.assertEqual(bam, '/output/sample/sample_5mCG.bam')

        mock_submit.assert_called_once()


if __name__ == '__main__':
    unittest.main()