#!/usr/bin/env python3

""""""

import sys
from utils import save_yaml, get_samples_in_dir

def parse_args(args:list):
    if len(args) != 3:
        print("Использование: get_nanopore_samples.py dir_with_samples out_dir")
        sys.exit(1)
    return args[1], args[2]

input_dir, output_dir = parse_args(sys.argv)
samples = get_sample_dirs_nanopore(dir=input_dir, )