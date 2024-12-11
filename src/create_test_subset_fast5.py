import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_dirs_in_dir, get_fast5_dirs, get_samples_in_dir


def main(in_dir:str):
    sample_dirs = get_dirs_in_dir(dir=in_dir)
    # Create dict with sample_name:[sample_fast5s_dirs] as key:val
    sample_data = {os.path.basename(os.path.normpath(s)):get_fast5_dirs(dir=s) for s in sample_dirs}
    # Create list of samples for iteration
    samples = list(sample_data.keys())
    samples.sort()
    i=0
    subset_fast5s = 10

    for sample in samples:
        if i < 2:
            fast5_dirs = sample_data[sample]
            for d in fast5_dirs:
                fast5s = get_samples_in_dir(dir=d, extensions=('.fast5'))[:subset_fast5s]
                for fast5 in fast5s:
                    os.system(f'mkdir -p {out_dir}{sample} && cp {fast5} {out_dir}{sample}/')
            i+=1


in_dir = f'{os.path.normpath(os.path.join(sys.argv[1]))}{os.sep}'
out_dir = f'{os.path.normpath(os.path.join(sys.argv[2]))}{os.sep}'

if __name__ == "__main__":
    main(in_dir=in_dir)