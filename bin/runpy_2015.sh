#!/bin/bash
#SBATCH -N 42
#SBATCH -C haswell
#SBATCH -q regular
#SBATCH -J spider2_analyzer_2015
#SBATCH --mail-user=x-spirit.zhang@ttu.edu
#SBATCH --mail-type=ALL
#SBATCH -t 24:00:00

module load python
# conda activate env

srun -n 42 -c 64 --cpu_bind=cores python spider2_log_analyzer.py -y 2015
