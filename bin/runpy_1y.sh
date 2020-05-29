#!/bin/bash
#SBATCH -N 1
#SBATCH -C haswell
#SBATCH -q premium
#SBATCH -J spider2_analyzer_1y
#SBATCH --mail-user=x-spirit.zhang@ttu.edu
#SBATCH --mail-type=ALL
#SBATCH -t 24:00:00

module load python
# conda activate env

srun -n 1 -c 64 --cpu_bind=cores python spider2_log_analyzer.py -y 2016
