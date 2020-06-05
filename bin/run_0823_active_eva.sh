#!/bin/bash
#SBATCH -N 20
#SBATCH -C haswell
#SBATCH -q premium
#SBATCH -J user_activity_analyzer
#SBATCH --mail-user=x-spirit.zhang@ttu.edu
#SBATCH --mail-type=ALL
#SBATCH -t 2:00:00

module load python
conda activate ActiveDR_env

srun -n 20 -c 64 --cpu_bind=cores python -u user_activity_analyzer_mpi.py -d 20160823