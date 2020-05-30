#!/bin/bash
#SBATCH -N 40
#SBATCH -C knl
#SBATCH -q premium
#SBATCH -J user_activity_analyzer
#SBATCH --mail-user=x-spirit.zhang@ttu.edu
#SBATCH --mail-type=ALL
#SBATCH -t 24:00:00

module load python
conda activate ActiveDR_env

cat /global/homes/w/wzhang5/software/ActiveDR/bin/2016_dir.txt | while read line
do
    srun -n 40 -c 272 --cpu_bind=cores python -u user_activity_analyzer_mpi.py -d $line
done
