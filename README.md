

# ActiveDR - An Activeness-based Data Retention Action Recommender for HPC Facilities

This is the implementation of ActiveDR, an Activeness-based Data Retention Action Recommender for HPC Facilities.

Please notice that our implementation will continue to improve as the project progresses.
Our implementation is written in python, and we use (or plan to use) the following packages in the program.

```
pyyaml
numpy
neo4j
pandas
scipy
networkx
mpi4py
sortedcontainers
```

The targeted working environment of our implementation is currently on Cori supercomputer, which is hosted by NERSC. 
For the rest of the document, we introduce how to install and run our implementation.

## Download our implementation

git clone "https://github.com/zhangwei217245/ActiveDR.git" 

## Build you own conda environment on Cori:

It is highly recommended that you use your own conda environment different than the one that is globally available on Cori. 
By doing this, you will be able to avoid conflicts between various versions of the packages. 

### 1. load required modules in order to be able to install `mpi4py` in your own conda environment

As we use `mpi4py` in our project, when installing the package in your own conda environment, it needs to be complied from scratch.
To ensure that the compilation can be done successfully, you need to load the a series of modules to guarantee a working environment for the compilation.

```bash
module unload PrgEnv-intel
module load PrgEnv-gnu/6.0.5
module load cmake/3.14.4
module load gcc
module load openmpi/3.1.3
module load llvm/10.0.0
module load python
```

### 2. create and switch to your own conda environment.

First, create a conda environment named `ActiveDR_env`

```bash
conda create -n ActiveDR_env python=3 
```

Now initialize your conda environment

```bash
conda init
```

Then, activate your conda environment

```bash
conda activate ActiveDR_env
```

If you need to get back to the original default environment, do the following:

```bash
conda deactivate
```

### 3. Installing required packages

First, make sure the conda environment is activated

```bash
conda activate ActiveDR_env
```

Then, install `pip`

```bash
conda install pip
```

Now, you can install required packages:

```bash
pip install -r requirements.txt
```

## Run a single process of this program

```bash
cd bin
nohup python -u user_activity_analyzer.py -d 20160823 > nohup.out 2>&1 &
```

## Run with MPI support on multiple KNL computing nodes

```bash
sbatch run_active_eva_knl.sh
```

## Run with MPI support on multiple Haswell computing nodes

```bash
sbatch run_active_eva.sh
```

# About the source code

Here we list all relevant source codes for the ActiveDR

```
┣━┓ lib
  ┣━┓ data_source
  ┃ ┣━┓ csv
  ┃ ┃ ┣━━ CSVReader.py             # a CSV reader class that can be reused for reading CSV file and generating pandas dataframe
  ┃ ┣━┓ ornl
  ┃ ┃ ┣━━ PurgeSimulator.py         # a purge policy simulator that runs by maintaining counters of the purged files of various types of users.
  ┃ ┃ ┣━━ UserActivityAnalyzer.py   # a user activeness analyzer that evaluate user's activeness based on their job submission and research publications.
```
