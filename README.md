# Nuvolos Tools

This package aids using R together with the HPC cluster from Nuvolos.

## Installation

```
install.packages('remotes')
remotes::install_github('nuvolos-cloud/r-nuvolos-tools')
```

## Usage 

### Package installation on cluster

```
nuvolos.tools::install.packages('package')
# or to install from github
nuvolos.tools::install_github('repo/package')
# or to install a local package
nuvolos.tools::install_local("~/files/my_package")
# or to sync existing libraries
nuvolos.tools::package_sync_hpc()
```

### Job submission / monitoring / cancellation

```
# Submitting a job on single node
nuvolos.tools::sbatch("~/files/xyz.R",n_cpus=4)
# Submitting a job on multiple nodes
nuvolos.tools::sbatch("~/files/xyz.R",n_cpus=16, n_nodes=2)
# Checking job status
nuvolos.tools::squeue()
# Cancel job
nuvolos.tools::scancel(jobid)
```

It is possible to embed a HPC job into an interactive script by using:

```
nuvolos.tools::run_job_interactive("~/files/xyz.R",n_cpus=4)
```

The above command will run in the foreground until the batch job either completes or fails for some reason. If the job emits large files, it is strongly suggested to increase wait time after job completion by setting the `sync_wait` parameter to a larger value than default (default is 60). This serves to make sure that the file system is synced between the HPC environmment and Nuvolos so that the rest of the interactive script works on valid data.
