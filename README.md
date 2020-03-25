# Nuvolos Tools

This package aids using R together with the HPC cluster from Nuvolos

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
# or to sync existing libraries
nuvolos.tools::package_sync_hpc()
```

### Job submission

```
nuvolos.tools::slurm_run("~/files/xyz.R",n_cpus=10)
```
