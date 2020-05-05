#' Run an R script through SLURM
#' @param script The R script to execute on the cluster. The path should start with ~/
#' @param n_cpus The number of cpus requested
#' @param mpi Whether to use MPI for the job (required for multi-node jobs)
#' @export
sbatch <- function(script, n_cpus=4, queue="intq", use_mpi=FALSE) {
  if (!grepl("^~/",script)) {
    stop("Error: script must be given as a path that starts with ~/")
  }
  if (!dir.exists("~/files/hpc_job_logs")) {
    dir.create("~/files/hpc_job_logs")
  }
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  aid <- read.delim('/lifecycle/.aid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  r_version <- paste0(R.version$major,".",R.version$minor)
  if (use_mpi) {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@scc-secondary.alphacruncher.net \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s && sbatch --export=ALL -p %s -n %s -o \\\"%s/files/hpc_job_logs/job-%%j.out\\\" -e \\\"%s/files/hpc_job_logs/job-%%j.err\\\" --wrap \\\"mpirun --quiet -np 1 Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aid, cluster_path,queue, n_cpus, cluster_path, cluster_path, script)
    print(command_value)
    system(command_value)
  } else {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@scc-secondary.alphacruncher.net \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s NUM_CPUS=%s && sbatch --export=ALL -p %s -n %s -o \\\"%s/files/hpc_job_logs/job-%%j.out\\\" -e \\\"%s/files/hpc_job_logs/job-%%j.err\\\" --wrap \\\"Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aid, cluster_path, n_cpus, queue, n_cpus, cluster_path, cluster_path, script)
    print(command_value)
    system(command_value)
  }
}

#' Get slurm job status
#' @param job_id The SLURM job id
#' @export
scancel <- function(job_id) {
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  system(sprintf("ssh -o ServerAliveInterval=30 %s@scc-secondary.alphacruncher.net \"module load slurm && scancel %s\"", user_name, job_id))
}

#' Get slurm job status
#' @export
squeue <- function() {
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  system(sprintf("ssh -o ServerAliveInterval=30 %s@scc-secondary.alphacruncher.net \"module load slurm && squeue\"", user_name))
}
