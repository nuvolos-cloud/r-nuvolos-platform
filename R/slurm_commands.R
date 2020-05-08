#' Run an R script through SLURM
#' @param script The R script to execute on the cluster. The path should start with ~/
#' @param n_cpus The number of cpus requested
#' @param use_mpi Whether to use MPI for the job (required for multi-node jobs)
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
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s && sbatch --export=ALL -p %s -n %s -o \\\"%s/files/hpc_job_logs/job-%%j.out\\\" -e \\\"%s/files/hpc_job_logs/job-%%j.err\\\" --wrap \\\"mpirun --quiet -np 1 Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aid, cluster_path,queue, n_cpus, cluster_path, cluster_path, script)
    system(command_value, intern = TRUE)
  } else {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s NUM_CPUS=%s && sbatch --export=ALL -p %s -n %s -o \\\"%s/files/hpc_job_logs/job-%%j.out\\\" -e \\\"%s/files/hpc_job_logs/job-%%j.err\\\" --wrap \\\"Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aid, cluster_path, n_cpus, queue, n_cpus, cluster_path, cluster_path, script)
    system(command_value, intern = TRUE)
  }
}

#' Get slurm job status
#' @param job_id The SLURM job id
#' @export
scancel <- function(job_id) {
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm && scancel %s\"", user_name, job_id))
}

#' Get slurm job status
#' @export
squeue <- function() {
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm && squeue\"", user_name))
}

#' Run job as an interactive R script
#' @param script The R script to execute on the cluster. The path to the script should start ~/
#' @param n_cpus The number of cpus requested for the operation
#' @param queue The queue to use for the job. By default this is intq. 
#' @param use_mpi Whether to use MPI for running the job (required for multi-node jobs)
#' @param sync_wait The amount of time to wait (in seconds) after job completion for the system to synchronize files between Nuvolos and the high performance computing cluster. Larger files warrant larger wait times.
#' @param report_freq The frequency of polling job information from the cluster (by default and at least 15 seconds)
#' @export
run_job_interactive <- function(script, n_cpus = 4, queue = "intq", use_mpi=FALSE, sync_wait = 60, report_freq = 15) {
  
  if (report_freq < 15) {
      stop("Error: Please provide a polling frequency of at least 15 seconds.")
  }

  if (sync_wait < 60) {
      stop("Error: Please provide a synchronization period of at least 60 seconds. We suggest longer wait periods if you expect a large amount of information to be emitted by your cluster process.")
  }
  
  val <- sbatch(script, n_cpus, queue, use_mpi)
  m <- regexec("Submitted batch job (\\d*)", val, perl=TRUE)
  job_id <- as.integer(regmatches(val, m)[[1]][2])
  cat(sprintf("Polling the cluster for the most recent information. Monitoring job id %s.\n", job_id))
  while(TRUE)
  {
    Sys.sleep(report_freq)
    state <- suppressWarnings({ extract_state(slookup_job(job_id)) })
    timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    if (state == "FAILED" || state == -1 || state == "COMPLETED") {
      if (state == -1) {
        state <- "UNKNOWN COMPLETED STATE"
      }
      cat(sprintf("%s > Job %s has finished with state %s.\n", timestamp, job_id ,state))
      break
    } else {
      cat(sprintf("%s > Job %s is not yet finished. State is %s.\n", timestamp, job_id, state))
    }
  }
  cat("Waiting for file system synchronization between Nuvolos and the compute cluster.\n")
  Sys.sleep(sync_wait)
  cat("Exiting job execution.\n")
}

#' Provide detailed information on status of running job
#' @param job_id The job to be checked. 
#' @export
slookup_job <- function(job_id) {
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  ret <- system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm && scontrol show job %s\"", user_name, job_id), intern = TRUE)
  return(ret)
}

#' Extract job state from detailed job information provided by slookup
#' @param lookup_val Result from running slookup_job. Can be either an array containing an error message or job details. In case of an error message, the job has finished.
#' @export
extract_state <- function(lookup_val) {
  if (is.na(lookup_val[4])) {
    return(-1) # job info doesn't exist, it has been stopped some time ago  
  }
  
  m <- regexec("JobState=(\\w*) Reason=.*", lookup_val[4], perl = TRUE)
  job_state <- regmatches(lookup_val[4], m)[[1]][2]
  return(job_state)
}
