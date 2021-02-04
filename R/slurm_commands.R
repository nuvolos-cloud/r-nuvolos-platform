#' Run an R script through SLURM
#' @param script The R script to execute on the cluster. The path should start with ~/
#' @param n_cpus The number of cpus requested
#' @param use_mpi Whether to use MPI for the job (required for multi-node jobs)
#' @param use_gsl Whether to make GSL (GNU Scientific Library) available for linking
#' @param user_name Username at the Nuvolos cluster, leave this empty from a Nuvolos application
#' @param array If running an array job, a 2-length vector specifying starting and ending array indices
#' @param n_nodes The number of nodes requested
#' @export
sbatch <- function(script, n_cpus=4, queue="intq", use_mpi=FALSE, use_gsl = FALSE, user_name = NULL, array=NULL, n_nodes=1) {
  if (!grepl("^~/",script)) {
    stop("Error: script must be given as a path that starts with ~/")
  }
  if (!dir.exists("~/files/hpc_job_logs")) {
    dir.create("~/files/hpc_job_logs")
  }
  cluster_path <- get_cluster_path()
  if(is.null(user_name)) {
    user_name <- get_user_name()
  }
  aoid <- read.delim('/lifecycle/.aoid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  r_version <- paste0(R.version$major,".",R.version$minor)
  
  if (!is.null(array)) {
    array_str <- sprintf("--array=%s-%s ",array[1],array[2])
    format_str <- "%A_%a"
  } else {
    array_str <- ""
    format_str <- "%j"
  }
  
  if (use_mpi && use_gsl) {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && module load gsl-2.4-intel-17.0.6-stf5st2 && export R_LIBS_USER=%s/lib/%s HOME=%s && cd ~/files && sbatch %s --export=ALL -p %s -n %s --nodes=%s -o \\\"%s/files/hpc_job_logs/job-%s.out\\\" -e \\\"%s/files/hpc_job_logs/job-%s.err\\\" --wrap \\\"mpirun --quiet -np 1 Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aoid, cluster_path, array_str, queue, n_cpus, n_nodes, cluster_path, format_str, cluster_path, format_str, script)
    system(command_value, intern = TRUE)
  } else if (use_mpi && !use_gsl) {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s && cd ~/files && sbatch %s --export=ALL -p %s -n %s --nodes=%s -o \\\"%s/files/hpc_job_logs/job-%s.out\\\" -e \\\"%s/files/hpc_job_logs/job-%s.err\\\" --wrap \\\"mpirun --quiet -np 1 Rscript --verbose %s\\\"\"",
                             user_name, r_version, cluster_path, aoid, cluster_path, array_str, queue, n_cpus, n_nodes, cluster_path, format_str, cluster_path, format_str, script)
    system(command_value, intern = TRUE)
  }else if (!use_mpi && use_gsl) {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && module load gsl-2.4-intel-17.0.6-stf5st2 && export R_LIBS_USER=%s/lib/%s HOME=%s NUM_CPUS=%s && cd ~/files &&  sbatch %s --export=ALL -p %s -n %s --nodes=%s -o \\\"%s/files/hpc_job_logs/job-%s.out\\\" -e \\\"%s/files/hpc_job_logs/job-%s.err\\\" --wrap \\\"Rscript --verbose %s\\\"\"",
                             user_name, r_version, cluster_path, aoid, cluster_path, n_cpus, array_str, queue, n_cpus, n_nodes, cluster_path, format_str, cluster_path, format_str, script)
    system(command_value, intern = TRUE)
  } else {
    command_value <- sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s NUM_CPUS=%s && cd ~/files &&  sbatch %s --export=ALL -p %s -n %s --nodes=%s -o \\\"%s/files/hpc_job_logs/job-%s.out\\\" -e \\\"%s/files/hpc_job_logs/job-%s.err\\\" --wrap \\\"Rscript --verbose %s\\\"\"",
    user_name, r_version, cluster_path, aoid, cluster_path, n_cpus, array_str, queue, n_cpus, n_nodes, cluster_path, format_str, cluster_path, format_str, script)
    system(command_value, intern = TRUE)
  }
}

#' Get slurm job status
#' @param job_id The SLURM job id
#' @param user_name Username at the Nuvolos cluster, leave this empty from a Nuvolos application
#' @export
scancel <- function(job_id, user_name = NULL) {
  cluster_path <- get_cluster_path()
  if(is.null(user_name)) {
    user_name <- get_user_name()
  }
  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load slurm && scancel %s\"", user_name, cluster_path, job_id))
}

#' Get slurm job status
#' @export
#' @param user_name Username at the Nuvolos cluster, leave this empty from a Nuvolos application
#' @export
squeue <- function(user_name = NULL) {
  cluster_path <- get_cluster_path()
  if(is.null(user_name)) {
    user_name <- get_user_name()
  }
  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load slurm && squeue\"", user_name, cluster_path))
}

#' Run job as an interactive R script
#' @param script The R script to execute on the cluster. The path to the script should start ~/
#' @param n_cpus The number of cpus requested for the operation
#' @param queue The queue to use for the job. By default this is intq.
#' @param use_mpi Whether to use MPI for running the job (required for multi-node jobs)
#' @param sync_wait The amount of time to wait (in seconds) after job completion for the system to synchronize files between Nuvolos and the high performance computing cluster. Larger files warrant larger wait times.
#' @param report_freq The frequency of polling job information from the cluster (by default and at least 15 seconds)
#' @param user_name Username at the Nuvolos cluster, leave this empty from a Nuvolos application
#' @export
run_job_interactive <- function(script, n_cpus = 4, queue = "intq", use_mpi=FALSE, sync_wait = 60, report_freq = 15, user_name = NULL) {

  if (report_freq < 15) {
      stop("Error: Please provide a polling frequency of at least 15 seconds.")
  }

  if (sync_wait < 60) {
      stop("Error: Please provide a synchronization period of at least 60 seconds. We suggest longer wait periods if you expect a large amount of information to be emitted by your cluster process.")
  }

  val <- sbatch(script, n_cpus, queue, use_mpi, user_name)
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
#' @param user_name Username at the Nuvolos cluster, leave this empty from a Nuvolos application
#' @export
slookup_job <- function(job_id, user_name) {
  cluster_path <- get_cluster_path()
  if(is.null(user_name)) {
    user_name <- get_user_name()
  }
  ret <- system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load slurm && scontrol show job %s\"", user_name, cluster_path, job_id), intern = TRUE)
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

get_cluster_path <- function() {
  return(read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1])
}

get_user_name <- function() {
  return(suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] }))
}
