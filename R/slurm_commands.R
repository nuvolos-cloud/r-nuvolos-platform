#' Run an R script through SLURM
#' @export
slurm_run <- function(script, n_cpus=4, queue="intq") {
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  r_version <- paste0(R.version$major,".",R.version$minor)
  system(sprintf("ssh -o ServerAliveInterval=30 %s@scc-secondary.alphacruncher.net \"module load slurm R/intel/mkl/%s && export R_LIBS_USER=%s/lib HOME=%s && sbatch --export=ALL -p %s -n %s -o \\\"%s/files/job-%%j.out\\\" -e \\\"%s/files/job-%%j.err\\\" --wrap \\\"mpirun --quiet -np 1 Rscript --verbose %s\\\"\"",user_name, r_version, cluster_path,cluster_path,queue, n_cpus, cluster_path, cluster_path, script))
}
