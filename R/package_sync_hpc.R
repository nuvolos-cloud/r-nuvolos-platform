#' Synchronize packages between Nuvolos and the HPC Cluster
#' @export
package_sync_hpc <- function(use_gsl=FALSE) {
  installed_packages_local <- dir("/dhlib")
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  aid <- read.delim('/lifecycle/.aoid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  # create remote package folder if doesn't exist yet
  system(sprintf("ssh %s@hpc.nuvolos.cloud 'mkdir -p %s/lib/%s'",user_name, cluster_path, aid))

  # create a folder where we track successful remote package installs
  system('mkdir -p ~/hpc_installed/lib')

  # remove any lock folders
  system(sprintf("ssh %s@hpc.nuvolos.cloud 'rm -rf %s/lib/%s/00*'",user_name, cluster_path, aid))

  # get list of installed packages
  installed_packages_remote <- system(sprintf('ls ~/hpc_installed/lib/%s', aid), intern = TRUE)

  packages_to_install <- setdiff(installed_packages_local, installed_packages_remote)

  if (length(packages_to_install) > 0) {
    r_version <- paste0(R.version$major,".",R.version$minor)
    for (p in packages_to_install) {
      print(sprintf("Installing package: %s", p))
      system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load R/intel/mkl/%s && Rscript -e \\\"install.packages('%s',lib='%s/lib/%s', repos='%s')\\\"\" && mkdir -p ~/hpc_installed/lib/%s/%s",
                     user_name, cluster_path, r_version, p, cluster_path, aid ,options()$repos,aid, p))
    }
  } else {
    print("No additional packages to install.")
  }
}

#' Install packages locally and remotely
#' @param package A vector of package names
#' @export
install.packages <- function(package, use_gsl = FALSE) {
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  aid <- read.delim('/lifecycle/.aoid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  # create remote package folder if doesn't exist yet
  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud 'mkdir -p %s/lib/%s'",user_name, cluster_path, aid))

  # remove any lock folders
  system(sprintf("ssh %s@hpc.nuvolos.cloud 'rm -rf %s/lib/%s/00*'",user_name, cluster_path, aid))

  # create a folder where we track successful remote package installs
  system(sprintf('mkdir -p ~/hpc_installed/lib/%s',aid))

  # install first on hpc cluster
  for (p in package) {
    r_version <- paste0(R.version$major,".",R.version$minor)
    print(sprintf("Installing package on cluster: %s", p))
    system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load R/intel/mkl/%s && Rscript -e \\\"install.packages('%s',lib='%s/lib/%s', repos='%s')\\\"\" && mkdir -p ~/hpc_installed/lib/%s/%s",
                   user_name, cluster_path, r_version, p, cluster_path, aid, options()$repos, aid,p))
  }

  utils::install.packages(package)
}


#' Install github packages locally and remotely
#' @param repo A github repo URL
#' @export
install_github <- function(repo, use_gsl = FALSE) {
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  aid <- read.delim('/lifecycle/.aoid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  r_version <- paste0(R.version$major,".",R.version$minor)

  if (!'remotes' %in% dir(sprintf('mkdir -p ~/hpc_installed/lib/%s',aid))) {
    nuvolos.tools:::install.packages('remotes')
  }

  # remove any lock folders
  system(sprintf("ssh %s@hpc.nuvolos.cloud 'rm -rf %s/lib/%s/00*'",user_name, cluster_path, aid))

  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s && Rscript -e \\\"remotes::install_github('%s')\\\"\" && mkdir -p ~/hpc_installed/lib/%s/%s",
                user_name, cluster_path, r_version, cluster_path, aid, repo, aid, tail(strsplit(repo,"/")[[1]],1)))
  remotes::install_github(repo)
}


#' Synchronize packages between Nuvolos and the HPC Cluster
#' @param path A local filesystem path (must be given in relative terms, starting with '~/')
#' @export
install_local <- function(path, use_gsl = FALSE) {
  gsub("(.*)/$","\\1",path, perl=TRUE)
  if (!grepl("^~/",path)) {
    stop("Error: path must start with ~/")
  }
  cluster_path <- read.delim("/lifecycle/.clusterpath", header = FALSE, stringsAsFactors = FALSE)[1,1]
  user_name <- suppressWarnings({ read.delim("/secrets/username", header = FALSE, stringsAsFactors = FALSE)[1,1] })
  aid <- read.delim('/lifecycle/.aoid', header = FALSE, stringsAsFactors = FALSE)[1,1]
  r_version <- paste0(R.version$major,".",R.version$minor)

  if (!'remotes' %in% dir(sprintf('mkdir -p ~/hpc_installed/lib/%s',aid))) {
    nuvolos.tools:::install.packages('remotes')
  }

  # remove any lock folders
  system(sprintf("ssh %s@hpc.nuvolos.cloud 'rm -rf %s/lib/%s/00*'",user_name, cluster_path, aid))

  system(sprintf("ssh -o ServerAliveInterval=30 %s@hpc.nuvolos.cloud \"export HOME=%s && cd ~/files && module load R/intel/mkl/%s && export R_LIBS_USER=%s/lib/%s HOME=%s && Rscript -e \\\"remotes::install_local('%s',force=TRUE)\\\"\" && mkdir -p ~/hpc_installed/lib/%s/%s",
                user_name, cluster_path, r_version, cluster_path, aid, cluster_path, path, aid, tail(strsplit(path,"/")[[1]],1)))
  remotes::install_local(path=path, force=TRUE)
}
