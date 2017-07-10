library(tidyverse)
library(stringr)

# Read STAR exports ----

#' Pull the latest exports from STAR into a temporary directory
#'
#' @return Path to temporary directory with the STAR exports
pull_star_exports <- function(export_repo = "ssh://git@stash.getty.edu:7999/griis/getty-provenance-index.git") {
  clone_path <- paste(tempdir(), "getty-clone", sep = "/")
  dir.create(clone_path)
  clone_file <- paste(clone_path, "archive.zip", sep = "/")
  message("Retrieving repo archive from ", export_repo)
  system2("git", args = c("archive", "--format zip", paste("--remote", export_repo), paste("-o", clone_file), "HEAD"))
  message("Decompressing archive into ", clone_path)
  paths <- unzip(clone_file, exdir = clone_path)
  clone_path
}

data_definitions <- function(definition_path = "data-raw/data_definitions.yml") {
  yaml::yaml.load_file(definition_path)
}

read_dat <- function(repo_path, dir_name, file_name, n_files) {
  base_name <- paste0("raw_", file_name)
  dir_path <- paste0(repo_path, "/csv/", dir_name)
  header_path <- paste0(dir_path, "/", file_name, "_headers.txt")
  if (!is.null(n_files)) {
    csv_paths <- paste0(dir_path, "/", file_name, "_", seq_len(n_files), ".csv")
  } else {
    csv_paths <- paste0(dir_path, "/", file_name, ".csv")
  }

  message("Reading ", csv_paths)

  headers <- read_lines(file = header_path) %>%
    str_trim() %>%
    str_replace_all("[[:punct:] ]+$", "") %>%
    str_replace_all("[[:punct:] ]+", "_") %>%
    tolower()

  # Read each CSV of the table into a dataframe. Because this creates a list of
  # dataframes and we may want to inspect for problems, before binding all dfs
  # together, this will call problems() on each one, and save the resulting
  # problems table as an attribute of the full data frame.
  tdata <- map(csv_paths, read_delim, delim = ",", col_names = headers, col_types = paste0(rep("c", length(headers)), collapse = ""), locale = locale(encoding = "UTF-8"))
  probs <- map(set_names(tdata, csv_paths), problems)
  tdata <- bind_rows(tdata)

  if (sum(map_int(probs, nrow)) / nrow(tdata) > 0.01)
    stop("More than 10 percent of the rows in ", base_name, " have readr problems.")

  attr(tdata, "problems") <- probs
  attr(tdata, "base_name") <- base_name
  tdata
}

walk_through_data <- function(data_files = data_definitions()[["star_exports"]], repo_path) {
  walk(data_files, function(x) {
    walk(x[["files"]], function(y) {
      output_data <- read_dat(repo_path = repo_path, dir_name = x[["dir_name"]], file_name = y, n_files = x[["n_files"]])
      obj_name <- attr(output_data, "base_name")
      saveRDS(output_data, file = paste0("data-raw/", obj_name, ".rds"))
    })
  })
}

#' Pull the latest STAR exports and parse into dataframes
#'
#' The resulting files are stored in the `data-raw` directory. If any files have
#' more than 1% parsing errors, then an error will be thrown.
read_all_exports <- function() {
  message("Pulling most recent getty-provenance-index commit...")
  star_repo <- pull_star_exports()

  walk_through_data(repo_path = star_repo)
}

# Read Google Docs ----

all_google_sheets <- function() {
  data_definitions()[["google_sheets"]]
}

read_all_concordances <- function() {
  walk(all_google_sheets(), function(s) {
    message("Reading ", s[["name"]])
    res <- googlesheets::gs_read(googlesheets::gs_url(s[["url"]]), skip = 1, col_names = s[["colnames"]], col_types = s[["coltypes"]])
    readr::stop_for_problems(res)
    saveRDS(res, file = paste0("data-raw/", "raw_", s[["name"]], ".rds"))
  })
}

