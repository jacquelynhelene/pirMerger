# Read STAR exports ----

#' Pull the latest exports from STAR into a temporary directory
#'
#' @return Path to temporary directory with the STAR exports
#'
#' @export
pull_star_exports <- function(out_dir, export_repo) {
  message("Pulling most recent getty-provenance-index commit...")
  clone_file <- paste(out_dir, "archive.zip", sep = "/")
  message("Retrieving repo archive...")
  system2("git", args = c("archive", "--format zip", paste("--remote", export_repo), paste("-o", clone_file), "HEAD"))
  message("Decompressing archive into ", out_dir)
  paths <- unzip(clone_file, exdir = out_dir)
}

data_definitions <- function(definitions) {
  yaml::yaml.load(definitions)
}

#' @import stringr
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

  headers <- readr::read_lines(file = header_path) %>%
    str_trim() %>%
    str_replace_all("[[:punct:] ]+$", "") %>%
    str_replace_all("[[:punct:] ]+", "_") %>%
    tolower()

  # Read each CSV of the table into a dataframe. Because this creates a list of
  # dataframes and we may want to inspect for problems, before binding all dfs
  # together, this will call problems() on each one, and save the resulting
  # problems table as an attribute of the full data frame.
  tdata <- map(csv_paths, readr::read_delim, delim = ",", col_names = headers, col_types = paste0(rep("c", length(headers)), collapse = ""), locale = locale(encoding = "UTF-8"))
  probs <- map(tdata, problems)
  tdata <- bind_rows(tdata)

  if (sum(map_int(probs, nrow)) / nrow(tdata) > 0.01)
    stop("More than 10 percent of the rows in ", base_name, " have readr problems.")

  attr(tdata, "problems") <- probs
  attr(tdata, "base_name") <- base_name
  tdata
}

#' Pull the latest STAR exports and parse into dataframes
#'
#' The resulting files are stored in the `data-raw` directory. If any files have
#' more than 1% parsing errors, then an error will be thrown.
#'
#' @param out_dir Directory to which rds files ought to be written
#' @param data_dict Path to the data files definition YAML
#' @param repo_path Location of git repository holding GPI data
#'
#' @export
read_all_exports <- function(out_dir, data_dict, repo_path) {
  data_files <- data_definitions(data_dict)[["star_exports"]]
  walk(data_files, function(x) {
    walk(x[["files"]], function(y) {
      output_data <- read_dat(repo_path = repo_path, dir_name = x[["dir_name"]], file_name = y, n_files = x[["n_files"]])
      obj_name <- attr(output_data, "base_name")
      saveRDS(output_data, file = paste0(out_dir, "/", obj_name, ".rds"))
    })
  })
}

# Read Google Docs ----

#' Pull the latest Google Sheets concordances and parse into dataframes
#'
#' @param out_dir Directory to which rds files ought to be written
#' @param data_dict Path to the data files definition YAML
#'
#' @export
read_all_concordances <- function(out_dir, data_dict) {
  data_files <- data_definitions(data_dict)[["google_sheets"]]
  walk(data_files, function(s) {
    message("Reading ", s[["name"]])
    res <- googlesheets::gs_read(googlesheets::gs_url(s[["url"]], verbose = FALSE), verbose = FALSE, skip = 1, col_names = s[["colnames"]], col_types = s[["coltypes"]])
    readr::stop_for_problems(res)
    saveRDS(res, file = paste0(out_dir, "/", "raw_", s[["name"]], ".rds"))
  })
}

