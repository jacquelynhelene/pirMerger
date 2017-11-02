# Read STAR exports ----

#' Pull the latest exports from STAR into a temporary directory
#'
#' This will request a zip archive of the HEAD state of the repository, and
#' decompress those files into out_dir.
#'
#' @param out_dir Destination directory for repo files
#' @param export_repo URL to git repository
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

#' Read data files with header names
#'
#' @import stringr
#' @export
read_dat <- function(..., header_file_name, stop_on_problems = TRUE) {
  file_names <- as.character(list(...))
  headers <- readr::read_lines(file = header_file_name) %>%
    str_trim() %>%
    str_replace_all("[[:punct:] ]+$", "") %>%
    str_replace_all("[[:punct:] ]+", "_") %>%
    tolower()

  # Read each CSV of the table into a dataframe. Because this creates a list of
  # dataframes and we may want to inspect for problems, before binding all dfs
  # together, this will call problems() on each one, and save the resulting
  # problems table as an attribute of the full data frame.
  tdata <- map(file_names, readr::read_delim, delim = ",", col_names = headers, col_types = paste0(rep("c", length(headers)), collapse = ""), locale = locale(encoding = "UTF-8"))
  probs <- map(tdata, problems)
  tdata <- bind_rows(tdata, .id = "original_file_name")

  if (stop_on_problems && sum(map_int(probs, nrow)) / nrow(tdata) > 0.01)
    warning("More than 10 percent of the rows in ", file_names, " have readr problems.")

  attr(tdata, "problems") <- probs
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
#' @param names Specify dataset names to refresh. If \code{NULL} (the default),
#'   will read all data frames specified in the data dictionary.
#'
#' @export
read_all_exports <- function(out_dir, data_dict, repo_path, names = NULL) {
  data_files <- data_definitions(data_dict)[["star_exports"]]

  if (!is.null(names))
    data_files <- keep(data_files, function(d) d[["dir_name"]] %in% names)

  walk(data_files, function(x) {
    walk(x[["files"]], function(y) {
      output_data <- read_dat(repo_path = repo_path, dir_name = x[["dir_name"]], file_name = y, n_files = x[["n_files"]])
      data_name <- attr(output_data, "base_name")
      message("Assigning ", data_name)
      save_data(out_dir, output_data, objname = data_name)
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

    # If no worksheet is specified, assume 1
    sheet <- s[["sheet"]]
    if (is.null(sheet))
      sheet <- 1

    res <- googlesheets::gs_read(googlesheets::gs_url(s[["url"]], verbose = FALSE), ws = sheet, verbose = FALSE, skip = 1, col_names = s[["colnames"]], col_types = s[["coltypes"]])
    readr::stop_for_problems(res)
    saveRDS(res, file = paste0(out_dir, "/", "raw_", s[["name"]], ".rds"))
  })
}

get_definitions <- function(definitions_file) {
  yaml::yaml.load_file(definitions_file)
}

refresh_concordances <- function(data_definitions) {
  walk(data_definitions$google_sheets, function(x) {
    dl_concordance(url = x$url, sheet = x$sheet, path = paste0("concordance_csv/", x$name, ".csv"))
  })
}

dl_concordance <- function(path, url, sheet = 1) {
  googlesheets::gs_download(googlesheets::gs_url(url, verbose = FALSE), ws = sheet, to = path, verbose = FALSE, overwrite = TRUE)
}

read_concordance <- function(filename, col_names, col_types) {
  res <- readr::read_csv(filename, skip = 1, col_names = str_split(col_names, ";")[[1]], col_types = col_types)
  readr::stop_for_problems(res)
  res
}

#' Pull data from a given directory and name, and load into an object
#'
#' @param dir The directory to read from
#' @param name The name of the file (sans .rds)
#'
#' @return An R object stored in that data file
#'
#' @export
get_data <- function(dir, name) {
  readRDS(paste(dir, paste(name, "rds", sep = "."), sep = "/"))
}

#' Save an object to working data directory
#'
#' The object will be saved as dir/name_of_object.rds
#'
#' @param dir The directory to write to
#' @param obj The object to save
#'
#' @return Invisibly returns the path of the saved object
#'
#' @export
save_data <- function(dir, obj, objname = NULL) {
  if (is.null(objname))
    objname <- as.character(substitute(obj))

  newpath <- paste(dir, paste(objname, "rds", sep = "."), sep = "/")
  saveRDS(obj, file = newpath)
  invisible(newpath)
}
