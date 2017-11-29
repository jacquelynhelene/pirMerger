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

  if (sum(map_int(probs, nrow)) / nrow(tdata) > 0.01) {
    msg <- paste0("More than 10 percent of the rows in ", file_names, " have readr problems.")
    if (stop_on_problems) {
      stop(msg)
    } else {
      warning(msg)
    }
  }

  attr(tdata, "problems") <- probs
  tdata
}

dl_concordance <- function(path, url, sheet = 1) {
  googlesheets::gs_download(googlesheets::gs_url(url, verbose = FALSE), ws = sheet, to = path, verbose = FALSE, overwrite = TRUE)
}

read_concordance <- function(filename, col_names, col_types) {
  res <- readr::read_csv(filename, skip = 1, col_names = str_split(col_names, ";")[[1]], col_types = col_types)
  readr::stop_for_problems(res)
  res
}
