#' Pipe-friendly message function
#'
#' Takes an object and a message, displays the message, and then returns the object. Ideal for use in a pipeline.
#'
#' @param x An object
#' @param m Character. Message to be displayed using `message`
#'
#' @return x
#'
#' @export
pipe_message <- function(x, m) {
  message(m)
  x
}

#' Grab the first non-NA value of a vector
#'
#' A useful function when calling \link{summarize}[dplyr]
#'
#' @param v A vector
#'
#' @return A scalar
#' @export
pick <- function(v) {
  first(na.omit(v))
}

#' Collapse multiple values into one string with a separator
#'
#' A useful function when calling \link{summarize}[dplyr]
#'
#' @param v A vector
#' @param sep Character to separate multiple values with
#' @param collapse Boolean. Only return unique values?
#'
#' @return A character scalar
#' @export
rollup <- function(v, sep = "; ", collapse = TRUE) {
  if (collapse)
    v <- unique(v)
  res <- paste0(na.omit(sort(v)), collapse = sep)
  if (res == "")
    return(NA_character_)
  res
}

no_dots <- function(df) {
  if (any(str_detect(names(df), "\\.")))
    stop("You have doubled up joins in this dataframe.")
  df
}

#' Produce a datestamped CSV of a dataframe
#'
#' @param df A data frame
#'
#' @return The path of the newly-written CSV file.
#'
#' @export
make_report <- function(df) {
  report_path <- paste0("~/Desktop/", deparse(substitute(df)), "_", Sys.Date(), ".csv")
  readr::write_csv(df, path = report_path, na = "")
  report_path
}

# Easily pull names not wanted in GitHub exports
redact <- function(x, redactions = c("NEW", "NON-UNIQUE", "non-unique")) {
  ifelse(x %in% redactions, NA_character_, x)
}

