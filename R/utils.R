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
