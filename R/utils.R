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
