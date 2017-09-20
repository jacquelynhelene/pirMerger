#' Convert amonsieurx codes to numbers
#'
#' @param s String with "amonsieurx" code
#' @param to_number Logical. Convert the result to numeric? If false, remains as character.
#'
#' @return Converted vector
#' @export
amonsieurx <- Vectorize(function(s, to_number = FALSE) {
  l <- tolower(s)
  e <- stringr::str_extract(l, "[amonsieurx]*")
  if (e != "" | is.na(e)) {
    r <- stringr::str_replace_all(e, c("a" = "1", "m" = "2", "o" = "3", "n" = "4", "s" = "5", "i" = "6", "e" = "7", "u" = "8", "r" = "9", "x" = "0"))
    if (to_number)
      return(as.numeric(r))
    return(r)
  } else {
    s
  }
})
