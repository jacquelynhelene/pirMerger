#' Parse fractions into numeric values
#'
#' @param s Character vector
#'
#' @return Numeric vector
#' @export
parse_fraction <- function(s) {
  is_scalar_character(s)

  frac <- str_extract(s, "\\d+/\\d+")

  if (is.na(frac))
    return(as.numeric(s))

  whole <- as.integer(str_replace(s, fixed(frac), ""))

  if (is.na(whole))
    whole <- 0

  num <- as.integer(str_match(frac, "(\\d+)")[,2])
  den <- as.integer(str_match(frac, "/(\\d+)")[,2])

  if (is.na(num) | is.na(den))
    return(NA_real_)

  if (num >= den | den > 16)
    return(NA_real_)

  whole + num / den
}
