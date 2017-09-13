#' Parse fractions into numeric values
#'
#' @param s Character vector
#'
#' @return Numeric vector
#' @export
parse_fraction <- Vectorize(function(s) {
  if (is.na(s))
    return(NA_real_)
  if (!str_detect(s, "/"))
    return(as.numeric(s))

  whole <- as.numeric(str_extract(s, "^\\d+ "))
  fraction <- str_extract(s, "\\d+/\\d+")
  numerator <- as.numeric(str_extract(fraction, "^\\d+"))
  denominator <- as.numeric(str_extract(fraction, "\\d+$"))

  whole_number <- ifelse(is.na(whole), 0, whole)
  numerator_number <- ifelse(is.na(numerator), 0, numerator)
  denominator_number <- ifelse(is.na(denominator), 0, denominator)
  as.numeric(whole_number) + as.numeric(numerator_number) / as.numeric(denominator_number)
})
