general_parse_prices <- function(p) {

}

replace_vulgar_fractions <- function(p) {
  str_replace_all(p, c(
    "½" = " 1/2"
  ))
}