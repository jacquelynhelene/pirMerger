general_parse_prices <- function(p) {
  amonsieurx(p) %>%
    str_replace(pattern = "^\\[", "") %>%
    str_split(pattern = regex("[[:punct:]]"), simplify = TRUE) %>%
    as.data.frame() %>%
    mutate(
      original = kp$purch_amount,
      index = seq_along(V1)) %>%
    mutate_all(na_if, y = "") %>%
    discard(function(p) all(is.na(p)))
}

replace_vulgar_fractions <- function(p) {
  str_replace_all(p, c(
    "½" = " 1/2"
  ))
}

#' Produce tables with currency AAT ids
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @export
produce_currency_ids <- function(source_dir, target_dir) {
  curr_auth <- get_data(source_dir, "raw_sales_contents_auth_currencies")
  curr_aat <- get_data(source_dir, "raw_currencies_aat")

  currency_aat <- curr_auth %>%
    mutate_at(vars(auth_currency), tolower) %>%
    left_join(mutate_at(curr_aat, vars(auth_currency), tolower), by = "auth_currency") %>%
    select(price_currency, currency_aat, primary_unit, secondary_unit, tertiary_unit)

  save_data(target_dir, currency_aat)
  invisible(currency_aat)
}

#' Produce tables with currency exchange rates
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @export
produce_exchange_rates <- function(source_dir, target_dir) {
  exchange_rates <- get_data(source_dir, "raw_exchange_rates") %>%
    mutate(fex_to_usd = usd / fex)

  save_data(target_dir, exchange_rates)
  invisible(exchange_rates)
}

#' Produce tables with US consumer price index
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @export
produce_cpi <- function(source_dir, target_dir) {
  us_cpi <- get_data(source_dir, "raw_us_cpi") %>%
    select(-source)

  rate_1900 <- us_cpi %>%
    filter(year == 1900) %>%
    pull(us_cpi)

  us_cpi <- us_cpi %>%
    mutate(base1900 = us_cpi / rate_1900)

  save_data(target_dir, us_cpi)
  invisible(us_cpi)
}
