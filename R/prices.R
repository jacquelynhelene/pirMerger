general_parse_prices <- function(p) {

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
    select(price_currency, currency_aat)

  save_data(target_dir, currency_aat)
  invisible(currency_aat)
}

