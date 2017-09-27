#' Generalized function for decimalizing and standardizing currencies on prices
#'
#' @param source_dir Directory from which to read currency authority tables.
#' @param df Source data frame
#' @param amount_col_name Quoted name of column with original price string.
#' @param currency_col_name Quoted name of column with original currency string.
#' @param id_col_name Quoted name of column with unique id.
#' @param decimalized_col_name Quoted name of column to produce containing
#'   decimalized amount.
#' @param aat_col_name Quoted name of column to produce containing AAT code for
#'   currency
#' @param amonsieurx Boolean. Apply \link{amonsieurx} transformation before
#'   parsing?
#' @param replace_slashes Boolean. Replace all \code{/} characters with \code{-}
#'   before parsing? (Necessary in cases where \code{/} is being used to
#'   demarcate currency subunits, rather than fractions.)
#'
#' @return A data frame with 3 columns: \code{id_col_name} containing IDs from
#'   original \code{df}, and  new columns for decimalized amount and currency.
parse_prices <- function(source_dir, df, amount_col_name, currency_col_name, id_col_name, decimalized_col_name, aat_col_name, amonsieurx = FALSE, replace_slashes = FALSE) {
  currency_aat <- get_data(source_dir, "currency_aat")

  parsed_prices <- select(df, !!id_col_name, original_price = !!amount_col_name, original_currency = !!currency_col_name)

  # If specified, do string replacement on "amonsieurx" price codes
  if (amonsieurx) {
    message("- Decoding 'amonsieurx' price codes")
    parsed_prices <- mutate(parsed_prices, price_amount = amonsieurx(original_price))
  } else {
    parsed_prices <- mutate(parsed_prices, price_amount = original_price)
  }

  # In the Knoedler project, slashes do not represent fractions, but instead are markers to differentiate between pounds/shillings/pence. Since the general parsing code expects slashes to stand for fractions, this replacement function substitutes a - for a /
  if (replace_slashes) {
    parsed_prices <- mutate(parsed_prices, price_amount = str_replace_all(price_amount, "/", "-"))
  }

  parsed_prices <- parsed_prices %>%
    mutate(
      # Do not continue to parse any price strings that may contain multiple values
      illegible = str_detect(price_amount, "[Ii]lleg"),
      is_multiple = str_detect(price_amount, "[\\[\\]]?(ou?r|ou|o|and|&|oder|-) ?[\\[\\]]? ?"),
      has_ellipses = str_detect(price_amount, "\\.\\.\\."),
      has_question = str_detect(price_amount, "\\?"),
      has_letters = str_detect(price_amount, "[a-z£]"),
      is_incomplete = str_detect(price_amount, "\\[ *\\]"),
      # Remove start/end brackets
      price_amount = str_replace_all(price_amount, c("^\\[" = "", "\\]$" = "")),
      # If any of the following cases, do NOT attempt to parse the price. Pass
      # NA along through the remainder of the parsing function?
      toss_record = illegible | has_letters | has_question | is_multiple | has_ellipses | is_incomplete,
      price_amount = if_else(toss_record, NA_character_, price_amount),
      # Compress whitespace
      price_amount = str_replace_all(price_amount, " +", " "),
      # NA for all blank fields
      price_amount = na_if(str_trim(price_amount), "")) %>%
    # Decimalize nubmers by extracting their components
    separate(price_amount, into = c("primary", "secondary", "tertiary"), sep = "[\\.\\-,:;=\" ]+", remove = FALSE, fill = "right") %>%
    # Remove spaces
    mutate_if(function(x) any(x == "", na.rm = TRUE), na_if, y = "") %>%
    # Parse fractional amounts into numeric values
    mutate_at(vars(primary, secondary, tertiary), funs("number" = coalesce(parse_fraction(.), 0)))

  # Proceed to decimalize these parsed subunits of the original number based on currency rules.
  # 1. Joins the AAT currency reconciliation table based on the original
  # currency string. Along with an AAT id, this also joins the divisors needed
  # to create closures with the decimal_function() generator.
  # 2. For each row, generate the decimalization function based on the
  # per-currency divisors, and then decimalize the actual record amount using
  # that decimliazation function.
  decimalized_prices <- parsed_prices %>%
    # Join currency IDs and divisors
    left_join(currency_aat, by = c("original_currency" = "price_currency")) %>%
    rename(!!aat_col_name := currency_aat) %>%
    # For each record...
    mutate(!!decimalized_col_name := pmap_dbl(
      # Get the actual units, and the currency's divisor units
      select(.,
             divisor_1 = primary_unit,
             divisor_2 = secondary_unit,
             divisor_3 = tertiary_unit,
             primary = primary_number,
             secondary = secondary_number,
             tertiary = tertiary_number),
      function(divisor_1, divisor_2, divisor_3, primary, secondary, tertiary) {
        # Produce the appropriate conversion function
        conversion_function <- decimal_function(divisor_1, divisor_2, divisor_3)
        # Run the actual values through the resulting conversion function
        conversion_function(primary, secondary, tertiary)
      }))

  # Output the decimalized value and currency AAT id, along with the necessary
  # record ID for joining to the original table
  select(decimalized_prices, !!id_col_name, !!decimalized_col_name, !!aat_col_name)
}

# Handles edge case where fl/fl. can stand for either German florins or for Dutch guidlers.
disambiguate_florins <- function(df) {

}

decimal_function <- function(divisor_1 = 1, divisor_2 = 1, divisor_3 = 1) {
  # If a supplied divisor is NA, set to 1
  set_to_one <- function(x) if_else(is.na(x), 1, x)
  divisor_1 <- set_to_one(divisor_1)
  divisor_2 <- set_to_one(divisor_2)
  divisor_3 <- set_to_one(divisor_3)
  function(primary, secondary, tertiary) {
    if (primary == 0 & secondary == 0 & tertiary == 0) {
      return(NA_real_)
    }
    primary / divisor_1 + secondary / (divisor_1 * divisor_2) + tertiary / (divisor_1 * divisor_2 * divisor_3)
  }
}

#' Produce tables with currency AAT ids
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @export
produce_currency_ids <- function(source_dir, target_dir) {
  # Concordance of verbatim currency values to standardized labels
  curr_auth <- get_data(source_dir, "raw_sales_contents_auth_currencies")
  # Concordance of standardized labels to ULAN IDs, along with subunit divisors
  # (e.g. describing 1 pound > 12 shillings > 20 pence)
  curr_aat <- get_data(source_dir, "raw_currencies_aat")

  # Join both tables together
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
    # Create multiplier that will convert a foreign currency to USD
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
    # Create multiplier that will express USD from a given year as USD from 1900
    mutate(base1900 = us_cpi / rate_1900)

  save_data(target_dir, us_cpi)
  invisible(us_cpi)
}
