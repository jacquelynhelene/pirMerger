#' Produce artist authority table from raw table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_artists_authority <- function(raw_artists_authority) {

  artists_authority <- raw_artists_authority %>%
    extract_artist_dates() %>%
    reconcile_artist_dates() %>%
    generate_artist_display_dates() %>%
    # Extract parentheticals from authority name
    bind_cols(re_match(.$artist_authority, " \\((?<parenthetical_text>[^I].+)\\)")) %>%
    mutate(artist_authority_clean = artist_authority) %>%
    mutate(artist_authority_clean = ifelse(is.na(.match), artist_authority_clean, str_replace(artist_authority_clean, fixed(.match), ""))) %>%
    select(-.match, -.text) %>%
    # "Unknown" nationality to NA
    rename(raw_nationality = nationality) %>%
    mutate(
      raw_nationality = na_if(raw_nationality, "Unknown"),
      raw_nationality = na_if(raw_nationality, "NEW")) %>%
    mutate(nationality = str_match(raw_nationality, "(\\w+)\\W?")[, 2]) %>%
    # Spot edits for two values where the simple regex cannot capture two
    # exceptional cases
    mutate(nationality = recode(nationality, New = "New Zealander", probably = "Flemish", South = "South African")) %>%
    # Select the final combination of columns to include in the table
    select(one_of(names(raw_artists_authority)), artist_early, artist_late, artist_display, artist_authority_clean, parenthetical_text)

  artists_authority
}

extract_artist_dates <- function(df) {
  message("- Extracting artist dates")
  df %>%
    # Artist birth and death dates are stored in strings in separate cells.
    # These functions break these strings out into prefixes, 4-digit integer
    # years, and suffixes.
    bind_re_match(birth_date, "^(?<birth_prefix>.+)?(?<birth_year>\\d{4})(?<birth_suffix>.+)?$") %>%
    bind_re_match(death_date, "^(?<death_prefix>.+)?(?<death_year>\\d{4})(?<death_suffix>.+)?$") %>%
    # Active periods for artists are generally denoted with one to two 4-digit
    # years. This function extracts the first, and second (if present) years into
    # separate columns
    bind_re_match(period_active, "(?<start_period>\\d{4}).?(?<end_period>\\d{4})?") %>%
    # In some cases, there is no active century, but that century (mostly for unidentified artists e.g. [GERMAN - 16TH C.] can be extracted from the authority name)
    bind_re_match(artist_authority, "(?<century_in_name>\\d{1,2})\\w{2} C") %>%
    # The lowest precision, but most prevalent attribute for artists is their
    # active century. Must parse 15 from 15th, for example
    bind_re_match(century_active, "(?<century_number>\\d{1,2})")
}

reconcile_artist_dates <- function(df) {
  message("- Reconciling artist dates")
  df %>%
    mutate(cent_bc = str_detect(century_active, regex("bc", ignore_case = TRUE))) %>%
    # Blank out any of the null references added by editors, like "|x"
    mutate_at(vars(dplyr::contains("birth"), dplyr::contains("death"), dplyr::contains("period"), dplyr::contains("century")), funs(if_else(. == "" | . == "|" | . == "x", NA_character_, .))) %>%
    # Convert all extracted years into integers
    mutate_at(vars(star_record_no, birth_year, death_year, start_period, end_period, century_in_name, century_number), funs(as.integer)) %>%
    mutate(century = if_else(is.na(century_number), century_in_name, century_number, missing = NA_integer_)) %>%
    # When the active century is marked as BC, make sure the century number is
    # made negative
    mutate(century = 100 * (ifelse(cent_bc, -1 * century, century) - 1)) %>%
    # Generate an upper and lower bound for the artist based on all available
    # information. If a life year is present, this will be the same number. If
    # not, the number is generated from the earliest year they were recorded to be
    # active. If activity period is unknown, then this bound is calculated from
    # their designated century.
    mutate(artist_early = as.integer(ifelse(is.na(birth_year), ifelse(is.na(start_period), century, start_period), birth_year)),
           artist_late = as.integer(ifelse(is.na(death_year), ifelse(is.na(end_period), century, end_period), death_year))) %>%
    # Bump the late century up to the end of the century if the artist's birth
    # falls inside it.
    mutate(artist_late = ifelse(artist_early >= artist_late, artist_late + 100, artist_late))
}

generate_artist_display_dates <- function(df) {
  message("- Generating display dates")
  df %>%
    # Produce a "display" early/late date
    mutate(artist_display = ifelse(
      !is.na(birth_year), paste(str_replace_na(birth_prefix, ""), birth_year, str_replace_na(birth_suffix, ""), "-", ifelse(!is.na(death_year), paste(str_replace_na(death_prefix, ""), death_year, str_replace_na(death_suffix, "")), "")), ifelse(!(is.na(start_period) & is.na(end_period)), paste("Active:", period_active), ifelse(!is.na(century), paste("Active:", century, "s", ifelse(cent_bc, "BCE", "CE")), "")))) %>%
    # Remove spaces between slash years and before s's
    mutate(
      artist_display = str_replace_all(artist_display, "(\\d) (/\\d|s|\\?)", "\\1\\2"))
}

#' Produce derivative artist authority table for ULAN
#'
#' @param source_dir Path where source RDS files are found
#' @param repo_path Path where derivative CSV should be saved
#'
#' @export
produce_ulan_derivative_artists <- function(source_dir, repo_path) {

  derivative <- get_data(source_dir, "artists_authority") %>%
    mutate(star_record_no = paste0("a", star_record_no)) %>%
    filter(!str_detect(artist_authority, "^\\[.*\\]") & !str_detect(artist_authority, "REJECTED"))

  write_csv(derivative, path = paste(repo_path, "derivatives", "ulan_load_artists.csv", sep = "/"))
}
