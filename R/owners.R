#' Produce owners authority table from raw table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_owners_authority <- function(source_dir, target_dir) {
  message("Producing owners authority (" , source_dir, "/raw_owners_authority.rds to ", target_dir, "/owners_authority.rds", ")")

  raw_owners_authority <- get_data(source_dir, "raw_owners_authority")

  owners_authority <- raw_owners_authority %>%
    extract_owner_dates() %>%
    reconcile_owner_dates() %>%
    generate_owner_display_dates() %>%
    # Extract preceding location - some names, particularly of museums, have the location ending in country code and a period.
    bind_re_match(owner_authority_clean, "^(?<location_from_name>.+?[A-Z]{2,}\\.) +\\w+") %>%
    mutate(owner_authority_clean = ifelse(is.na(location_from_name), owner_authority_clean, str_replace(owner_authority_clean, fixed(location_from_name), ""))) %>%
    # Recode the nationality column to standardize a handful of nonstandard values
    mutate(nationality = recode(nationality, "French ?" = "French", "Dutch?" = "Dutch", "British or French" = "British", "Unknown" = NA_character_)) %>%

    select(one_of(names(raw_owners_authority)), owner_early, owner_late, owner_display, owner_authority_clean, parenthetical_text, location_from_name)

  save_data(target_dir, owners_authority)
}

extract_owner_dates <- function(df) {
  message("- Extracting owner dates")

  df %>%
    # Extract years from birth_date, death_date, and active_period. Where there
    # are no life dates, the active period will be used as a fallback.
    bind_re_match(birth_date, "^(?<birth_prefix>.+)?(?<birth_year>\\d{4})(?<birth_suffix>.+)?$") %>%
    bind_re_match(death_date, "^(?<death_prefix>.+)?(?<death_year>\\d{4})(?<death_suffix>.+)?$") %>%
    # Active periods for artists are generally denoted with one to two 4-digit
    # years. This function extracts the first, and second (if present) years into
    # separate columns
    bind_re_match(active_dates, "(?<start_period>\\d{4}).?(?<end_period>\\d{4})?") %>%
    # Get centuries out of active dates
    bind_re_match(active_dates, "(?<century>\\d{2}).+ c")
}

reconcile_owner_dates <- function(df) {
  message("- Reconciling owner dates")
  df %>%
    # For each extracted column, ensure blank or 0-length whitespace is set to NA
  mutate_at(vars(dplyr::contains("birth"), dplyr::contains("death"), dplyr::contains("period")), funs(na_if(str_trim(.), ""))) %>%
    mutate_at(vars(star_record_no, birth_year, death_year, start_period, end_period, century), funs(as.integer)) %>%
    mutate(
      century = (century - 1) * 100,
      owner_early = ifelse(is.na(birth_year), ifelse(is.na(start_period), century, start_period), birth_year),
      owner_late = ifelse(is.na(death_year), ifelse(is.na(end_period), century + 100, end_period), death_year))
}

generate_owner_display_dates <- function(df) {
  message("- Generating display dates")
  df %>%
    mutate(owner_display = ifelse(!is.na(birth_year), paste(str_replace_na(birth_prefix, ""), birth_year, str_replace_na(birth_suffix, ""), "-", ifelse(!is.na(death_year), paste(str_replace_na(death_prefix, ""), death_year, str_replace_na(death_suffix, "")), "")), ifelse(!(is.na(start_period) & is.na(end_period)), paste0("Active: ", active_dates), ifelse(!is.na(century), paste("Active: ", century, "s"), "")))) %>%
    mutate(
      owner_display_clean = owner_display,
      owner_display_clean = str_replace_all(owner_display_clean, "(\\d) (/\\d|s|\\?)", "\\1\\2")) %>%
    # Extract parenthetical text
    mutate(owner_authority_clean = owner_authority) %>%
    bind_cols(re_match(.$owner_authority_clean, " \\((?<parenthetical_text>.+)\\)")) %>%
    mutate(owner_authority_clean = ifelse(is.na(.match), owner_authority_clean, str_replace(owner_authority_clean, fixed(.match), ""))) %>%
    select(-.match, -.text)
}

#' Produce derivative owner authority table for ULAN
#'
#' @param source_dir Path where source RDS files are found
#' @param repo_path Path where derivative CSV should be saved
#'
#' @export
produce_ulan_derivative_owners <- function(source_dir, repo_path) {

  derivative <- get_data(source_dir, "owners_authority") %>%
    mutate(star_record_no = paste0("o", star_record_no))

  write_csv(derivative, path = paste(repo_path, "derivatives", "ulan_load_owners.csv", sep = "/"))
}
