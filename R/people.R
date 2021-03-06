# All ULAN values exported as 0 should instead be set to NA
null_ulan <- function(df) {
  mutate_at(df, vars(contains("ulan")), funs(na_if(., "0")))
}

# Artists ----

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
    select(one_of(names(raw_artists_authority)), artist_early, artist_late, artist_display, artist_authority_clean, parenthetical_text) %>%
    mutate_at(vars(ulan_id), as.integer)

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

produce_gpi_nationality_aat <- function(raw_gpi_artist_nationality_aat) {
  raw_gpi_artist_nationality_aat %>%
    single_separate("aat_nationality", sep = ";") %>%
    mutate_at(vars(contains("aat_nationality")), funs(as.integer)) %>%
    select(-or_and)
}

# Owners ----

#' Produce owners authority table from raw table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_owners_authority <- function(raw_owners_authority) {
  owners_authority <- raw_owners_authority %>%
    extract_owner_dates() %>%
    reconcile_owner_dates() %>%
    generate_owner_display_dates() %>%
    # Extract preceding location - some names, particularly of museums, have the location ending in country code and a period.
    bind_re_match(owner_authority_clean, "^(?<location_from_name>.+?[A-Z]{2,}\\.) +\\w+") %>%
    mutate(owner_authority_clean = ifelse(is.na(location_from_name), owner_authority_clean, str_replace(owner_authority_clean, fixed(location_from_name), ""))) %>%
    # Recode the nationality column to standardize a handful of nonstandard values
    mutate(nationality = recode(nationality, "French ?" = "French", "Dutch?" = "Dutch", "British or French" = "British", "Unknown" = NA_character_)) %>%
    select(one_of(names(raw_owners_authority)), owner_early, owner_late, owner_display, owner_authority_clean, parenthetical_text, location_from_name) %>%
    mutate_at(vars(ulan_id), as.integer)

  owners_authority
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
produce_ulan_derivative_owners <- function(owners_authority) {

  derivative <- owners_authority %>%
    mutate(star_record_no = paste0("o", star_record_no))

  write_csv(derivative, path = paste(repo_path, "derivatives", "ulan_load_owners.csv", sep = "/"))
}

# Merged Authority ----

produce_combined_authority <- function(owners_authority, artists_authority) {
  renamed_owners <- owners_authority %>%
    select(
      authority_name = owner_authority,
      person_role = type,
      project,
      birth_date,
      death_date,
      active_dates,
      locations = owner_locations,
      nationality,
      address,
      source,
      brief_notes,
      working_notes,
      text,
      ulan_id,
      date_last_edited,
      early_date = owner_early,
      late_date = owner_late,
      display_date = owner_display,
      clean_authority_name = owner_authority_clean,
      parenthetical_text,
      location_from_name
    )

  renamed_artists <- artists_authority %>%
    select(
      authority_name = artist_authority,
      birth_date,
      death_date,
      active_dates = period_active,
      century_active,
      locations = active_city_date,
      nationality,
      school,
      subjects_painted,
      source = source_of_name,
      medal_received,
      bha_rila,
      working_notes = notes,
      ulan_id,
      message,
      date_last_edited,
      early_date = artist_early,
      late_date = artist_late,
      display_date = artist_display,
      clean_authority_name = artist_authority_clean,
      parenthetical_text
    )

  combined_authority <- bind_rows(
    owners = renamed_owners,
    artists = renamed_artists,
    .id = "authority_source"
  )
}

# Takes tables from multiple database sources and positions (artist, buyer,
# etc.) and computes unique IDs across all datasets. Input tables should contain
# the following columns:
# - source_record_id (e.g. star_record_no, puri)
# - source_document_id (document-level aggregation to be used when creating
# persons based on their contexts in a document. For example, an unidentified
# buyer with the same name across one sales catalog may be treated as an
# individual, even when no other identity information is known outside the
# context of that sales catalog)
# - person_name ("verbatim" string information for that artist in the context of the source record)
# - person_auth ("authorized" name)
# - person_ulan (known ULAN ID, if such an id has been entered into the original STAR record)
# - id_process (Whether ID should be derived from ULAN id, from the verbatim
# name, from "nothing" [given an unrepeated id], from authority name, or from
# authority name WITH a document grouping provision)
produce_union_person_ids <- function(..., combined_authority, nationality_aat) {
  list(...) %>%
    map(function(x) mutate_at(x, vars(source_record_id), funs(as.character))) %>%
    bind_rows(.id = "source_db") %>%
    assertr::assert(assertr::in_set("from_ulan", "from_name", "from_nothing", "from_auth")) %>%
    mutate(
      person_uid = case_when(
        id_process == "from_ulan" ~ paste0("ulan-person-", group_indices(., person_ulan)),
        id_process == "from_name" ~ paste0("unauthorized-person-", group_indices(., person_name)),
        id_process == "from_nothing" ~ paste0("anon-person-", seq_along(person_auth)),
        id_process == "from_auth" ~ paste0("known-person-", group_indices(., person_auth)),
        id_process == "from_auth_grouped" ~ paste0("known-person-indoc-", group_indices(., person_auth, source_document_id)))) %>%
    left_join(select(combined_authority, authority_name, birth_date, death_date, early_date, late_date, nationality), by = c("person_auth" = "authority_name")) %>%
    mutate(joining_nationality = if_else(is.na(nationality), if_else(person_auth %in% nationality_aat[["nationality_name"]], person_auth, NA_character_), nationality)) %>%
    left_join(nationality_aat, by = c("joining_nationality" = "nationality_name")) %>%
    mutate_at(vars(contains("date")), as.character) %>%
    mutate(
      # Favor the explicit dates given by authority, otherwise fall back to dates listed in concordance
      active_early = if_else(is.na(early_date), start_year, early_date),
      active_late = if_else(is.na(late_date), end_year, late_date)) %>%
    select(source_db,
           source_record_id,
           source_document_id,
           id_process,
           person_name,
           person_auth,
           person_ulan,
           person_uid,
           person_birth_date = birth_date,
           person_death_date = death_date,
           person_active_early = active_early,
           person_active_late = active_late,
           person_nationality = joining_nationality,
           contains("aat_nationality")) %>%
    distinct() %>%
    mutate(person_name_id = group_indices(., person_name, person_uid))
}

produce_generic_artists <- function(raw_generic_artists) {
  raw_generic_artists %>%
    # Fill back in empty artist_authority cells
    group_by(star_record_no) %>%
    mutate(artist_authority = pick(artist_authority)) %>%
    ungroup() %>%
    # Keep only those identities selected by editors
    filter(selected == "x") %>%
    select(
      generic_artist_star_record_no = star_record_no,
      generic_artist_authority = artist_authority,
      generic_artist_ulan_id = Vocab_ID)
}
