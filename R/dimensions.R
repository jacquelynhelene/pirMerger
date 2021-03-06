# Create a tidy table of extracted dimensions
#
# dimcol - name of column containing original dimension string
# idcol - name of column contianing uid for given database record (e.g. star_record_no or puri)
# exclusion_col - name of column with logical flag stating whether that
# dimension ought to be parsed. Any dimension that has a missing value, or which
# has been flagged for exclusion, will not be parsed at all.
general_dimension_extraction <- function(df, dimcol, idcol, exclusion_col) {
  # Exclude from parsing all records that have been marked as excluded. This
  # exclusion column should be defined on a per-database basis (e.g. sales
  # contents may have different methods for defining exclusions than archival
  # inventories)
  df <- df[which(!is.na(df[[dimcol]]) & !df[[exclusion_col]]),]

  tryd <- rematch2::re_match_all(df[[dimcol]],
                                 # Find any combo of acceptable
                                 # value/unit/dimension chars IFF there is
                                 # at least one digit in the bunch
                                 pattern = "(?<dim>[0-9 '\"/\\.lhwd]*(?:cm)*\\d+[0-9 '\"/\\.lhwd]*(?:cm)*)")

  tryd[[idcol]] <- df[[idcol]]

  tryd <- tryd %>%
    unnest() %>%
    mutate(dim = str_replace(dim, "w w", "w")) %>%
    # Do not attempt to parse any extracted dimension containing more than one
    # dimensions type marker. This indicates that extraction has failed, and the
    # parse will not work.
    filter(str_count(dim, "[lwhd]") <= 1) %>%
    # Extract the dimension marker to its own column, leaving only the dimension value with its unit markers
    mutate(dimtype = as.factor(str_trim(str_extract(dim, "(^[lhwd] | [lhwd] | [lhwd]$)\\.?")))) %>%
    mutate(value_wo_dim = str_trim(str_replace_all(dim, c("(^[lhwd] | [lhwd] | [lhwd]$)\\.?" = "", " +" = " ")))) %>%
    # Pull out mixed & decimal numbers from their unit markers
    rematch2::bind_re_match(value_wo_dim, "^(?<dim_d1>[0-9]{1,2}/[0-9]{1,2}|[0-9\\.]+(?: [0-9]{1,2}/[0-9]{1,2})?)(?<dim_c1>[^0-9\\./]*)(?<dim_d2>[0-9]{1,2}/[0-9]{1,2}|[0-9\\.]*(?: [0-9]{1,2}/[0-9]{1,2})?)(?<dim_c2>[^0-9\\./]*)") %>%
    mutate_at(vars(dim_d1, dim_c1, dim_d2, dim_c2), funs(na_if(str_trim(.), ""))) %>%
    mutate_at(vars(dim_d1, dim_d2), funs(parsed = parse_fraction)) %>%
    mutate_at(vars(dim_c1, dim_c2), as.factor) %>%
    mutate(decimalized_dim_value = compile_inches(dim_d1_parsed, dim_c1, dim_d2_parsed, dim_c2)) %>%
    group_by(.dots = idcol) %>%
    mutate(dimension_order = row_number()) %>%
    ungroup()
}

# Return whether a string matches any in a vector of patterns
str_any <- function(string, patterns) {
  map_lgl(string, function(x) {
    match_res <- map_lgl(patterns, function(y) str_detect(x, y))
    any(match_res)
  })
}

#' Given a two pairs of values and feet/inches, parse
#'
#' If first is explicity feet, assume feet. Otherwise, assume inches. Note that,
#' because if() statements cannot be NA, we have to check cases in decreasing
#' order of NA-ness, so each must evaluate correctly and filter out any cases
#' that would break later statements.
#'
#' @param d1 Numeric.
#' @param c2 Character.
#' @param d2 Numeric
#' @param c2 Character.
#'
#' @return Numeric.
#' @export
compile_inches <- function(d1, c1, d2, c2) {
  case_when(
    is.na(d1) & is.na(d2) & is.na(c1) & is.na(c2) ~ NA_real_,
    !is.na(d1) & is.na(d2) & is.na(c1) & is.na(c2) ~ d1,
    !is.na(d1) & is.na(d2) & c1 == "\'" & is.na(c2) ~ d1 * 12,
    !is.na(d1) & is.na(d2) & c1 == "\"" & is.na(c2) ~ d1,
    !is.na(d1) & !is.na(d2) & is.na(c1) & c2 == "\"" ~ d1 * 12 + d2,
    !is.na(d1) & !is.na(d2) & c1 == "\"" & is.na(c2) ~ d1,
    !is.na(d1) & !is.na(d2) & c1 == "\'" & is.na(c2) ~ d1 * 12 + d2,
    !is.na(d1) & !is.na(d2) & c1 == "\'" & c2 == "\'" ~ d1 * 12 + d2,
    !is.na(d1) & !is.na(d2) & c1 == "'" & c2 == "\"" ~ d1 * 12 + d2,
    TRUE ~ NA_real_)
}
