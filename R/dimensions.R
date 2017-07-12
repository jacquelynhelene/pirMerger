#' Given a dimensions string, returns a tidy table with values, units, and dimension types.
#'
#' @param s Character. Dimension strings to parse.
#'
#' @return A dataframe
#'
#' @import stringr
#' @importFrom rematch2 re_match_all bind_re_match
#'
#' @export
parse_dimensions <- function(s) {
  extracted_dimensions <- general_dimension_extraction(s)


}

general_dimension_extraction <- function(s) {
  re_match_all(s,
               # Find any combo of acceptable
               # value/unit/dimension chars IFF there is
               # at least one digit in the bunch
               "(?<dim>[0-9 '\"/\\.lhwd]*(?:cm)*\\d+[0-9 '\"/\\.lhwd]*(?:cm)*)") %>%
    unnest() %>%
    # Extract the dimension marker to its own column, leaving only the dimension value with its unit markers
    mutate(dim = str_replace(dim, "w w", "w")) %>%
    mutate(dimtype = as.factor(str_trim(str_extract(dim, "( [lhwd] | [lhwd]$)\\.?")))) %>%
    mutate(value_wo_dim = str_trim(str_replace_all(dim, "( [lhwd] | [lhwd]$)\\.?", ""))) %>%
    # Pull out mixed & decimal numbers from their unit markers
    bind_re_match(value_wo_dim, "(?<dim_d1>[0-9\\.\\/]+(?: ?[0-9]\\/[0-9])?)(?<dim_c1>\\D*)(?<dim_d2>[0-9\\.]*(?: ?[0-9]\\/[0-9])?)(?<dim_c2>\\D*)") %>%
    mutate_at(vars(dim_d1, dim_c1, dim_d2, dim_c2), funs(na_if(str_trim(.), ""))) %>%
    mutate_at(vars(dim_d1, dim_d2), funs(parsed = parse_fraction)) %>%
    mutate_at(vars(dim_c1, dim_c2), as.factor)
}

general_parse_fraction <- function(df) {
  df %>%
    group_by(star_record_no) %>%
    mutate_at(vars(dim_c1, dim_c2), funs(first = if_else(is.na(first(.)), last(.), first(.)))) %>%
    ungroup() %>%
    mutate(
      dim_c1 = if_else(is.na(dim_c1) & !is.na(dim_d1), dim_c1_first, dim_c1),
      dim_c2 = if_else(is.na(dim_c2) & !is.na(dim_d2), dim_c2_first, dim_c2)) %>%
    ungroup() %>%
    mutate(dim_value = compile_inches(dim_d1, dim_c1, dim_d2, dim_c2))
}
