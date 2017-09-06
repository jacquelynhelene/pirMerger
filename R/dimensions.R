#' Create a tidy table of extracted dimensions
#'
#' @return A data frame with an id column for joining to original table,
#'
#' @import stringr
#'
#' @export
general_dimension_extraction <- function(df, dimcol, idcol) {
  df <- df[which(!is.na(df[[dimcol]])),]
  tryd <- rematch2::re_match_all(df[[dimcol]],
               # Find any combo of acceptable
               # value/unit/dimension chars IFF there is
               # at least one digit in the bunch
               pattern = "(?<dim>[0-9 '\"/\\.lhwd]*(?:cm)*\\d+[0-9 '\"/\\.lhwd]*(?:cm)*)") %>%
    add_column(star_record_no = df[[idcol]]) %>%
    unnest() %>%
    # Extract the dimension marker to its own column, leaving only the dimension value with its unit markers
    mutate(dim = str_replace(dim, "w w", "w")) %>%
    mutate(dimtype = as.factor(str_trim(str_extract(dim, "( [lhwd] | [lhwd]$)\\.?")))) %>%
    mutate(value_wo_dim = str_trim(str_replace_all(dim, c("( [lhwd] | [lhwd]$)\\.?" = "", " +" = " ")))) %>%
    # Pull out mixed & decimal numbers from their unit markers
    rematch2::bind_re_match(value_wo_dim, "^(?<dim_d1>[0-9]{1,2}/[0-9]{1,2}|[0-9\\.]+(?: [0-9]{1,2}/[0-9]{1,2})?)(?<dim_c1>[^0-9\\./]*)(?<dim_d2>[0-9]{1,2}/[0-9]{1,2}|[0-9\\.]*(?: [0-9]{1,2}/[0-9]{1,2})?)(?<dim_c2>[^0-9\\./]*)") %>%
    mutate_at(vars(dim_d1, dim_c1, dim_d2, dim_c2), funs(na_if(str_trim(.), ""))) %>%
    mutate_at(vars(dim_d1, dim_d2), funs(parsed = parse_fraction)) %>%
    mutate_at(vars(dim_c1, dim_c2), as.factor)
}
