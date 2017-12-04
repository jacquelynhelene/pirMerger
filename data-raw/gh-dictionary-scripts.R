convert_table <- function(table_url) {
  options(knitr.kable.NA = '')
  googlesheets::gs_read(googlesheets::gs_url(table_url)) %>%
    select(field_name, data_type, missing_values, field_description, field_example, public_notes) %>%
    mutate_all(funs(str_replace_all(., "\\n", "; "))) %>%
    mutate_at(vars(field_name), funs(paste0("`", ., "`"))) %>%
    kable()
}

clipr_convert_table <- function() {
  write_clip(convert_table(read_clip()))
}
