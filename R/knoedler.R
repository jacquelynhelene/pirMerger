produce_knoedler <- function(source_dir, target_dir) {
  raw_knoedler <- readRDS(paste(source_dir, "raw_knoedler.rds", sep = "/"))

  raw_knoedler %>%
    # Remove redundant columns imported from owners/artists authorities
    select(-art_authority_1, -nationality_1, -art_authority_2, -nationality_2) %>%
    mutate(
      has_share = !is.na(joint_own_1),
      is_commission = str_detect(main_heading, "[Cc]ommiss"),
      is_consignment = str_detect(main_heading, "[Cc]onsign")) %>%
    # Pull out numbers (including decimals) from price amount columns. This strips
    # off editorial brackets, as well as any trailing numbers such as shillings
    # and pence. Also applies the amonsieurx transformation.
    mutate(deciphered_purch = amonsieurx(purch_amount),
           purch_amount = ifelse(deciphered_purch == "", purch_amount, deciphered_purch)) %>%
    select(-deciphered_purch) %>%
    mutate_at(vars(purch_amount, knoedpurch_amt, price_amount), funs(as.numeric(str_match(., "(\\d+\\.?\\d*)")[,2]))) %>%
    mutate(
      divided_purch_amount = purch_amount / purch_amt_divisor,
      divided_knoedpurch_amount = knoedpurch_amt / purch_amt_divisor,
      divided_price_amount = price_amount / price_amt_divisor) %>%
    # Convert numeric strings into integers
    mutate_at(vars(star_record_no, stock_book_no, page_number, row_number, dplyr::contains("entry_date"), dplyr::contains("sale_date")), funs(as.integer)) %>%
    # Where genre or object type is not identified, set to NA
    mutate_at(vars(genre, object_type), funs(na_if(., "[not identified]"))) %>%
    # Where month or day components of entry or sale dates are 0, set to NA
    mutate_at(vars(dplyr::contains("day"), dplyr::contains("month")), funs(na_if(., 0))) %>%
    filter(is.na(sale_date_year) | sale_date_year < 1980) %>%
    # Parse fractions
    bind_re_match(dimensions, "(?<dimension1>\\d+ ?\\d*/?\\d*) ? ?\\[?x?X?\\]? ?(?<dimension2>\\d+ ?\\d*/?\\d*)?")
}

identify_knoedler_transactions <- function(df) {
  df %>%
    # Detect the number of artworks between which a given purchase/sale amt. was
    # split, first by standardizing all purchase/price notes, then finding matches.
    mutate_at(
      vars(purch_note, knoedpurch_note, price_note),
      funs(
        str_replace_all(., c(
          "(?:shared[,;])|(?:[;,] shared)|(?:shared)" = "",
          " ([&-]) " = "\\1",
          "&" = "-")) %>%
          str_trim() %>%
          na_if(""))) %>%
    # Prefer the purch note over the knoedpurch note, only falling back to
    # knoedpurch when reuglar note is NA
    mutate(purch_note = if_else(is.na(purch_note), knoedpurch_note, purch_note)) %>%
    select(-knoedpurch_note) %>%
    # Only keep those notes that refer to prices paid "for" n objects
    mutate_at(vars(purch_note, price_note),
              funs(if_else(str_detect(., regex("for", ignore_case = TRUE)), ., NA_character_))) %>%
    group_by(stock_book_no, purch_note) %>%
    mutate(
      purch_amt_divisor = n(),
      purch_amt_divisor = ifelse(is.na(purch_note), 1L, purch_amt_divisor)) %>%
    group_by(stock_book_no, price_note) %>%
    mutate(
      price_amt_divisor = n(),
      price_amt_divisor = ifelse(is.na(price_note), 1L, price_amt_divisor)) %>%
    ungroup()
}


