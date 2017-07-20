#' Produce Knoedler table from raw table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_knoedler <- function(source_dir, target_dir) {
  raw_knoedler <- get_data(source_dir, "raw_knoedler")

  knoedler_stocknumber_concordance <- produce_knoedler_stocknumber_concordance(source_dir, target_dir)

  knoedler <- raw_knoedler %>%
    # Convert numeric strings into integers
    mutate_at(vars(star_record_no, stock_book_no, page_number, row_number, dplyr::contains("entry_date"), dplyr::contains("sale_date")), funs(as.integer)) %>%
    parse_knoedler_monetary_amounts() %>%
    identify_knoedler_objects(knoedler_stocknumber_concordance) %>%
    identify_knoedler_transactions() %>%
    order_knoedler_object_events() %>%
    add_count(object_id) %>%
    arrange(desc(n), object_id, event_order) %>%
    # Where genre or object type is not identified, set to NA
    mutate_at(vars(genre, object_type), funs(na_if(., "[not identified]"))) %>%
    # Where month or day components of entry or sale dates are 0, set to NA
    mutate_at(vars(dplyr::contains("day"), dplyr::contains("month")), funs(na_if(., 0))) %>%
    # Parse fractions
    bind_re_match(dimensions, "(?<dimension1>\\d+ ?\\d*/?\\d*) ? ?\\[?x?X?\\]? ?(?<dimension2>\\d+ ?\\d*/?\\d*)?")

  message("- Extracting knoedler_artists")
  knoedler_artists <- norm_vars(knoedler, base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "star_rec_no"), n_reps = 2, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(artist_name_1:star_rec_no_2))
  saveRDS(knoedler_artists, paste(target_dir, "knoedler_artists.rds", sep = "/"))

  message("- Extracting knoedler_sellers")
  knoedler_sellers <- norm_vars(knoedler, base_names = c("seller_name", "seller_loc", "sell_auth_name", "sell_auth_loc"), n_reps = 2, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(seller_name_1:sell_auth_loc_2))
  saveRDS(knoedler_sellers, paste(target_dir, "knoedler_sellers.rds", sep = "/"))

  message("- Extracting knoedler_joint_owners")
  knoedler_joint_owners <- norm_vars(knoedler, base_names = c("joint_own", "joint_own_sh"), n_reps = 4, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(joint_own_1:joint_own_sh_4))
  saveRDS(knoedler_joint_owners, paste(target_dir, "knoedler_joint_owners.rds", sep = "/"))

  message("- Extracting knoedler_buyers")
  knoedler_buyers <- knoedler %>%
    add_column(buyer_loc_2 = NA_character_, buy_auth_addr_2 = NA_character_) %>%
    norm_vars(base_names = c("buyer_name", "buyer_loc", "buy_auth_name", "buy_auth_addr"), n_reps = 2, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(buyer_name_1:buy_auth_name_2))
  saveRDS(knoedler_buyers, paste(target_dir, "knoedler_buyers.rds", sep = "/"))

  saveRDS(knoedler, paste(target_dir, "knoedler.rds", sep = "/"))
  invisible(knoedler)
}

identify_knoedler_transactions <- function(df) {
  df %>%
    # Detect the number of artworks between which a given purchase/sale amt. was
    # split, first by standardizing all purchase/price notes, then finding matches.
    mutate_at(
      vars(purch_note, knoedpurch_note, price_note),
      funs(working =
        str_replace_all(., c(
          "(?:shared[,;])|(?:[;,] shared)|(?:shared)" = "",
          " ([&-]) " = "\\1",
          "&" = "-")) %>%
          str_trim() %>%
          na_if(""))) %>%
    # Prefer the purch note over the knoedpurch note, only falling back to
    # knoedpurch when reuglar note is NA
    mutate(purch_note_working = if_else(is.na(purch_note_working), knoedpurch_note_working, purch_note_working)) %>%
    # Only keep those notes that refer to prices paid "for" n objects
    mutate_at(vars(purch_note_working, price_note_working),
              funs(if_else(str_detect(., regex("for", ignore_case = TRUE)), ., NA_character_))) %>%
    # Fill in null purchase notes with dummy ids
    mutate_at(vars(purch_note_working, price_note_working), funs(if_else(is.na(.), as.character(seq_along(.)), .))) %>%
    # Produce a unique id for unique pairings of purchase notes
    # TO-DO: distinguish between inventory events and new purchase events
    mutate(
      inventory_or_purchase_id = paste("k", "purch", group_indices(., stock_book_no, purch_note_working), sep = "-"),
      sale_transaction_id = paste("k", "sale", group_indices(., stock_book_no, price_note_working), sep = "-"))
}

produce_knoedler_stocknumber_concordance <- function(source_dir, target_dir) {
  raw_knoedler_stocknumber_concordance <- readRDS(paste(source_dir, "raw_knoedler_stocknumber_concordance.rds", sep = "/"))

  # Take the
  knoedler_stocknumber_concordance <- raw_knoedler_stocknumber_concordance %>%
    select(sn1, sn2, sn3, sn4) %>%
    mutate(prime_stock_number = sn1) %>%
    gather(number_index, stock_number, sn1:sn4, na.rm = TRUE) %>%
    group_by(stock_number) %>%
    summarize(prime_stock_number = first(prime_stock_number))

  saveRDS(knoedler_stocknumber_concordance, file = paste(target_dir, "knoedler_stocknumber_concordance.rds", sep = "/"))
  invisible(knoedler_stocknumber_concordance)
}

# Produce unique ids for knoedler objects based on their stock numbers
identify_knoedler_objects <- function(df, knoedler_stocknumber_concordance) {
  df %>%
    left_join(knoedler_stocknumber_concordance, by = c("knoedler_number" = "stock_number")) %>%
    # Because some of the knoedler stock numbers changed or were re-used, we
    # will consult against a stock number concordance that we can use to create
    # a "functional" stock number - an identifier that connects objects even
    # when their nominal stock numbers are different. Those entries without any
    # stock nubmers at all are assumed to be standalone objects, and given a
    # unique id.
    mutate(
      prepped_sn = case_when(
      is.na(knoedler_number) ~ paste("gennum", as.character(seq_along(knoedler_number)), sep = "-"),
      knoedler_number %in% names(knoedler_stocknumber_concordance) ~ prime_stock_number,
      TRUE ~ paste("orignnum", knoedler_number, sep = "-"))) %>%
    mutate(object_id = paste("k", "object", group_indices(., prepped_sn), sep = "-"))
}

# For a given object_id, attempt to discern an event order, which can be useful
# for discerning timespand boundaries as well as figuring out when an object
# first entered, and then finally left, knoedler's collection.
order_knoedler_object_events <- function(df) {
  df %>%
    # Use the entry date as the primary index of event date, falling back to the sale date if the entry date is not available.
    mutate(
      event_year = case_when(
        is.na(entry_date_year) ~ sale_date_year,
        TRUE ~ entry_date_year),
      event_month = case_when(
        is.na(entry_date_month) ~ sale_date_month,
        TRUE ~ entry_date_month),
      event_day = case_when(
        is.na(entry_date_day) ~ sale_date_day,
        TRUE ~ entry_date_day)) %>%
    # Produce an index per object_id based on this event year/month/day, falling
    # back to position in stock book if there is no year.
    group_by(object_id) %>%
    arrange(event_year, event_month, event_day, stock_book_no, page_number, row_number, .by_group = TRUE) %>%
    mutate(event_order = seq_along(star_record_no)) %>%
    ungroup()
}

# Harmonize monetary amounts and currencies
parse_knoedler_monetary_amounts <- function(df) {
  df %>%
    # Pull out numbers (including decimals) from price amount columns. This strips
    # off editorial brackets, as well as any trailing numbers such as shillings
    # and pence. Also applies the amonsieurx transformation.
    mutate(deciphered_purch = amonsieurx(purch_amount),
           purch_amount = ifelse(deciphered_purch == "", purch_amount, deciphered_purch)) %>%
    select(-deciphered_purch) %>%
    mutate_at(vars(purch_amount, knoedpurch_amt, price_amount), funs(as.numeric(str_match(., "(\\d+\\.?\\d*)")[,2])))
}

produce_knoedler_materials_AAT <- function(source_dir, target_dir) {
  raw_knoedler_materials_aat <- get_data(source_dir, "raw_knoedler_materials_aat")

  message("- Concordance for object materials")
  knoedler_materials_object_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_materials = made_of_materials) %>%
    single_separate(source_col = "aat_materials") %>%
    gather(gcol, aat_materials, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_materials), as.integer)
  save_data(target_dir, knoedler_materials_object_aat)

  message("- Concordance for support materials")
  knoedler_materials_support_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_support = made_of_support) %>%
    single_separate(source_col = "aat_support") %>%
    gather(gcol, aat_support, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_support), as.integer)
  save_data(target_dir, knoedler_materials_support_aat)

  message("- Concordance for classified_as tags")
  knoedler_materials_classified_as_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, classified_as_1, classified_as_2) %>%
    single_separate(source_col = "classified_as_1") %>%
    single_separate(source_col = "classified_as_2") %>%
    gather(gcol, aat_support, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_support), as.integer)
  save_data(target_dir, knoedler_materials_classified_as_aat)

  message("- Concordance for techniques")
  knoedler_materials_technique_as_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_technique = technique) %>%
    single_separate(source_col = "aat_technique") %>%
    gather(gcol, aat_technique, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_technique), as.integer)
  save_data(target_dir, knoedler_materials_technique_as_aat)
}

produce_joined_knoedler <- function(source_dir, target_dir) {
  knoedler <- get_data(source_dir, "knoedler")
  knoedler_artists <- get_data(source_dir, "knoedler_artists")
  knoedler_buyers <- get_data(source_dir, "knoedler_buyers")
  knoedler_sellers <- get_data(source_dir, "knoedler_sellers")
  knoedler_joint_owners <- get_data(source_dir, "knoedler_joint_owners")
  artists_authority <- get_data(source_dir, "artists_authority")
  owners_authority <- get_data(source_dir, "owners_authority")
  knoedler_materials_classified_as_aat <- get_data(source_dir, "knoedler_materials_classified_as_aat")
  knoedler_materials_object_aat <- get_data(source_dir, "knoedler_materials_object_aat")
  knoedler_materials_support_aat <- get_data(source_dir, "knoedler_materials_support_aat")
  knoedler_materials_technique_as_aat <- get_data(source_dir, "knoedler_materials_technique_as_aat")

  knoedler %>%
    left_join(knoedler_artists, from = "star_record_no", to = "star_record_no", n_reps = 2, prefix = "")
}
