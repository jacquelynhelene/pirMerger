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
  artists_authority <- get_data(target_dir, "artists_authority")
  owners_authority <- get_data(target_dir, "owners_authority")

  knoedler_stocknumber_concordance <- produce_knoedler_stocknumber_concordance(source_dir, target_dir)

  knoedler <- raw_knoedler %>%
    # Convert numeric strings into integers
    mutate_at(vars(star_record_no, stock_book_no, page_number, row_number, dplyr::contains("entry_date"), dplyr::contains("sale_date")), funs(as.integer))

  message("- Extracting knoedler_artists")
  knoedler_artists <- norm_vars(knoedler, base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "star_rec_no", "artist_ulan_id"), n_reps = 2, idcols = "star_record_no") %>%
    rename(artist_star_record_no = star_rec_no) %>%
    # Join ulan ids to this list
    rename(artist_authority = art_authority, artist_nationality = nationality, artist_attribution_mod = attribution_mod) %>%
    # Generate unique IDs for all artists mentioned here
    identify_knoedler_anonymous_artists()
  knoedler <- knoedler %>%
    select(-(artist_name_1:artist_ulan_id_2))
  saveRDS(knoedler_artists, paste(target_dir, "knoedler_artists.rds", sep = "/"))

  message("- Extracting knoedler_sellers")
  knoedler_sellers <- norm_vars(knoedler, base_names = c("seller_name", "seller_loc", "sell_auth_name", "sell_auth_loc", "seller_ulan_id"), n_reps = 2, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(seller_name_1:seller_ulan_id_2))

  message("- Extracting knoedler_joint_owners")
  knoedler_joint_owners <- norm_vars(knoedler, base_names = c("joint_own", "joint_own_sh", "joint_ulan_id"), n_reps = 4, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(joint_own_1:joint_ulan_id_4))
  saveRDS(knoedler_joint_owners, paste(target_dir, "knoedler_joint_owners.rds", sep = "/"))

  message("- Extracting knoedler_buyers")
  knoedler_buyers <- knoedler %>%
    norm_vars(base_names = c("buyer_name", "buyer_loc", "buy_auth_name", "buy_auth_addr", "buyer_ulan_id"), n_reps = 2, idcols = "star_record_no")
  knoedler <- knoedler %>%
    select(-(buyer_name_1:buyer_ulan_id_2))

  # Add owner uids
  message("- Identifying Knoedler owners")
  owner_uids <- identify_knoedler_anonymous_owners(buyers_df = knoedler_buyers, sellers_df = knoedler_sellers)

  knoedler_buyers <- knoedler_buyers %>%
    bind_cols(select(filter(owner_uids, owner_type == "buyers"), buyer_uid = person_uid))

  knoedler_sellers <- knoedler_sellers %>%
    bind_cols(select(filter(owner_uids, owner_type == "sellers"), seller_uid = person_uid))

  saveRDS(knoedler_buyers, paste(target_dir, "knoedler_buyers.rds", sep = "/"))
  saveRDS(knoedler_sellers, paste(target_dir, "knoedler_sellers.rds", sep = "/"))

  message("- Extracting Knoedler dimensions")
  produce_knoedler_dimensions(source_dir, target_dir, kdf = knoedler)

  # produce_knoedler_transactions(source_dir, target_dir, kdf = knoedler)

  knoedler <- knoedler %>%
    parse_knoedler_monetary_amounts() %>%
    identify_knoedler_transactions() %>%
    order_knoedler_object_events() %>%
    # Where genre or object type is not identified, set to NA
    mutate_at(vars(genre, object_type), funs(na_if(., "[not identified]"))) %>%
    # Where month or day components of entry or sale dates are 0, set to NA
    mutate_at(vars(dplyr::contains("day"), dplyr::contains("month")), funs(na_if(., 0)))

  produce_knoedler_materials_aat(source_dir, target_dir, kdf = knoedler)
  produce_knoedler_subject_aat(source_dir, target_dir, kdf = knoedler)

  saveRDS(knoedler, paste(target_dir, "knoedler.rds", sep = "/"))
  invisible(knoedler)
}

produce_knoedler_dimensions <- function(source_dir, target_dir, kdf) {
  dimensions_aat <- get_data(source_dir, "raw_dimensions_aat")
  units_aat <- get_data(source_dir, "raw_units_aat")
  knoedler_dimensions <- general_dimension_extraction(kdf, "dimensions", "star_record_no") %>%
    mutate(
      dimension_unit = case_when(
        # Default to inches
        is.na(dim_c1) ~ "inches",
        dim_c1 == "\"" ~ "inches",
        dim_c1 == "cm" ~ "centimeters",
        TRUE ~ "inches"),
      dimension_type = case_when(
        is.na(dimtype) & dimension_order == 1 ~ "width",
        is.na(dimtype) & dimension_order == 2 ~ "height",
        is.na(dimtype) & dimension_order > 2 ~ NA_character_,
        dimtype == "h" ~ "height",
        dimtype == "w" ~ "width",
        TRUE ~ NA_character_
      )
    ) %>%
    inner_join(dimensions_aat, by = c("dimension_type" = "dimension")) %>%
    inner_join(units_aat, by = c("dimension_unit" = "unit")) %>%
    select(star_record_no, dimension_value = dim_d1_parsed, dimension_unit, dimension_unit_aat = unit_aat, dimension_type, dimension_type_aat = dimension_aat)

  save_data(target_dir, knoedler_dimensions)
}

produce_knoedler_transactions <- function(source_dir, target_dir, kdf) {
  knoedler_sellers <- get_data(source_dir, "knoedler_sellers")
  knoedler_buyers <- get_data(source_dir, "knoedler_buyers")
  knoedler_joint_owners <- get_data(source_dir, "knoedler_joint_owners")

  knoedler_firm_id <- "500304270"

  knoedler_transactions <- kdf %>%
    identify_knoedler_objects(knoedler_stocknumber_concordance) %>%
    identify_knoedler_transactions() %>%
    order_knoedler_object_events()

  # Purchase events - for each purchase event, isolate the price coming from the
  # buyers, the sellers from whom custody is being transferred, and the buyers
  # to whom custody is being transferred
  joint_payment <- knoedler_joint_owners %>%
    inner_join(select(knoedler_transactions, star_record_no, purchase_event_id), by = "star_record_no") %>%
    select(-star_record_no) %>%
    distinct() %>%
    spread_out("purchase_event_id") %>%
    set_names(paste("purchase", names(.), sep = "_")) %>%
    rename(purchase_event_id = purchase_purchase_event_id)

  payment_from_buyers <- knoedler_transactions %>%
    group_by(purchase_event_id) %>%
    summarize_at(
      vars(purch_amount,
      purch_currency,
      knoedpurch_amt,
      knoedpurch_curr),
      funs(first(na.omit(.)))
    ) %>%
    left_join(joint_payment, by = "purchase_event_id")

  ownership_from <- knoedler_sellers %>%
    left_join(select(knoedler_transactions, star_record_no, purchase_event_id), by = "star_record_no") %>%
    select(purchase_event_id, seller_uid) %>%
    distinct() %>%
    spread_out("purchase_event_id")

  knoedler_purchases <- payment_from_buyers %>%
    left_join(ownership_from, by = "purchase_event_id")

  save_data(target_dir, knoedler_purchases)


  # Generate table of Knoedler transactions:
  # - Transfer of payment to sellers
  # - Transfer of payment from buyers
  # - Transfer of custody from sellers
  # - Transfer of custody to buyers
  knoedler_sales <- knoedler_transactions %>%
    filter(transaction == "Sold")

  sale_transactions <- knoedler_sales %>%
    group_by(sale_transaction_id) %>%
    summarize_at(vars(sale_date_year, sale_date_month, sale_date_day, price_amount, price_currency),
                 funs(first(na.omit(.))))

  sales_to <- knoedler_sales %>%
    left_join(knoedler_buyers, by = "star_record_no") %>%
    select(sale_transaction_id, buyer_uid) %>%
    filter(!is.na(buyer_uid)) %>%
    distinct()

  sales_from <- knoedler_sales %>%
    left_join(knoedler_joint_owners, by = "star_record_no") %>%
    select(star_record_no, sale_transaction_id) %>%
    add_column(seller_uid = knoedler_firm_id)
}

identify_knoedler_transactions <- function(df) {
  event_ids <- df %>%
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
    mutate(
      entry_event_id = paste("k", "entry", group_indices(., stock_book_no, purch_note_working), sep = "-"),
      sale_event_id = paste("k", "sale", group_indices(., stock_book_no, price_note_working), sep = "-"),
      # No sale_event_id in the case that the record is not showing any sold
      # info, and has no transaction date info or price info.
      sale_event_id = ifelse(transaction == "Unsold" & is.na(sale_date_year) & is.na(price_amount), NA_character_, sale_event_id)) %>%
    # Calculate the order of events
    order_knoedler_object_events() %>%
    group_by(object_id) %>%
    mutate(
      purchase_event_id = if_else(event_order == 1, entry_event_id, NA_character_),
      inventory_event_id = if_else(is.na(purchase_event_id), entry_event_id, NA_character_)) %>%
    select(-purch_note_working, -knoedpurch_note_working, -price_note_working, -entry_event_id)
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
    mutate(object_id = paste("k", "object", group_indices(., prepped_sn), sep = "-")) %>%
    select(-prime_stock_number, -prepped_sn)
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
    ungroup() %>%
    # Remove intermediate columns
    select(-event_year, -event_month, -event_day)
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

produce_knoedler_materials_aat <- function(source_dir, target_dir, kdf) {
  raw_knoedler_materials_aat <- get_data(source_dir, "raw_knoedler_materials_aat")

  message("- Concordance for object materials")
  knoedler_materials_object_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_materials = made_of_materials) %>%
    single_separate(source_col = "aat_materials") %>%
    gather(gcol, aat_materials, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_materials), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_materials_object_aat)

  message("- Concordance for support materials")
  knoedler_materials_support_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_support = made_of_support) %>%
    single_separate(source_col = "aat_support") %>%
    gather(gcol, aat_support, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_support), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_materials_support_aat)

  message("- Concordance for classified_as tags")
  knoedler_materials_classified_as_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, classified_as_1, classified_as_2) %>%
    single_separate(source_col = "classified_as_1") %>%
    single_separate(source_col = "classified_as_2") %>%
    gather(gcol, aat_classified_as, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_classified_as), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_materials_classified_as_aat)

  message("- Concordance for techniques")
  knoedler_materials_technique_as_aat <- raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_technique = technique) %>%
    single_separate(source_col = "aat_technique") %>%
    gather(gcol, aat_technique, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_technique), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_materials_technique_as_aat)
}

produce_knoedler_subject_aat <- function(source_dir, target_dir, kdf) {
  raw_knoedler_subjects_aat <- get_data(source_dir, "raw_knoedler_subject_aat")

  message("- Concordance for subject")
  knoedler_subject_aat <- raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, aat_subject = subject) %>%
    single_separate(source_col = "aat_subject") %>%
    gather(gcol, aat_subject, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_subject), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_subject_aat)

  message("- Concordance for sytle")
  knoedler_style_aat <- raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, aat_style = style) %>%
    single_separate(source_col = "aat_style") %>%
    gather(gcol, aat_style, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_style), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_style_aat)

  message("- Concordance for subject classified_as")
  knoedler_subject_classified_as_aat <- raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, subject_classified_as = classified_as) %>%
    single_separate(source_col = "subject_classified_as") %>%
    gather(gcol, subject_classified_as, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(subject_classified_as), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_subject_classified_as_aat)

  message("- Concordance for depicts")
  knoedler_depicts_aat <- raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, depicts_aat = depicts) %>%
    single_separate(source_col = "depicts_aat") %>%
    gather(gcol, depicts_aat, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(depicts_aat), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
  save_data(target_dir, knoedler_depicts_aat)
}

#' @importFrom stringr str_detect
#' @importFrom rematch2 bind_re_match
identify_knoedler_anonymous_artists <- function(df) {
  df %>%
    mutate(
      is_anon = str_detect(artist_authority, "^\\["),
      person_uid = case_when(
        !is.na(artist_ulan_id) & !is_anon ~ paste0("ulan-artist-", group_indices(., artist_ulan_id)),
        is.na(artist_authority) & (is.na(is_anon) | !is_anon) ~ paste0("blank-artist-", seq_along(artist_authority)),
        is_anon ~ paste0("anon-artist-", seq_along(artist_authority)),
        !is.na(artist_authority) & !is_anon ~ paste0("known-artist-", group_indices(., artist_authority))
      )
    ) %>%
    select(-is_anon) %>%
    # Every artist MUST have a person_uid
    assertr::assert(assertr::not_na, person_uid)
}

identify_knoedler_anonymous_owners <- function(buyers_df, sellers_df) {

  # Combine both buyer and seller listings order to produce anonymous ids for everyone
  all_owners <- bind_rows(
    buyers = select(buyers_df,star_record_no, owner_name = buyer_name, owner_auth = buy_auth_name, owner_ulan_id = buyer_ulan_id),
    sellers = select(sellers_df, star_record_no, owner_name = seller_name, owner_auth = sell_auth_name, owner_ulan_id = seller_ulan_id),
    .id = "owner_type") %>%
    mutate(
      is_anon = owner_auth %in% c("Anonymous Collection"),
      is_new = owner_auth == "NEW" | is.na(owner_auth),
      person_uid = case_when(
        !is.na(owner_ulan_id) & !is_anon ~ paste0("ulan-owner-", group_indices(., owner_ulan_id)),
        # In the case that an owner has the name "NEW", this means that authority work hasn't been done yet. This will instead group and assign IDs based on the owner name
        is_new ~ paste0("unauthorized-owner-", group_indices(., owner_name)),
        is_anon ~ paste0("anon-owner-", seq_along(owner_auth)),
        !is.na(owner_auth) & !is_anon & !is_new ~ paste0("known-owner-", group_indices(., owner_auth))
      )
    ) %>%
    select(-is_anon, -is_new) %>%
    # Every artist MUST have a person_uid
    assertr::assert(assertr::not_na, person_uid)
}

#' Produce a joined Knoedler table
#'
#' @param source_dir Where to load preprocessed Knoedler files.
#' @param target_dir Where to save the fully joined Knoedler table.
#'
#' @return A data frame.
#'
#' @export
produce_joined_knoedler <- function(source_dir, target_dir) {
  knoedler <- get_data(source_dir, "knoedler")
  knoedler_artists <- get_data(source_dir, "knoedler_artists") %>%
    # The per-artist star_record_no's in Knoedler are out-of-date and
    # misaligned; matching is done via the authority name, anyway. This can be
    # discarded
    select(-artist_star_record_no)
  knoedler_buyers <- get_data(source_dir, "knoedler_buyers")
  knoedler_sellers <- get_data(source_dir, "knoedler_sellers")
  knoedler_joint_owners <- get_data(source_dir, "knoedler_joint_owners")
  knoedler_materials_classified_as_aat <- get_data(source_dir, "knoedler_materials_classified_as_aat")
  knoedler_materials_object_aat <- get_data(source_dir, "knoedler_materials_object_aat")
  knoedler_materials_support_aat <- get_data(source_dir, "knoedler_materials_support_aat")
  knoedler_materials_technique_as_aat <- get_data(source_dir, "knoedler_materials_technique_as_aat")
  knoedler_subject_aat <- get_data(source_dir, "knoedler_subject_aat")
  knoedler_style_aat <- get_data(source_dir, "knoedler_style_aat")
  knoedler_subject_classified_as_aat <- get_data(source_dir, "knoedler_subject_classified_as_aat")
  knoedler_depicts_aat <- get_data(source_dir, "knoedler_depicts_aat")
  currency_aat <- get_data(source_dir, "currency_aat")
  knoedler_dimensions <- get_data(source_dir, "knoedler_dimensions")

  knoedler_name_order <- c(
    "star_record_no",
    "pi_record_no",
    "stock_book_no",
    "knoedler_number",
    "page_number",
    "row_number",
    "consign_no",
    "consign_name",
    "consign_loc",
    "title",
    "description",
    "artist_name_1",
    "artist_authority_1",
    "artist_nationality_1",
    "artist_attribution_mod_1",
    "artist_ulan_id_1",
    "artist_name_2",
    "artist_authority_2",
    "artist_nationality_2",
    "artist_attribution_mod_2",
    "artist_ulan_id_2",
    "subject",
    "genre",
    "depicts_aat_1",
    "subject_classified_as_1",
    "subject_classified_as_2",
    "subject_classified_as_3",
    "aat_style_1",
    "aat_subject",
    "object_type",
    "materials",
    "aat_classified_as_1",
    "aat_classified_as_2",
    "aat_classified_as_3",
    "aat_support_1",
    "aat_support_2",
    "aat_materials_1",
    "aat_materials_2",
    "aat_materials_3",
    "aat_technique_1",
    "dimensions",
    "entry_date_year",
    "entry_date_month",
    "entry_date_day",
    "sale_date_year",
    "sale_date_month",
    "sale_date_day",
    "purch_amount",
    "purch_currency",
    "purch_currency_aat",
    "purch_note",
    "knoedpurch_amt",
    "knoedpurch_curr",
    "knoedpurch_curr_aat",
    "knoedpurch_note",
    "price_amount",
    "price_currency",
    "price_currency_aat",
    "price_note",
    "knoedshare_amt",
    "knoedshare_curr",
    "knoedpurch_curr_aat",
    "knoedshare_note",
    "transaction",
    "folio",
    "verbatim_notes",
    "main_heading",
    "subheading",
    "object_id",
    "inventory_or_purchase_id",
    "sale_transaction_id",
    "event_order",
    "joint_own_1",
    "joint_own_sh_1",
    "joint_own_2",
    "joint_own_sh_2",
    "joint_own_3",
    "joint_own_sh_3",
    "joint_own_4",
    "joint_own_sh_4",
    "buyer_name_1",
    "buyer_loc_1",
    "buy_auth_name_1",
    "buy_auth_addr_1",
    "buyer_ulan_id_1",
    "buyer_name_2",
    "buyer_loc_2",
    "buy_auth_name_2",
    "buy_auth_addr_2",
    "buyer_ulan_id_2",
    "seller_name_1",
    "seller_loc_1",
    "sell_auth_name_1",
    "sell_auth_loc_1",
    "seller_ulan_id_1",
    "seller_name_2",
    "seller_loc_2",
    "sell_auth_name_2",
    "sell_auth_loc_2",
    "seller_ulan_id_2"
  )

  joined_knoedler <- knoedler %>%
    pipe_message("- Join spread knoedler_artists to knoedler") %>%
    left_join(spread_out(knoedler_artists, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_buyers to knoedler") %>%
    left_join(spread_out(knoedler_buyers, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_sellers to knoedler") %>%
    left_join(spread_out(knoedler_sellers, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_joint_owners to knoedler") %>%
    left_join(spread_out(knoedler_joint_owners, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join knoedler_dimensions to knoedler") %>%
    left_join(spread_out(knoedler_dimensions, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_classified_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_classified_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_support_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_support_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_object_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_object_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_technique_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_technique_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_depicts_aat to knoedler") %>%
    left_join(spread_out(knoedler_depicts_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_subject_classified_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_subject_classified_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_style_aat to knoedler") %>%
    left_join(spread_out(knoedler_style_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_subject_aat to knoedler") %>%
    left_join(spread_out(knoedler_subject_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining currency IDs to knoedler") %>%
    left_join(rename(currency_aat, purch_currency = price_currency, purch_currency_aat = currency_aat), by = "purch_currency") %>%
    left_join(rename(currency_aat, knoedpurch_curr = price_currency, knoedpurch_curr_aat = currency_aat), by = "knoedpurch_curr") %>%
    left_join(rename(currency_aat, price_currency = price_currency, price_currency_aat = currency_aat), by = "price_currency") %>%
    left_join(rename(currency_aat, knoedshare_curr = price_currency, knoedshare_curr_aat = currency_aat), by = "knoedshare_curr")

  save_data(target_dir, joined_knoedler)
  invisible(joined_knoedler)
}
