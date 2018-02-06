# Sales Contents Normalization ----

produce_sales_contents_ids <- function(raw_sales_contents) {
  raw_sales_contents %>%
    mutate_at(vars(lot_sale_year, lot_sale_month, lot_sale_day), funs(as.integer)) %>%
    mutate(project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    # Lowercase all text fields that need to be used as joining keys
    mutate_at(vars(subject, genre, object_type, materials), funs(tolower)) %>%
    rename(puri = persistent_puid) %>%
    select(-star_record_no) %>%
    assert(not_na, puri, catalog_number) %>%
    assert(is_uniq, puri)
}

produce_sales_contents <- function(sales_contents, sales_contents_prev_sales, sales_contents_post_sales, sales_contents_prices) {
  sales_contents %>%
    select(-(expert_auth_1:expert_ulan_4)) %>%
    select(-(commissaire_pr_1:comm_ulan_4)) %>%
    select(-(auction_house_1:house_ulan_4)) %>%
    select(-(artist_name_1:artist_ulan_5)) %>%
    select(-(hand_note_1:hand_note_so_7)) %>%
    select(-(sell_name_1:sell_ulan_5)) %>%
    select(-(buy_name_1:buy_ulan_4)) %>%
    select(-(price_amount_1:price_citation_3)) %>%
    select(-(prev_owner_1:prev_own_ulan_9)) %>%
    select(-(prev_sale_year_1:prev_sale_coll_7)) %>%
    select(-(post_sale_yr_1:post_sale_col_13)) %>%
    select(-(post_own_1:post_own_ulan_6)) %>%
    identify_sales_unique_objects(sales_contents_prev_sales, sales_contents_post_sales) %>%
    identify_sales_transactions(sales_contents_prices) %>%
    assert(not_na, puri, catalog_number) %>%
    assert(is_uniq, puri) %>%
    no_dots()
}

# Sales Contents - People ----

identify_sales_contents_id_process <- function(person_df, combined_authority) {

  # Identify those "authority names" that are not actually present in the
  # artists authority, and so should not be linked to it
  missing_auth_names <- person_df %>%
    filter(!is.na(person_auth) & !(person_auth %in% combined_authority$authority_name)) %>%
    pull(person_auth)

  mutate(person_df,
         is_bracketed = !is.na(person_auth) & str_detect(person_auth, "^\\["),
         is_anon_collex = !is.na(person_auth) & person_auth %in% c("Anonymous Collection"),
         is_new = is.na(person_auth) | person_auth == "NEW",
         is_anon =  is_anon_collex | is_bracketed,
         is_ulan = !is.na(person_ulan),
         is_known = !is.na(person_auth) & !is_anon & !is_new,
         is_bad_authority = person_auth %in% missing_auth_names,
         id_process = case_when(
           !is_new & !is_anon & !is_ulan & is_known & !is_bad_authority ~ "from_auth",
           !is_new & !is_anon & !is_ulan & is_known & is_bad_authority ~ "from_auth_grouped",
           !is_new & !is_anon & is_ulan & is_known & !is_bad_authority ~ "from_ulan",
           !is_new & is_anon & is_ulan & is_known ~ "from_nothing",
           is_new & !is_anon & !is_ulan & !is_known & !is_bad_authority ~ "from_nothing",
           is_new & !is_anon & is_ulan & !is_known & !is_bad_authority ~ "from_ulan",
           is_ulan ~ "from_ulan",
           is_new ~ "from_name",
           is_anon ~ "from_nothing",
           is_known ~ "from_auth",
           is_bad_authority ~ "from_auth_grouped")) %>%
    select(source_record_id, source_document_id, person_name, person_auth, person_ulan, id_process) %>%
    assertr::assert(assertr::not_na, id_process)
}

produce_sales_contents_experts_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("expert_auth", "expert_ulan"), n_reps = 4, idcols = "puri")
}

produce_sales_contents_experts_lookup <- function(sales_contents_experts_tmp, sales_contents_ids, combined_authority) {
  sales_contents_experts_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_auth = expert_auth,
      person_ulan = expert_ulan
    ) %>%
    mutate(person_name = NA_character_) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_experts <- function(sales_contents_experts_tmp, union_person_ids) {
  experts_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_experts") %>%
    select(-source_db, -source_document_id) %>%
    rename(expert_uid = person_uid)

  left_join(sales_contents_experts_tmp,
            experts_ids,
            by = c(
              "puri" = "source_record_id",
              "expert_auth" = "person_auth",
              "expert_ulan" = "person_ulan")) %>%
    select(puri, contains("expert")) %>%
    distinct()
}

produce_sales_contents_commissaire_pr_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("commissaire_pr", "comm_ulan"), n_reps = 4, idcols = "puri")
}

produce_sales_contents_commissaire_pr_lookup <- function(sales_contents_commissaire_pr_tmp, sales_contents_ids, combined_authority) {
  sales_contents_commissaire_pr_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_name = commissaire_pr,
      person_ulan = comm_ulan
    ) %>%
    mutate(person_auth = NA_character_) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_commissaire_pr <- function(sales_contents_commissaire_pr_tmp, union_person_ids) {
  commissaire_pr_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_commissaire_pr") %>%
    select(-source_db, -source_document_id) %>%
    rename(comm_uid = person_uid)

  left_join(sales_contents_commissaire_pr_tmp,
            commissaire_pr_ids,
            by = c(
              "puri" = "source_record_id",
              "commissaire_pr" = "person_name",
              "comm_ulan" = "person_ulan")) %>%
    select(puri, contains("comm")) %>%
    distinct()
}

produce_sales_contents_auction_houses_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("auction_house", "house_ulan"), n_reps = 4, idcols = "puri")
}

produce_sales_contents_auction_houses_lookup <- function(sales_contents_auction_houses_tmp, sales_contents_ids, combined_authority) {
  sales_contents_auction_houses_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_name = auction_house,
      person_ulan = house_ulan
    ) %>%
    mutate(person_auth = NA_character_) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_auction_houses <- function(sales_contents_auction_houses_tmp, union_person_ids) {
  auction_house_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_auction_houses") %>%
    select(-source_db, -source_document_id) %>%
    rename(auction_house_uid = person_uid)

  left_join(sales_contents_auction_houses_tmp,
            auction_house_ids,
            by = c(
              "puri" = "source_record_id",
              "auction_house" = "person_name",
              "house_ulan" = "person_ulan")) %>%
    select(puri, contains("house")) %>%
    distinct()
}

produce_sales_contents_artists_tmp <- function(sales_contents, generic_artists) {
  provisional_sales_contents_artists <- norm_vars(sales_contents, base_names = c("artist_name", "artist_info", "art_authority", "nationality", "attrib_mod", "attrib_mod_auth", "star_rec_no", "artist_ulan"), n_reps = 5, idcols = "puri") %>%
    select(-star_rec_no)

  # Expand "generic" artists into multiple "possibly by" relationships
  provisional_sales_contents_artists %>%
    left_join(select(generic_artists, -generic_artist_star_record_no, generic_artist_ulan_id), by = c("art_authority" = "generic_artist_authority")) %>%
    mutate(
      is_generic = !is.na(generic_artist_ulan_id),
      art_authority = if_else(is_generic, NA_character_, art_authority),
      artist_ulan = if_else(is_generic, generic_artist_ulan_id, artist_ulan),
      attrib_mod_auth = if_else(is_generic, "possibly by", attrib_mod_auth)
    ) %>%
    select(-generic_artist_ulan_id, -is_generic)
}

produce_sales_contents_artists_lookup <- function(sales_contents_artists_tmp, sales_contents_ids, combined_authority) {
  sales_contents_artists_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_name = artist_name,
      person_auth = art_authority,
      person_ulan = artist_ulan
    ) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_artists <- function(sales_contents_artists_tmp, union_person_ids) {
  artists_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_artists") %>%
    select(-source_db, -source_document_id) %>%
    rename(artist_uid = person_uid)

  left_join(sales_contents_artists_tmp,
            artists_ids,
            by = c(
              "puri" = "source_record_id",
              "artist_name" = "person_name",
              "art_authority" = "person_auth",
              "artist_ulan" = "person_ulan")) %>%
    distinct()
}

produce_sales_contents_hand_notes <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("hand_note", "hand_note_so"), n_reps = 7, idcols = "puri")
}

produce_sales_contents_sellers_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("sell_name", "sell_name_so", "sell_name_ques", "sell_mod", "sell_auth_name", "sell_auth_nameq", "sell_auth_mod", "sell_auth_mod_a", "sell_ulan"), n_reps = 5, idcols = "puri")
}

produce_sales_contents_sellers_lookup <- function(sales_contents_sellers_tmp, sales_contents_ids, combined_authority) {
  sales_contents_sellers_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_name = sell_name,
      person_auth = sell_auth_name,
      person_ulan = sell_ulan
    ) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_sellers <- function(sales_contents_sellers_tmp, union_person_ids) {
  seller_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_sellers") %>%
    select(-source_db, -source_document_id) %>%
    rename(seller_uid = person_uid)

  left_join(sales_contents_sellers_tmp,
            seller_ids,
            by = c(
              "puri" = "source_record_id",
              "sell_name" = "person_name",
              "sell_auth_name" = "person_auth",
              "sell_ulan" = "person_ulan")) %>%
    distinct()
}

produce_sales_contents_buyers_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("buy_name", "buy_name_so", "buy_name_ques", "buy_name_cite", "buy_mod", "buy_auth_name", "buy_auth_nameq", "buy_auth_mod", "buy_auth_mod_a", "buy_ulan"), n_reps = 4, idcols = "puri")
}

produce_sales_contents_buyers_lookup <- function(sales_contents_buyers_tmp, sales_contents_ids, combined_authority) {
  sales_contents_buyers_tmp %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    select(
      source_record_id = puri,
      source_document_id = catalog_number,
      person_name = buy_name,
      person_auth = buy_auth_name,
      person_ulan = buy_ulan
    ) %>%
    identify_sales_contents_id_process(combined_authority)
}

produce_sales_contents_buyers <- function(sales_contents_buyers_tmp, union_person_ids) {
  buyers_ids <- union_person_ids %>%
    filter(source_db == "sales_contents_buyers") %>%
    select(-source_db, -source_document_id) %>%
    rename(buyer_uid = person_uid)

  left_join(sales_contents_buyers_tmp,
            buyers_ids,
            by = c(
              "puri" = "source_record_id",
              "buy_name" = "person_name",
              "buy_auth_name" = "person_auth",
              "buy_ulan" = "person_ulan")) %>%
    distinct()
}

produce_sales_contents_prices_tmp <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("price_amount", "price_currency", "price_note", "price_source", "price_citation"), n_reps = 3, idcols = "puri") %>%
    add_column(price_id = seq_len(nrow(.)))
}

produce_sales_contents_parsed_prices <- function(sales_contents_prices_tmp, currency_aat) {
  parse_prices(currency_aat, sales_contents_prices_tmp, amount_col_name = "price_amount", currency_col_name = "price_currency", id_col_name = "price_id", decimalized_col_name = "decimalized_price_currency", aat_col_name = "price_currency_aat", amonsieurx = FALSE, replace_slashes = FALSE)
}

produce_sales_contents_prices <- function(sales_contents_prices_tmp, sales_contents_parsed_prices) {
  sales_contents_prices_tmp %>%
    left_join(sales_contents_parsed_prices, by = "price_id")
}

produce_sales_contents_prev_owners <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("prev_owner", "prev_own_ques", "prev_own_so", "prev_own_auth", "prev_own_auth_d", "prev_own_auth_l", "prev_own_auth_q", "prev_own_ulan"), n_reps = 9, idcols = "puri")
}

produce_sales_contents_prev_sales <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("prev_sale_year", "prev_sale_mo", "prev_sale_day", "prev_sale_loc", "prev_sale_lot", "prev_sale_ques", "prev_sale_artx", "prev_sale_ttlx", "prev_sale_note", "prev_sale_coll"), n_reps = 6, idcols = "puri") %>%
    mutate_at(vars(prev_sale_year, prev_sale_mo, prev_sale_day), funs(as.integer))
}

proudce_sales_contents_post_sales <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc", "post_sale_lot", "post_sale_q", "post_sale_art", "post_sale_ttl", "post_sale_nte", "post_sale_col"), n_reps = 13, idcols = "puri") %>%
    mutate_at(vars(post_sale_yr, post_sale_mo, post_sale_day), funs(as.integer))
}

proudce_sales_contents_post_owners <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("post_own", "post_own_q", "post_own_so", "post_own_auth", "post_own_auth_d", "post_own_auth_l", "post_own_auth_q", "post_own_ulan"), n_reps = 6, idcols = "puri")
}

produce_sales_contents_materials_classified_as_aat <- function(raw_sales_contents_materials_aat, sales_contents_ids) {
  raw_sales_contents_materials_aat %>%
    select(sales_contents_object_type, sales_contents_materials, contains("classified_as")) %>%
    single_separate("classified_as_2") %>%
    mutate_at(vars(contains("classified_as")), as.integer) %>%
    gather(ca_index, classified_as, contains("classified_as")) %>%
    left_join(select(sales_contents_ids, puri, object_type, materials), by = c("sales_contents_object_type" = "object_type", "sales_contents_materials" = "materials")) %>%
    select(puri, object_type_classified_as_aat = classified_as) %>%
    na.omit()
}

produce_sales_contents_made_of_materials_aat <- function(raw_sales_contents_materials_aat, sales_contents_ids) {
  raw_sales_contents_materials_aat %>%
    select(sales_contents_object_type, sales_contents_materials, made_of_materials) %>%
    single_separate("made_of_materials") %>%
    mutate_at(vars(contains("made_of_materials")), as.integer) %>%
    gather(mo_index, made_of_aat, contains("made_of_materials")) %>%
    left_join(select(sales_contents_ids, puri, object_type, materials), by = c("sales_contents_object_type" = "object_type", "sales_contents_materials" = "materials")) %>%
    select(puri, made_of_aat) %>%
    na.omit()
}

produce_sales_contents_support_materials_aat <- function(raw_sales_contents_materials_aat, sales_contents_ids) {
  raw_sales_contents_materials_aat %>%
    select(sales_contents_object_type, sales_contents_materials, made_of_support) %>%
    single_separate("made_of_support") %>%
    mutate_at(vars(contains("made_of_support")), as.integer) %>%
    gather(mo_index, support_aat, contains("made_of_support")) %>%
    left_join(select(sales_contents_ids, puri, object_type, materials), by = c("sales_contents_object_type" = "object_type", "sales_contents_materials" = "materials")) %>%
    select(puri, support_aat) %>%
    na.omit()
}

produce_sales_contents_technique_aat <- function(raw_sales_contents_materials_aat, sales_contents_ids) {
  raw_sales_contents_materials_aat %>%
    select(sales_contents_object_type, sales_contents_materials, technique) %>%
    single_separate("technique") %>%
    mutate_at(vars(contains("technique")), as.integer) %>%
    gather(mo_index, technique, contains("technique")) %>%
    left_join(select(sales_contents_ids, puri, object_type, materials), by = c("sales_contents_object_type" = "object_type", "sales_contents_materials" = "materials")) %>%
    select(puri, technique) %>%
    na.omit()
}

# Subject-related AAT terms

produce_sales_contents_subject_aat <- function(raw_sales_contents_subject_aat, sales_contents_ids) {
  raw_sales_contents_subject_aat %>%
    select(sales_contents_subject, sales_contents_genre, subject_aat = subject) %>%
    single_separate("subject_aat") %>%
    mutate_at(vars(contains("subject_aat")), as.integer) %>%
    gather(mo_index, subject_aat, contains("subject_aat")) %>%
    left_join(select(sales_contents_ids, puri, subject, genre), by = c("sales_contents_subject" = "subject", "sales_contents_genre" = "genre")) %>%
    select(puri, subject_aat) %>%
    na.omit()
}

produce_sales_contents_style_aat <- function(raw_sales_contents_subject_aat, sales_contents_ids) {
  raw_sales_contents_subject_aat %>%
    select(sales_contents_subject, sales_contents_genre, style_aat = style) %>%
    single_separate("style_aat") %>%
    mutate_at(vars(contains("style_aat")), as.integer) %>%
    gather(mo_index, style_aat, contains("style_aat")) %>%
    left_join(select(sales_contents_ids, puri, subject, genre), by = c("sales_contents_subject" = "subject", "sales_contents_genre" = "genre")) %>%
    select(puri, style_aat) %>%
    na.omit()
}

produce_sales_contents_subject_classified_as_aat <- function(raw_sales_contents_subject_aat, sales_contents_ids) {
  raw_sales_contents_subject_aat %>%
    select(sales_contents_subject, sales_contents_genre, subject_classified_as_aat = classified_as) %>%
    single_separate("subject_classified_as_aat") %>%
    mutate_at(vars(contains("subject_classified_as_aat")), as.integer) %>%
    gather(mo_index, subject_classified_as_aat, contains("subject_classified_as_aat")) %>%
    full_join(select(sales_contents_ids, puri, subject, genre), by = c("sales_contents_subject" = "subject", "sales_contents_genre" = "genre")) %>%
    select(puri, subject_classified_as_aat) %>%
    na.omit()
}

produce_sales_contents_depicts_aat <- function(raw_sales_contents_subject_aat, sales_contents_ids) {
  raw_sales_contents_subject_aat %>%
    select(sales_contents_subject, sales_contents_genre, depicts_aat = depicts) %>%
    single_separate("depicts_aat") %>%
    mutate_at(vars(contains("depicts_aat")), as.integer) %>%
    gather(mo_index, depicts_aat, contains("depicts_aat")) %>%
    full_join(select(sales_contents_ids, puri, subject, genre), by = c("sales_contents_subject" = "subject", "sales_contents_genre" = "genre")) %>%
    select(puri, depicts_aat) %>%
    na.omit()
}

produce_sales_contents_dimensions <- function(sales_contents_ids) {
  sales_contents_ids %>%
    mutate(exclude_dimension = case_when(
      project == "Br" & str_detect(dimensions, "bracci[oa]") ~ TRUE,
      TRUE ~ FALSE
    )) %>%
    general_dimension_extraction(dimcol = "dimensions", idcol = "puri", exclusion_col = "exclude_dimension")
}

# Sales Descriptions Normalization ----

produce_sales_descriptions_ids <- function(raw_sales_descriptions) {
  raw_sales_descriptions %>%
    mutate_at(vars(sale_begin_year, sale_begin_month, sale_begin_day, sale_end_year, sale_end_month, sale_end_day, no_of_ptgs_lots), funs(as.integer)) %>%
    mutate(description_project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    rename(description_puri = persistent_puid) %>%
    select(-star_record_no) %>%
    assert(not_na, description_puri) %>%
    assert(is_uniq, description_puri)
}

produce_sales_descriptions <- function(sales_descriptions) {
  sales_descriptions %>% select(-(lugt_number_1:lugt_number_3)) %>%
    select(-(title_pg_sell_1:title_pg_sell_2)) %>%
    select(-(auc_copy_seller_1:auc_copy_seller_4)) %>%
    select(-(other_seller_1:other_seller_3)) %>%
    select(-(sell_auth_name_1:sell_auth_q_5)) %>%
    select(-(expert_1:expert_auth_4)) %>%
    select(-(comm_pr_1:comm_pr_auth_4)) %>%
    select(-(auc_house_name_1:auc_house_auth_4)) %>%
    select(-(country_auth_1:country_auth_2))
}

produce_sales_descriptions_lugt_numbers <- function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = "lugt_number", n_reps = 3, idcols = "description_puri")
}

produce_sales_descriptions_title_seller <- function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = "title_pg_sell", n_reps = 2, idcols = "description_puri")
}

produce_sales_descriptions_auc_copy_seller <- function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = "auc_copy_seller", n_reps = 4, idcols = "description_puri")
}

produce_sales_descriptions_other_seller <-  function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("other_seller"), n_reps = 3, idcols = "description_puri")
}

produce_sales_descriptions_auth_seller <-  function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("sell_auth_name", "sell_auth_q"), n_reps = 5, idcols = "description_puri")
}

produce_sales_descriptions_expert_auth <- function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("expert", "expert_auth"), n_reps = 4, idcols = "description_puri")
}

produce_sales_descriptions_commissaire_pr <-  function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("comm_pr", "comm_pr_auth"), n_reps = 4, idcols = "description_puri")
}

produce_sales_descriptions_auction_house <- function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("auc_house_name", "auc_house_auth"), n_reps = 4, idcols = "description_puri")
}

produce_sales_descriptions_country <-  function(sales_descriptions) {
  sales_descriptions %>%
    # small naming variation needs to be fixed
    norm_vars(base_names = c("country_auth"), n_reps = 2, idcols = "description_puri")
}

produce_sales_catalogs_info <- function(raw_sales_catalogs_info, raw_sales_catalogs_loccodes) {
  message("Reading raw_sales_catalogs_info")
  sales_catalogs_info <- raw_sales_catalogs_info %>%
    left_join(select(raw_sales_catalogs_loccodes, -star_record_no), by = "owner_code")
}

# Sales Contents Computations ----

identify_sales_unique_objects <- function(scdf, prev_sales, post_sales) {
  # Produce an easily-inspected set of sales_contents that has a sale_loc
  # variable: the alphabetic part of each sale code indiciating the location.
  # This, along with lot numbers and date components, are the only shared
  # information with the
  target_sales_contents <- scdf %>%
    select(
      target_puri = puri,
      sale_code,
      catalog_number,
      lot_number,
      lot_sale_year,
      lot_sale_month,
      lot_sale_day) %>%
    mutate(sale_loc = str_extract(sale_code, "[A-Z]+$"))

  # Attempt to join these candidate sales contents records to the records
  # indicated in sales_contents_prev_sales
  message("- Attempting to match previous sales")
  sales_prev_join <- prev_sales %>%
    rename(prev_puri = puri) %>%
    inner_join(target_sales_contents, by = c(
      "prev_sale_loc" = "sale_loc",
      "prev_sale_lot" = "lot_number",
      "prev_sale_year" = "lot_sale_year",
      "prev_sale_mo" = "lot_sale_month",
      "prev_sale_day" = "lot_sale_day"
    ))

  # Records that claim to have a match, but we can't find it
  failed_prev_match <- prev_sales %>%
    # Only keep those that could not be successfully matched
    anti_join(sales_prev_join, by = c("puri" = "prev_puri")) %>%
    # Add additional identifying information to those records for editors to see
    inner_join(select(scdf, puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = "puri")

  # Records that claim to have a match, and we get MULTIPLE matches back
  multiple_prev_match <- sales_prev_join %>%
    # Only keep those joins that succeeded
    filter(!is.na(target_puri)) %>%
    # To find duplicated rows, find those that have been duplicated based on the
    # original join query
    add_count(prev_puri, prev_sale_lot, prev_sale_loc, prev_sale_day, prev_sale_mo, prev_sale_year) %>%
    filter(n > 1) %>%
    # Join the records for the _target_ sales so that editors can find them
    inner_join(select(scdf, puri, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = c("target_puri" = "puri"))

  # Only those records with exactly one match
  exact_prev_match <- sales_prev_join %>%
    anti_join(multiple_prev_match, by = "prev_puri")

  # Attempt to join these candidate sales contents records to the records
  # indicated in sales_contents_post_sales
  message("- Attempt to match post sales")
  sales_post_join <- post_sales %>%
    rename(post_puri = puri) %>%
    inner_join(target_sales_contents, by = c(
      "post_sale_loc" = "sale_loc",
      "post_sale_lot" = "lot_number",
      "post_sale_yr" = "lot_sale_year",
      "post_sale_mo" = "lot_sale_month",
      "post_sale_day" = "lot_sale_day"
    ))

  # Records that claim to have a match, but we can't find it
  failed_post_match <- post_sales %>%
    # Only keep those that could not be successfully matched
    anti_join(sales_post_join, by = c("puri" = "post_puri")) %>%
    # Add additional identifying information to those records for editors to see
    inner_join(select(scdf, puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = "puri")

  multiple_post_match <- sales_post_join %>%
    filter(!is.na(target_puri)) %>%
    # Must group by the entire unqiue row from the original
    # sales_contents_post_sales
    add_count(post_puri, post_sale_lot, post_sale_loc, post_sale_day, post_sale_mo, post_sale_yr) %>%
    filter(n > 1) %>%
    inner_join(select(scdf, puri, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = c("target_puri" = "puri")) %>%
    arrange(post_puri)

  # Keep only those with exactly one match
  exact_post_match <- sales_post_join %>%
    anti_join(multiple_post_match, by = "post_puri")

  # Create transaction graph
  # For any given record, it is not guaranteed that it has prev/post references
  # to every single sale that may have concerned the same object. It is
  # necessary to find all records that share a link with each other across any
  # number of siblings. In other words, we want to identify all the connected
  # components of the graph representing the prev/post connections between each
  # sales contents records. This is a simple network analysis task.

  # Merge each of the exact match tables into one large adjacency list
  message("- Identify connected components in graph of related sales")
  transaction_edgelist <- bind_rows(
    select(exact_post_match, source = post_puri, target = target_puri),
    select(exact_prev_match, source = prev_puri, target = target_puri))

  transaction_nodes <- scdf %>%
    filter(puri %in% transaction_edgelist$source | puri %in% transaction_edgelist$target) %>%
    select(name = puri, title, lot_sale_year, lot_sale_month, lot_sale_day, catalog_number, lot_number) %>%
    mutate(
      lot_sale_day = if_else(lot_sale_day == 0, 1L, lot_sale_day),
      lot_sale_month = if_else(lot_sale_month == 0, 1L, lot_sale_month),
      lot_sale_date = lubridate::ymd(paste(lot_sale_year, lot_sale_month, lot_sale_day, sep = "-")))

  transaction_graph <- graph_from_data_frame(transaction_edgelist, directed = FALSE, vertices = transaction_nodes)

  # Simplify graph and check component size
  flat_trans_graph <- simplify(transaction_graph)

  # Add human readable labels for graph inspections
  V(transaction_graph)$label <- str_wrap(paste0(V(transaction_graph)$catalog_number, ": ", V(transaction_graph)$lot_number, " - ", str_trunc(V(transaction_graph)$title, width = 20), "(", V(transaction_graph)$lot_sale_year, "/", V(transaction_graph)$lot_sale_month, "/", V(transaction_graph)$lot_sale_day, ")"), width = 30)

  # Calculate the components
  transaction_components <- components(flat_trans_graph)
  transaction_betweenness <- centr_betw(transaction_graph)

  V(transaction_graph)$component <- transaction_components$membership
  V(transaction_graph)$bc <- transaction_betweenness$res

  transaction_membership_list <- data_frame(
    puri = names(transaction_components$membership),
    object_uid = paste("multiple", "object", transaction_components$membership, sep = "-"))

  # Join new object uids on to sales contents records, and produce singleton IDs
  # for those records with no prev/post sale information
  scdf %>%
    left_join(transaction_membership_list, by = "puri") %>%
    mutate(object_uid = if_else(is.na(object_uid), paste("single", "object", seq_along(puri), sep = "-"), object_uid))
}

identify_sales_transactions <- function(sales_contents_ids, sales_contents_prices) {
  transaction_ids <- sales_contents_prices %>%
    # When there are multiple price entries, just pick the first note in each one
    group_by(puri) %>%
    summarize(price_note = pick(price_note)) %>%
    left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
    mutate(joining_note = if_else(str_detect(price_note, regex("(f[üu]r|pour|for|avec)", ignore_case = TRUE)), price_note, NA_character_)) %>%
    mutate(transaction_id = if_else(is.na(joining_note), paste0("transaction-", seq_along(puri)), paste0("group-transaction-", group_indices(., catalog_number, joining_note)))) %>%
    select(-catalog_number)

  jt <- sales_contents_ids %>%
    left_join(transaction_ids, by = "puri")
}

# Sales Contents Joined Table

produce_joined_sales_contents <- function(sales_contents,
                                          sales_contents_dimensions,
                                          sales_contents_parsed_prices) {
  sales_contents %>%
    pipe_message(" - Joining sales_contents_dimensions") %>%
    left_join(spread_out(sales_contents_dimensions, "puri")) %>%
    left_join(spread_out(sales_contents_parsed_prices, "puri"))
}

# Upstream Error Fixes

produce_sales_unparsed_dimensions <- function(sales_contents_ids, sales_contents_dimensions) {
  sales_contents_ids %>%
    select(puri, dimensions) %>%
    filter(!is.na(dimensions)) %>%
    anti_join(sales_contents_dimensions, by = "puri")
}

# GH Export ----

produce_gh_sales_contents <- function(raw_sales_contents) {
  nsc <- nrow(raw_sales_contents)
  nsplits <- ceiling(nsc / 100000)
  split_vector <- rep(1:nsplits, each = 100000)[1:nsc]

  raw_sales_contents %>%
    select(-contains("star"), -original_file_name, -sale_code, -file_segment, -(contains("ulan"))) %>%
    select(persistent_puid, everything()) %>%
    mutate_at(vars(contains("auth")), redact) %>%
    split(split_vector)
}

write_gh_sales_contents <- function(split_sc, path, index) {
  write_csv(split_sc[[index]], path = path, na = "")
}

produce_gh_sales_descriptions <- function(raw_sales_descriptions) {
  raw_sales_descriptions %>%
    select(-original_file_name, -star_record_no, -cat_input_status, -persistent_puid)
}

produce_gh_sales_catalogs_info <- function(raw_sales_catalogs_info, raw_sales_catalogs_loccodes) {
  raw_sales_catalogs_info %>%
    left_join(select(raw_sales_catalogs_loccodes, owner_code, owner_location), by = "owner_code") %>%
    select(catalog_number, copy_number, gri_has_copy, price_recorded, owner_code, owner_location)
}

