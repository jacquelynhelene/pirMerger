produce_goupil_ids <- function(raw_goupil) {
  raw_goupil %>%
    mutate_at(vars(contains("stock_book"), page_number, row_number, contains("_date_")), funs(as.integer)) %>%
    # Convert all "0" ULAN values to NA
    null_ulan() %>%
    select(-persistent_uid, -original_file_name) %>%
    rename(transaction_type = transaction) %>%
    assert(not_na, star_record_no) %>%
    assert(is_uniq, star_record_no)
}

produce_goupil <- function(goupil_with_ids) {
  goupil_with_ids %>%
    select(-(other_stock_book_no_1:other_stock_book_row_14)) %>%
    select(-(artist_name_1:artist_ulan_id_2)) %>%
    select(-(buyer_name_1:buyer_ulan_id_2)) %>%
    select(-(previous_owner_1:previous_owner_2))
}

produce_goupil_stock_book_nos <- function(goupil) {
  norm_vars(goupil, base_names = c("other_stock_book_no", "other_stock_book_goupil_no", "other_stock_book_page", "other_stock_book_row"), n_reps = 14, idcols = "star_record_no")
}

# Goupil People ----

identify_goupil_artists <- function(goupil) {
  goupil_artist_ids <- goupil %>%
    mutate(artist_id = "A")

  joined_artists <- goupil_artists %>%
    group_by(star_record_no) %>%
    summarize(artist_string = paste(artist_authority, collapse = ";"))

  goupil_ided_artists <- goupil %>%
    left_join(joined_artists, by = "star_record_no") %>%
    mutate(dummy_artist_string = if_else(is.na(artist_string) | artist_string == "[ANONYMOUS]" | artist_string == "[ANONYMOUS - 18TH C.]" | artist_string == "[ANONYMOUS - ANTIQUE]" | artist_string == "[BRITISH]" | artist_string == "[DUTCH - 17TH C.]" | artist_string == "[FLEMISH]" | artist_string == "[FRENCH - 18TH C.]" | artist_string == "[FRENCH]" | artist_string == "[GERMAN]" | artist_string == "[ITALIAN]", paste("missing", star_record_no), artist_string)) %>%
    mutate(artist_id = group_indices(., dummy_artist_string, artist_string))

  count_artist_ids <- goupil_ided_artists %>%
    count(artist_id)

  check_artist_ids <- goupil_ided_artists %>%
    filter(artist_id == "258")

  return(goupil_artist_ids)
}

produce_goupil_artists <- function(goupil) {
  norm_vars(goupil, base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "attribution_mod_auth", "star_rec_no", "artist_ulan_id"), n_reps = 2, idcols = "star_record_no") %>%
    # Join ulan ids to this list
    rename(artist_star_record_no = star_rec_no, artist_authority = art_authority, artist_nationality = nationality, artist_attribution_mod = attribution_mod)
}

produce_goupil_buyers <- function(goupil) {
  norm_vars(goupil, base_names = c("buyer_name", "buyer_loc", "buyer_mod", "buy_auth_name", "buy_auth_addr", "buy_auth_mod", "buyer_ulan_id"), n_reps = 2, idcols = "star_record_no")
}

produce_goupil_previous_owners <- function(goupil) {
  norm_vars(goupil, base_names = c("previous_owner"), n_reps = 2, idcols = "star_record_no")
}

# Goupil Transactions ----

identify_goupil_purchases <- function(goupil) {

}

identify_goupil_sales <- function(goupil) {

}

# Goupil Objects ----

identify_goupil_objects <- function(goupil) {
  goupil_objects <- goupil %>%
    mutate(object_id = "A")

  joined_artists <- goupil_artists %>%
    group_by(star_record_no) %>%
    summarize(artist_string = paste(artist_authority, collapse = ";"))

  goupil_object_ids <- goupil %>%
    left_join(joined_artists, by = "star_record_no") %>%
    mutate(dummy_goupil_number = if_else(is.na(goupil_number) | goupil_number == "[Aucun]" | goupil_number == "unknown", paste("missing", star_record_no), goupil_number)) %>%
    mutate(object_id = group_indices(., dummy_goupil_number, artist_string))

  return(goupil_objects)
}



produce_goupil_classified_as_aat <- function(raw_goupil_subject_genre_aat, goupil_with_ids) {
  tt <- raw_goupil_subject_genre_aat %>%
    select(goupil_subject, goupil_genre, classified_as_aat) %>%
    filter(!is.na(classified_as_aat)) %>%
    single_separate("classified_as_aat") %>%
    gather(classified_index, classified_as_aat, contains("classified_as_aat")) %>%
    mutate_at(vars(classified_as_aat), funs(as.integer)) %>%
    left_join(select(goupil_with_ids, star_record_no, subject, genre), by = c("goupil_subject" = "subject", "goupil_genre" = "genre")) %>%
    select(-goupil_subject, -goupil_genre, -classified_index)
}

produce_goupil_depicts_aat <- function(raw_goupil_subject_genre_aat, goupil_with_ids) {
  tt <- raw_goupil_subject_genre_aat %>%
    select(goupil_subject, goupil_genre, depicts_aat) %>%
    filter(!is.na(depicts_aat)) %>%
    single_separate("depicts_aat") %>%
    gather(depicts_aat_index, depicts_aat, contains("depicts_aat")) %>%
    mutate_at(vars(depicts_aat), funs(as.integer)) %>%
    left_join(select(goupil_with_ids, star_record_no, subject, genre), by = c("goupil_subject" = "subject", "goupil_genre" = "genre")) %>%
    select(-goupil_subject, -goupil_genre, -depicts_aat_index)
}

produce_goupil_sqlite <- function(dbpath,
                                  goupil,
                                  goupil_artists,
                                  goupil_buyers,
                                  goupil_previous_owners,
                                  goupil_classified_as_aat,
                                  goupil_depicts_aat) {

  gdb <- db_setup(dbpath)

  g_srn_single <- list(f_key = "star_record_no", parent_f_key = "star_record_no", parent_tbl_name = "goupil")
  g_srn <- list(g_srn_single)

  write_tbl_key(gdb, goupil, "goupil",
                p_key = "star_record_no")

  write_tbl_key(gdb, goupil_artists, "goupil_artists",
                nn_keys = "star_record_no",
                f_keys = g_srn)

  write_tbl_key(gdb, goupil_buyers, "goupil_buyers",
                nn_keys = "star_record_no",
                f_keys = g_srn)

  write_tbl_key(gdb, goupil_previous_owners, "goupil_previous_owners",
                nn_keys = "star_record_no",
                f_keys = g_srn)

  write_tbl_key(gdb, goupil_classified_as_aat, "goupil_classified_as_aat",
                f_keys = g_srn)

  write_tbl_key(gdb, goupil_depicts_aat, "goupil_depicts_aat",
                f_keys = g_srn)

  db_cleanup(gdb)
}
