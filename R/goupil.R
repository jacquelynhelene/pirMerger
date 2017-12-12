produce_goupil_ids <- function(raw_goupil) {
  raw_goupil %>%
    mutate_at(vars(contains("stock_book"), page_number, row_number, contains("_date_")), funs(as.integer)) %>%
    select(-persistent_uid, -original_file_name) %>%
    assert(not_na, star_record_no) %>%
    assert(is_uniq, star_record_no)
}

produce_goupil <- function(goupil_with_ids) {
  goupil_with_ids %>%
    select(-(stock_book_no_1:stock_book_row_15)) %>%
    select(-(artist_name_1:artist_ulan_id_2)) %>%
    select(-(buyer_name_1:buyer_ulan_id_2)) %>%
    select(-(previous_owner_1:previous_owner_2))
}

produce_goupil_stock_book_nos <- function(goupil) {
  norm_vars(goupil, base_names = c("stock_book_no", "stock_book_goupil_no", "stock_book_page", "stock_book_row"), n_reps = 15, idcols = "star_record_no")
}

produce_goupil_artists <- function(goupil) {
  norm_vars(goupil, base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "star_rec_no", "artist_ulan_id"), n_reps = 2, idcols = "star_record_no") %>%
    # Join ulan ids to this list
    rename(artist_star_record_no = star_rec_no, artist_authority = art_authority, artist_nationality = nationality, artist_attribution_mod = attribution_mod)
}

produce_goupil_buyers <- function(goupil) {
  norm_vars(goupil, base_names = c("buyer_name", "buyer_loc", "buyer_mod", "buy_auth_name", "buy_auth_addr", "buy_auth_mod", "buyer_ulan_id"), n_reps = 2, idcols = "star_record_no")
}

produce_goupil_previous_owners <- function(goupil) {
  norm_vars(goupil, base_names = c("previous_owner"), n_reps = 2, idcols = "star_record_no")
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

produce_joined_goupil <- function(goupil, goupil_artists, goupil_buyers, goupil_previous_owners, goupil_present_location_ulan, goupil_classified_as_aat, goupil_depicts_aat) {
  goupil %>%
    pipe_message("- Joining goupil_artists to goupil") %>%
    left_join(spread_out(goupil_artists, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining goupil_buyers to goupil") %>%
    left_join(spread_out(goupil_buyers, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining goupil_previous_owners to goupil") %>%
    left_join(spread_out(goupil_previous_owners, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining goupil_present_location_ulan to goupil") %>%
    left_join(spread_out(goupil_present_location_ulan, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining goupil_classified_as_aat to goupil") %>%
    left_join(spread_out(goupil_classified_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Joining goupil_depicts_aat to goupil") %>%
    left_join(spread_out(goupil_depicts_aat, "star_record_no"), by = "star_record_no")
}
