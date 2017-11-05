produce_goupil_ids <- function(raw_goupil) {
  raw_goupil %>%
    mutate_at(vars(star_record_no, contains("stock_book"), page_number, row_number, contains("_date_")), funs(as.integer)) %>%
    select(-persistent_uid, -original_file_name)
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
  norm_vars(goupil, base_names = c("buyer_name", "buyer_loc", "buyer_mod", "buy_auth_name", "buy_auth_mod", "buyer_ulan_id"), n_reps = 2, idcols = "star_record_no")
}

produce_goupil_previous_owners <- function(goupil) {
  norm_vars(goupil, base_names = c("previous_owner"), n_reps = 2, idcols = "star_record_no")
}
