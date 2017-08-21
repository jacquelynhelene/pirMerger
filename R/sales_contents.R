#' Produce sales contents tables
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_sales_contents <- function(source_dir, target_dir) {
  raw_sales_contents <- get_data(source_dir, "raw_sales_contents")

  sales_contents <- raw_sales_contents

  ### expert_auth
  sales_contents_experts <- norm_vars(sales_contents, base_names = "expert_auth", n_reps = 4, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(expert_auth_1:expert_auth_4))
  save_data(target_dir, sales_contents_experts)

  ### commissaire_pr
  sales_contents_commissaire_pr <- norm_vars(sales_contents, base_names = "commissaire_pr", n_reps = 4, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(commissaire_pr_1:commissaire_pr_4))
  save_data(target_dir, sales_contents_commissaire_pr)

  ### artist_name
  sales_contents_artists <- norm_vars(sales_contents, base_names = c("artist_name", "artist_info", "art_authority", "nationality", "attribution_mod", "star_rec_no"), n_reps = 5, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(artist_name_1:star_rec_no_5))
  save_data(target_dir, sales_contents_artists)

  ### hand_note
  sales_contents_hand_notes <- norm_vars(sales_contents, base_names = c("hand_note", "hand_note_so"), n_reps = 7, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(hand_note_1:hand_note_so_7))
  save_data(target_dir, sales_contents_hand_notes)

  ### sellers
  sales_contents_sellers <- norm_vars(sales_contents, base_names = c("sell_name", "sell_name_so", "sell_name_ques", "sell_mod", "sell_mod_so", "sell_auth_name", "sell_auth_mod"), n_reps = 5, idcols = "star_record_no")

  sales_contents <- sales_contents %>% select(-(dplyr::contains("sell_")))
  save_data(target_dir, sales_contents_sellers)

  ### buyers
  sales_contents_buyers <- norm_vars(sales_contents, base_names = c("buy_name", "buy_name_so", "buy_name_ques", "buy_name_cite", "buy_mod", "buy_mod_so", "buy_auth_name", "buy_auth_nameq", "buy_auth_mod", "buy_auth_modq"), n_reps = 5, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(buy_name_1:buy_auth_modq_5))
  save_data(target_dir, sales_contents_buyers)

  sales_contents_prices <- norm_vars(sales_contents, base_names = c("price_amount", "price_currency", "price_note", "price_source", "price_citation"), n_reps = 3, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(price_amount_1:price_citation_3))
  save_data(target_dir, sales_contents_prices)

  ### prev_own
  sales_contents_prev_owners <- norm_vars(sales_contents, base_names = c("prev_owner", "prev_own_ques", "prev_own_so", "prev_own_auth", "prev_own_auth_d", "prev_own_auth_l", "prev_own_auth_q"), n_reps = 9, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(prev_owner_1:prev_own_auth_q_9))
  save_data(target_dir, sales_contents_prev_owners)

  ### prev_sale
  sales_contents_prev_sales <- norm_vars(sales_contents, base_names = c("prev_sale_year", "prev_sale_mo", "prev_sale_day", "prev_sale_loc", "prev_sale_lot", "prev_sale_ques", "prev_sale_artx", "prev_sale_ttlx", "prev_sale_note", "prev_sale_coll"), n_reps = 7, idcols = "star_record_no")
  sales_contents_prev_sales <- sales_contents_prev_sales %>%
    mutate_at(vars(prev_sale_year, prev_sale_mo, prev_sale_day), funs(as.integer))
  sales_contents <- sales_contents %>% select(-(prev_sale_year_1:prev_sale_coll_7))
  save_data(target_dir, sales_contents_prev_sales)

  ### post_sale
  sales_contents_post_sales <- norm_vars(sales_contents, base_names = c("post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc", "post_sale_lot", "post_sale_q", "post_sale_art", "post_sale_ttl", "post_sale_nte", "post_sale_col"), n_reps = 13, idcols = "star_record_no")
  sales_contents_post_sales <- sales_contents_post_sales %>%
    mutate_at(vars(post_sale_yr, post_sale_mo, post_sale_day), funs(as.integer))
  sales_contents <- sales_contents %>% select(-(post_sale_yr_1:post_sale_col_13))
  save_data(target_dir, sales_contents_post_sales)

  ### post_own
  sales_contents_post_owners <- norm_vars(sales_contents, base_names = c("post_own", "post_own_q", "post_own_so", "post_own_so_q", "post_own_auth", "post_own_auth_d", "post_own_auth_l", "post_own_auth_q"), n_reps = 6, idcols = "star_record_no")
  sales_contents <- sales_contents %>% select(-(post_own_1:post_own_auth_q_6))
  save_data(target_dir, sales_contents_post_owners)

  save(sales_contents, file = "data/sales_contents.rda")
}
