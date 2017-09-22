#' Produce sales contents tables
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_sales_contents <- function(source_dir, target_dir) {
  message("Reading raw_sales_contents")
  raw_sales_contents <- get_data(source_dir, "raw_sales_contents")

  # Coerce appropriate columns to integer/numeric
  sales_contents <- raw_sales_contents %>%
    mutate_at(vars(star_record_no, lot_sale_year, lot_sale_month, lot_sale_day), funs(as.integer)) %>%
    mutate(project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    rename(puri = persistent_puid)

  ### expert_auth
  message("- Sales contents experts")
  sales_contents_experts <- norm_vars(sales_contents, base_names = "expert_auth", n_reps = 4, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(expert_auth_1:expert_auth_4))
  save_data(target_dir, sales_contents_experts)

  ### commissaire_pr
  message("- sales contents commissaire pr")
  sales_contents_commissaire_pr <- norm_vars(sales_contents, base_names = "commissaire_pr", n_reps = 4, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(commissaire_pr_1:commissaire_pr_4))
  save_data(target_dir, sales_contents_commissaire_pr)

  ### artist_name
  message("- sales contents artists")
  sales_contents_artists <- norm_vars(sales_contents, base_names = c("artist_name", "artist_info", "art_authority", "nationality", "attribution_mod", "star_rec_no"), n_reps = 5, idcols = "puri") %>%
    rename(artist_star_rec_no = star_rec_no) %>%
    mutate_at(vars(artist_star_rec_no), funs(as.integer))
  sales_contents <- sales_contents %>% select(-(artist_name_1:star_rec_no_5))
  save_data(target_dir, sales_contents_artists)

  ### hand_note
  message("- sales contents hand notes")
  sales_contents_hand_notes <- norm_vars(sales_contents, base_names = c("hand_note", "hand_note_so"), n_reps = 7, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(hand_note_1:hand_note_so_7))
  save_data(target_dir, sales_contents_hand_notes)

  ### sellers
  message("- sales contents sellers")
  sales_contents_sellers <- norm_vars(sales_contents, base_names = c("sell_name", "sell_name_so", "sell_name_ques", "sell_mod", "sell_mod_so", "sell_auth_name", "sell_auth_mod"), n_reps = 5, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(dplyr::contains("sell_")))
  save_data(target_dir, sales_contents_sellers)

  ### buyers
  message("- sales contents buyers")
  sales_contents_buyers <- norm_vars(sales_contents, base_names = c("buy_name", "buy_name_so", "buy_name_ques", "buy_name_cite", "buy_mod", "buy_mod_so", "buy_auth_name", "buy_auth_nameq", "buy_auth_mod", "buy_auth_modq"), n_reps = 5, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(buy_name_1:buy_auth_modq_5))
  save_data(target_dir, sales_contents_buyers)

  message("- sales contents prices")
  sales_contents_prices <- norm_vars(sales_contents, base_names = c("price_amount", "price_currency", "price_note", "price_source", "price_citation"), n_reps = 3, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(price_amount_1:price_citation_3))
  save_data(target_dir, sales_contents_prices)

  ### prev_own
  message("- sales contents prev own")
  sales_contents_prev_owners <- norm_vars(sales_contents, base_names = c("prev_owner", "prev_own_ques", "prev_own_so", "prev_own_auth", "prev_own_auth_d", "prev_own_auth_l", "prev_own_auth_q"), n_reps = 9, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(prev_owner_1:prev_own_auth_q_9))
  save_data(target_dir, sales_contents_prev_owners)

  ### prev_sale
  message("- sales contents prev sale")
  sales_contents_prev_sales <- norm_vars(sales_contents, base_names = c("prev_sale_year", "prev_sale_mo", "prev_sale_day", "prev_sale_loc", "prev_sale_lot", "prev_sale_ques", "prev_sale_artx", "prev_sale_ttlx", "prev_sale_note", "prev_sale_coll"), n_reps = 7, idcols = "puri")
  sales_contents_prev_sales <- sales_contents_prev_sales %>%
    mutate_at(vars(prev_sale_year, prev_sale_mo, prev_sale_day), funs(as.integer))
  sales_contents <- sales_contents %>% select(-(prev_sale_year_1:prev_sale_coll_7))
  save_data(target_dir, sales_contents_prev_sales)

  ### post_sale
  message("- sales contents post sale")
  sales_contents_post_sales <- norm_vars(sales_contents, base_names = c("post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc", "post_sale_lot", "post_sale_q", "post_sale_art", "post_sale_ttl", "post_sale_nte", "post_sale_col"), n_reps = 13, idcols = "puri")
  sales_contents_post_sales <- sales_contents_post_sales %>%
    mutate_at(vars(post_sale_yr, post_sale_mo, post_sale_day), funs(as.integer))
  sales_contents <- sales_contents %>% select(-(post_sale_yr_1:post_sale_col_13))
  save_data(target_dir, sales_contents_post_sales)

  ### post_own
  message("- sales contents post own")
  sales_contents_post_owners <- norm_vars(sales_contents, base_names = c("post_own", "post_own_q", "post_own_so", "post_own_so_q", "post_own_auth", "post_own_auth_d", "post_own_auth_l", "post_own_auth_q"), n_reps = 6, idcols = "puri")
  sales_contents <- sales_contents %>% select(-(post_own_1:post_own_auth_q_6))
  save_data(target_dir, sales_contents_post_owners)

  message("- final normalized sales contents")
  save_data(target_dir, sales_contents)
}

#' Produce sales descriptions tables
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_sales_descriptions <- function(source_dir, target_dir) {
  message("Reading raw_sales_descriptions")
  raw_sales_descriptions <- get_data(source_dir, "raw_sales_descriptions")

  sales_descriptions <- raw_sales_descriptions %>%
    mutate_at(vars(star_record_no, sale_begin_year, sale_begin_month, sale_begin_day, sale_end_year, sale_end_month, sale_end_day, no_of_ptgs_lots), funs(as.integer)) %>%
    mutate(project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    rename(puri = persistent_puid)

  ### lugt numbers
  sales_descriptions_lugt_numbers <- sales_descriptions %>%
    norm_vars(base_names = "lugt_number", n_reps = 3, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(lugt_number_1:lugt_number_3))
  save_data(target_dir, sales_descriptions_lugt_numbers)

  ### Sellers from title page
  sales_descriptions_title_seller <- sales_descriptions %>%
    norm_vars(base_names = "title_pg_sell", n_reps = 2, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(title_pg_sell_1:title_pg_sell_2))
  save_data(target_dir, sales_descriptions_title_seller)

  ### Sellers from auctioneer's copy
  sales_descriptions_auc_copy_seller <- sales_descriptions %>%
    norm_vars(base_names = "auc_copy_seller", n_reps = 4, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(auc_copy_seller_1:auc_copy_seller_4))
  save_data(target_dir, sales_descriptions_auc_copy_seller)

  ### Sellers from other sources
  sales_descriptions_other_seller <- sales_descriptions %>%
    norm_vars(base_names = c("other_seller"), n_reps = 3, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(other_seller_1:other_seller_3))
  save_data(target_dir, sales_descriptions_other_seller)

  ### Seller authority names (not yet exported properly)
  sales_descriptions_auth_seller <- sales_descriptions %>%
    norm_vars(base_names = c("sell_auth_name", "sell_auth_q"), n_reps = 5, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(sell_auth_name_1:sell_auth_q_5))
  save_data(target_dir, sales_descriptions_auth_seller)

  ### expert_auth
  sales_descriptions_expert_auth <- sales_descriptions %>%
    norm_vars(base_names = c("expert_auth"), n_reps = 4, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(expert_auth_1:expert_auth_4))
  save_data(target_dir, sales_descriptions_expert_auth)

  ### commissaire_pr
  sales_descriptions_commissaire_pr <- sales_descriptions %>%
    norm_vars(base_names = c("commissaire_pr"), n_reps = 4, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(commissaire_pr_1:commissaire_pr_4))
  save_data(target_dir, sales_descriptions_commissaire_pr)

  ### auction house
  sales_descriptions_auction_house <- sales_descriptions %>%
    norm_vars(base_names = c("auc_house_name", "auc_house_auth"), n_reps = 4, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(auc_house_name_1:auc_house_auth_4))
  save_data(target_dir, sales_descriptions_auction_house)

  sales_descriptions_country <- sales_descriptions %>%
    # small naming variation needs to be fixed
    rename(country_auth_1 = country_auth) %>%
    norm_vars(base_names = c("country_auth"), n_reps = 2, idcols = "puri")
  sales_descriptions <- sales_descriptions %>% select(-(country_auth:country_auth_2))

  save_data(target_dir, sales_descriptions)
}

#' Produce sales catalog info tables
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_sales_catalogs_info <- function(source_dir, target_dir) {
  message("Reading raw_sales_catalogs_info")
  raw_sales_catalogs_info <- get_data(source_dir, "raw_sales_catalogs_info")
  raw_sales_catalogs_loccodes <- get_data(source_dir, "raw_sales_catalogs_loccodes")

  sales_catalogs_info <- raw_sales_catalogs_info %>%
    left_join(select(raw_sales_catalogs_loccodes, -star_record_no), by = "owner_code")

  save_data(target_dir, sales_catalogs_info)
}
