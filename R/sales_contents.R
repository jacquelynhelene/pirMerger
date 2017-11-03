# Sales Contents Normalization ----

#' Produce sales contents tables
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_sales_contents_ids <- function(raw_sales_contents) {
  raw_sales_contents %>%
    mutate_at(vars(lot_sale_year, lot_sale_month, lot_sale_day), funs(as.integer)) %>%
    mutate(project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    rename(puri = persistent_puid) %>%
    select(-star_record_no) %>%
    assertr::assert(assertr::not_na, puri)
}

produce_sales_contents <- function(sales_contents, sales_contents_prev_sales, sales_contents_post_sales) {
  sales_contents %>%
    select(-(expert_auth_1:expert_auth_4)) %>%
    select(-(commissaire_pr_1:commissaire_pr_4)) %>%
    select(-(artist_name_1:star_rec_no_5)) %>%
    select(-(hand_note_1:hand_note_so_7)) %>%
    select(-(dplyr::contains("sell_"))) %>%
    select(-(buy_name_1:buy_auth_modq_5)) %>%
    select(-(price_amount_1:price_citation_3)) %>%
    select(-(prev_owner_1:prev_own_auth_q_9)) %>%
    select(-(prev_sale_year_1:prev_sale_coll_7)) %>%
    select(-(post_sale_yr_1:post_sale_col_13)) %>%
    select(-(post_own_1:post_own_auth_q_6)) %>%
    identify_unique_objects(sales_contents_prev_sales, sales_contents_post_sales)
}

produce_sales_contents_expert_auth <- function(sales_contents) {
  norm_vars(sales_contents, base_names = "expert_auth", n_reps = 4, idcols = "puri")
}

produce_sales_contents_commissaire_pr <- function(sales_contents) {
  norm_vars(sales_contents, base_names = "commissaire_pr", n_reps = 4, idcols = "puri")
}

produce_sales_contents_artists <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("artist_name", "artist_info", "art_authority", "nationality", "attribution_mod", "star_rec_no"), n_reps = 5, idcols = "puri") %>%
    rename(artist_star_rec_no = star_rec_no) %>%
    mutate_at(vars(artist_star_rec_no), funs(as.integer))
}

produce_sales_contents_hand_notes <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("hand_note", "hand_note_so"), n_reps = 7, idcols = "puri")
}

produce_sales_contents_sellers <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("sell_name", "sell_name_so", "sell_name_ques", "sell_mod", "sell_mod_so", "sell_auth_name", "sell_auth_mod"), n_reps = 5, idcols = "puri")
}

produce_sales_contents_buyers <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("buy_name", "buy_name_so", "buy_name_ques", "buy_name_cite", "buy_mod", "buy_mod_so", "buy_auth_name", "buy_auth_nameq", "buy_auth_mod", "buy_auth_modq"), n_reps = 5, idcols = "puri")
}

produce_sales_contents_prices <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("price_amount", "price_currency", "price_note", "price_source", "price_citation"), n_reps = 3, idcols = "puri")
}

produce_sales_contents_prev_owners <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("prev_owner", "prev_own_ques", "prev_own_so", "prev_own_auth", "prev_own_auth_d", "prev_own_auth_l", "prev_own_auth_q"), n_reps = 9, idcols = "puri")
}

produce_sales_contents_prev_sale <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("prev_sale_year", "prev_sale_mo", "prev_sale_day", "prev_sale_loc", "prev_sale_lot", "prev_sale_ques", "prev_sale_artx", "prev_sale_ttlx", "prev_sale_note", "prev_sale_coll"), n_reps = 7, idcols = "puri") %>%
    mutate_at(vars(prev_sale_year, prev_sale_mo, prev_sale_day), funs(as.integer))
}

proudce_sales_contents_post_sale <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc", "post_sale_lot", "post_sale_q", "post_sale_art", "post_sale_ttl", "post_sale_nte", "post_sale_col"), n_reps = 13, idcols = "puri") %>%
    mutate_at(vars(post_sale_yr, post_sale_mo, post_sale_day), funs(as.integer))
}

proudce_sales_contents_post_owners <- function(sales_contents) {
  norm_vars(sales_contents, base_names = c("post_own", "post_own_q", "post_own_so", "post_own_so_q", "post_own_auth", "post_own_auth_d", "post_own_auth_l", "post_own_auth_q"), n_reps = 6, idcols = "puri")
}

# Sales Descriptions Normalization ----

produce_sales_descriptions_ids <- function(raw_sales_descriptions) {
  raw_sales_descriptions %>%
    mutate_at(vars(sale_begin_year, sale_begin_month, sale_begin_day, sale_end_year, sale_end_month, sale_end_day, no_of_ptgs_lots), funs(as.integer)) %>%
    mutate(description_project = str_extract(catalog_number, "^[A-Za-z]{1,2}")) %>%
    rename(description_puri = persistent_puid) %>%
    select(-star_record_no)
}

produce_sales_descriptions <- function(sales_descriptions) {
  sales_descriptions %>% select(-(lugt_number_1:lugt_number_3)) %>%
    select(-(title_pg_sell_1:title_pg_sell_2)) %>%
    select(-(auc_copy_seller_1:auc_copy_seller_4)) %>%
    select(-(other_seller_1:other_seller_3)) %>%
    select(-(sell_auth_name_1:sell_auth_q_5)) %>%
    select(-(expert_auth_1:expert_auth_4)) %>%
    select(-(commissaire_pr_1:commissaire_pr_4)) %>%
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
    norm_vars(base_names = c("expert_auth"), n_reps = 4, idcols = "description_puri")
}

produce_sales_descriptions_commissaire_pr <-  function(sales_descriptions) {
  sales_descriptions %>%
    norm_vars(base_names = c("commissaire_pr"), n_reps = 4, idcols = "description_puri")
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

identify_unique_objects <- function(scdf, prev_sales, post_sales) {
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
