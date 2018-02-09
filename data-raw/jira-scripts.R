# Manual snippets used for intermediary data cleaning upstream in STAR

library(remake)
library(tidyverse)
library(googlesheets)
library(assertr)
create_bindings()


# 246 - sale contents - reimport french prices ----

fp <- sales_contents_prices %>%
  left_join(select(sales_contents, puri, project)) %>%
  filter(project == "F")

fp %>% filter(is.na(price_currency_aat)) %>% View()

french_prices_reimport <- fp %>%
  bind_re_match(price_currency, "livres (?<digits>\\d+)") %>%
  filter(!is.na(digits)) %>%
  mutate(
    new_price = paste(price_amount, digits, sep = "."),
    new_currency = "livres") %>%
  select(puri, price_amount = new_price, price_currency = new_currency)

fp %>% anti_join(currency_aat) %>% View()

# 252 - Sales Contents - duplicated records ----

sales_contents_duplicates <- sales_contents %>%
  add_count(catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day) %>%
  filter(n >= 2) %>%
  select(puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day, title, title_modifier, lot_notes) %>%
  arrange(catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day)

applicable_artists <- sales_contents_artists %>%
  filter(puri %in% sales_contents_duplicates$puri) %>%
  group_by(puri) %>%
  summarize(artist_name = rollup(artist_name)) %>%
  assert(is_uniq, puri)

sales_contents_duplicates <- sales_contents_duplicates %>%
  left_join(applicable_artists, by = "puri") %>%
  assert(is_uniq, puri)

make_report(sales_contents_duplicates)

# 278 - Artist Generics ----

generics <- gs_read(gs_url("https://docs.google.com/spreadsheets/d/1ZwjVSwaVjBnA_n8p3Fd5X2nX1JOHi3LYK1pE8ZrfOJU"), ws = "generic_authorities_queries_label_reconciled.csv", col_types = paste0(rep("c", 86), collapse = ""))

ics <- c("star_record_no",	"artist_authority",	"variant_names",	"nationality",	"artist_early",	"artist_late",	"century_active",	"active_city_date",	"subjects_painted",	"notes")

sc_generics_count <- sales_contents_artists %>%
  filter(art_authority %in% generics$artist_authority) %>%
  count(art_authority, sort = TRUE) %>%
  rename(sc_count = n)

k_generics_count <- knoedler_artists %>%
  filter(artist_authority %in% generics$artist_authority) %>%
  count(artist_authority, sort = TRUE) %>%
  rename(k_count = n)

mj <- generics %>%
  norm_vars(base_names = c("Vocab_ID", "URL", "Score", "type", "names", "nationalities", "roles"), n_reps = 10, idcols = ics) %>%
  filter(type == "Person") %>%
  left_join(k_generics_count, by = "artist_authority") %>%
  left_join(sc_generics_count, by = c("artist_authority" = "art_authority")) %>%
  arrange(desc(k_count), desc(sc_count), artist_authority, type, desc(Score)) %>%
  group_by(artist_authority) %>%
  mutate(is_first = row_number() == 1) %>%
  ungroup() %>%
  mutate_at(vars(artist_authority), funs(case_when(is_first ~ ., TRUE ~ NA_character_))) %>%
  select(-is_first)

write_clip(mj)

# 306 - Join new Artist ULAN ids from Vocabs ----

artist_ulan_ids <- readxl::read_excel("~/Downloads/ProvArtists_ULANConcordance_20171207.xlsx", col_names = c("star_record_no", "ulan_id"), col_types = c("text", "text"), skip = 1) %>%
  mutate(star_record_no = str_replace(star_record_no, "a", "")) %>%
  mutate_all(as.integer)

artists_authority_reimport <- artist_ulan_ids %>%
  inner_join(select(artists_authority, -ulan_id), by = "star_record_no") %>%
  select(star_record_no, ulan_id)

artist_ulan_ids_star <- artist_ulan_ids %>%
  left_join(select(artists_authority, star_record_no, artist_authority), by = "star_record_no") %>%
  select(-star_record_no)

knoedler_artists_ulan_id <- raw_knoedler %>%
  select(star_record_no, art_authority_1, art_authority_2) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_1" = "artist_authority")) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_2" = "artist_authority")) %>%
  rename(artist_ulan_id_1 = ulan_id.x, artist_ulan_id_2 = ulan_id.y)

make_report(knoedler_artists_ulan_id)

# 294 - Sales Contents - Unvalidated Artist Authority Names ----

unvalidated_authority_names <- sales_contents_artists %>%
  filter(!is.na(art_authority) & !(art_authority %in% c("NEW", "NON-UNIQUE", "Non-unique"))) %>%
  anti_join(artists_authority, by = c("art_authority" = "artist_authority"))

make_report(unvalidated_authority_names)

# 308 - Sales Contents join ULAN ids to validated artists ----

artist_ulan_ids_star <- artists_authority %>%
  select(artist_authority, ulan_id)

sales_contents_artists_reimport <- raw_sales_contents %>%
  select(persistent_puid, art_authority_1, art_authority_2, art_authority_3, art_authority_4, art_authority_5) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_1" = "artist_authority")) %>%
  rename(artist_ulan_id_1 = ulan_id) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_2" = "artist_authority")) %>%
  rename(artist_ulan_id_2 = ulan_id) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_3" = "artist_authority")) %>%
  rename(artist_ulan_id_3 = ulan_id) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_4" = "artist_authority")) %>%
  rename(artist_ulan_id_4 = ulan_id) %>%
  left_join(artist_ulan_ids_star, by = c("art_authority_5" = "artist_authority")) %>%
  rename(artist_ulan_id_5 = ulan_id) %>%
  filter(!is.na(artist_ulan_id_1) | !is.na(artist_ulan_id_2) | !is.na(artist_ulan_id_3) | !is.na(artist_ulan_id_4) | !is.na(artist_ulan_id_5)) %>%
  select(-(contains("art_authority")))

make_report(sales_contents_artists_reimport)

multi_artists <- sales_contents_artists %>%
  add_count(puri) %>%
  filter(n > 1) %>%
  arrange(puri)

# 310 - reimport old dimensions to sales contents ----

load("~/Desktop/march_sales_contents.rda")

unmodified_dimensions <- march_sales_contents %>%
  select(catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day, dimensions) %>%
  filter(!is.na(dimensions))

joined_unmodified_dimensions <- raw_sales_contents %>%
  select(persistent_puid, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day, modified_dimensions = dimensions) %>%
  inner_join(unmodified_dimensions, by = c("catalog_number", "lot_number", "lot_sale_year", "lot_sale_month", "lot_sale_day")) %>%
  group_by(persistent_puid) %>%
  summarize_all(funs(pick))

joined_unmodified_dimensions <- joined_unmodified_dimensions %>%
  assert(is_uniq, persistent_puid) %>%
  select(persistent_puid, dimensions, modified_dimensions)

changed_joined <- joined_unmodified_dimensions %>%
  filter(dimensions != modified_dimensions)

original_dimensions_reimport <- joined_unmodified_dimensions %>%
  select(persistent_puid, original_dimensions = dimensions)

unfixed_dimensions <- raw_sales_contents %>%
  select(persistent_puid, dimensions) %>%
  filter(!is.na(dimensions)) %>%
  anti_join(joined_unmodified_dimensions, by = "persistent_puid")

make_report(joined_unmodified_dimensions)

missing_dims <- read_csv("~/Downloads/missing.csv") %>%
  select(-(X5:X9)) %>%
  separate(lot_sale_date, into = c("lot_sale_year", "lot_sale_month", "lot_sale_day")) %>%
  mutate_at(vars(lot_number), funs(str_pad(., width = 4, side = "left", pad = "0"))) %>%
  left_join(select(sales_contents, puri, project), by = c("persistent_uid" = "puri")) %>%
  mutate(catalog_number = paste(project, catalog_number, sep = "-")) %>%
  left_join(select(march_sales_contents, catalog_number, lot_number, contains("lot_sale"), dimensions), by = c("catalog_number", "lot_number"))

# 248 - Knoedler present location institution reconciliation  ----

knoedler_present_location_worksheet <- gs_read(gs_url("https://docs.google.com/spreadsheets/d/1FpHe4zWjFzBrxAm1CQ-VwnC0LmszDIyZ1u-X-DD8rqI")) %>%
  select(star_record_no, pres_own_ulan_id = ulan_id) %>%
  mutate_all(as.character) %>%
  filter(!is.na(pres_own_ulan_id)) %>%
  distinct()

knoedler_present_location_ulan_reimport <- raw_knoedler %>%
  select(star_record_no, present_loc_inst) %>%
  filter(!is.na(present_loc_inst)) %>%
  left_join(knoedler_present_location_worksheet) %>%
  filter(!is.na(pres_own_ulan_id))

make_report(knoedler_present_location_ulan_reimport)

# 260 - Goupil object type and material ----

goupil_objecttype_materials <- raw_goupil %>%
  count(object_type, materials, sort = TRUE)
write_clip(goupil_objecttype_materials)

# 202 - Sales Contents artist attribution modifiers ----

sc_attr <- sales_contents_artists %>%
  filter(!is.na(attrib_mod_auth)) %>%
  single_separate(source_col = "attrib_mod_auth") %>%
  norm_vars(base_names = "attrib_mod_auth", n_reps = 4, idcols = "puri")

# 161 - Sales contents shared prices ----

transaction_ids <- sales_contents_prices %>%
  # When there are multiple price entries, just pick the first note in each one
  group_by(puri) %>%
  summarize(price_note = pick(price_note)) %>%
  left_join(select(sales_contents_ids, puri, catalog_number), by = "puri") %>%
  mutate(joining_note = if_else(str_detect(price_note, regex("(f[üu]r|pour|for|avec)", ignore_case = TRUE)), price_note, NA_character_)) %>%
  mutate(transaction_id = if_else(is.na(joining_note), paste0("transaction-", seq_along(puri)), paste0("group-transaction-", group_indices(., catalog_number, joining_note))))

missing_price_note_candidates <- transaction_ids %>%
  filter(str_detect(transaction_id, 'group')) %>%
  add_count(transaction_id) %>%
  filter(n == 1) %>%
  select(-joining_note, -transaction_id, -n, -catalog_number) %>%
  left_join(select(sales_contents, puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = "puri") %>%
  mutate_at(vars(lot_number), funs(str_pad(., width = 4, side = "left", pad = "0"))) %>%

  arrange(catalog_number)

error_fixes <- read_csv("~/Downloads/error_paired_sales_2017-08-01.csv") %>%
  mutate_at(vars(lot_number), funs(str_pad(., width = 4, side = "left", pad = "0"))) %>%
  mutate_at(vars(price_note), funs(str_replace(., "\xcc_", "ü")))



merged_joins <- missing_price_note_candidates %>%
  left_join(select(error_fixes, catalog_number, lot_number, new_price_note = price_note, absent_lots, star_edit), by = c("catalog_number", "lot_number"))

absent_lots <- merged_joins %>%
  mutate_at(vars(absent_lots), funs(if_else(is.na(absent_lots), lot_number, paste(lot_number, absent_lots, sep = ";")))) %>%
  single_separate("absent_lots") %>%
  norm_vars(base_names = "absent_lots", n_reps = 17, idcols = "puri") %>%
  rename(lot_number = absent_lots) %>%
  mutate_at(vars(lot_number), funs(str_pad(., width = 4, side = "left", pad = "0"))) %>%
  left_join(select(merged_joins, -absent_lots, -lot_number), by = "puri") %>%
  left_join(select(sales_contents, target_puri = puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day)) %>%
  mutate(inferred_row = factor(if_else(puri != target_puri, "matched row", "original row", missing = "no match found"), levels = c("original row", "matched row", "no match found"), ordered = TRUE)) %>%
  select(target_puri, source_puri = puri, inferred_row, original_price_note = price_note, new_price_note, everything()) %>%
  arrange(catalog_number, source_puri, inferred_row)

make_report(absent_lots)

price_corrections <- gs_read(gs_url("https://docs.google.com/spreadsheets/d/1iMQ3014EhVdtgqhsX2qLnA9qrBEcWfexSyqhJCVZ6T8"), ws = "processed 2018-01-22")

out_of_scope_lots <- price_corrections %>%
  filter(target_puri == "X") %>%
  inner_join(select(sales_contents_prices, puri, price_amount, price_currency, price_source, price_citation), by = c("source_puri" = "puri")) %>%
  select(
    catalog_number,
    lot_number,
    lot_sale_year,
    lot_sale_month,
    lot_sale_day,
    price_amount,
    price_currency,
    price_note = new_price_note,
    price_source,
    price_citation) %>%
  anti_join(sales_contents, by = c("catalog_number", "lot_number", "lot_sale_year", "lot_sale_month", "lot_sale_day"))

price_notes_reimport <- price_corrections %>%
  filter(target_puri != "X" & !is.na(new_price_note) & original_price_note != new_price_note & is.na(star_edit)) %>%
  select(puri = target_puri, price_note = new_price_note) %>%
  distinct()

make_report(price_notes_reimport)

# 314 1-to-1 between Buyer and Buy Auth Mod ----

mismatch_buy_mod_auth <- sales_contents_buyers %>%
  filter(!is.na(buy_auth_mod_a) & is.na(buy_auth_name))

make_report(mismatch_buy_mod_auth)

# 320 1-to-1 between Seller and Sell Auth Mods ----

mismatch_sell_mod_auth <- sales_contents_sellers %>%
  filter(!is.na(sell_auth_mod_a) & is.na(sell_auth_name))

make_report(mismatch_sell_mod_auth)

# 148/149 Sales Contents prev/post sales ----

target_sales_contents <- sales_contents %>%
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
sales_prev_join <- sales_contents_prev_sales %>%
  rename(prev_puri = puri) %>%
  inner_join(target_sales_contents, by = c(
    "prev_sale_loc" = "sale_loc",
    "prev_sale_lot" = "lot_number",
    "prev_sale_year" = "lot_sale_year",
    "prev_sale_mo" = "lot_sale_month",
    "prev_sale_day" = "lot_sale_day"
  ))

# Records that claim to have a match, but we can't find it
failed_prev_match <- sales_contents_prev_sales %>%
  # Only keep those that could not be successfully matched
  anti_join(sales_prev_join, by = c("puri" = "prev_puri")) %>%
  # Add additional identifying information to those records for editors to see
  inner_join(select(sales_contents, puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = "puri")

# Records that claim to have a match, and we get MULTIPLE matches back
multiple_prev_match <- sales_prev_join %>%
  # Only keep those joins that succeeded
  filter(!is.na(target_puri)) %>%
  # To find duplicated rows, find those that have been duplicated based on the
  # original join query
  add_count(prev_puri, prev_sale_lot, prev_sale_loc, prev_sale_day, prev_sale_mo, prev_sale_year) %>%
  filter(n > 1) %>%
  # Join the records for the _target_ sales so that editors can find them
  inner_join(select(sales_contents, puri, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = c("target_puri" = "puri"))

# Only those records with exactly one match
exact_prev_match <- sales_prev_join %>%
  anti_join(multiple_prev_match, by = "prev_puri")

# Attempt to join these candidate sales contents records to the records
# indicated in sales_contents_post_sales
message("- Attempt to match post sales")
sales_post_join <- sales_contents_post_sales %>%
  rename(post_puri = puri) %>%
  inner_join(target_sales_contents, by = c(
    "post_sale_loc" = "sale_loc",
    "post_sale_lot" = "lot_number",
    "post_sale_yr" = "lot_sale_year",
    "post_sale_mo" = "lot_sale_month",
    "post_sale_day" = "lot_sale_day"
  ))

# Records that claim to have a match, but we can't find it
failed_post_match <- sales_contents_post_sales %>%
  # Only keep those that could not be successfully matched
  anti_join(sales_post_join, by = c("puri" = "post_puri")) %>%
  # Add additional identifying information to those records for editors to see
  inner_join(select(sales_contents, puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = "puri")

multiple_post_match <- sales_post_join %>%
  filter(!is.na(target_puri)) %>%
  # Must group by the entire unqiue row from the original
  # sales_contents_post_sales
  add_count(post_puri, post_sale_lot, post_sale_loc, post_sale_day, post_sale_mo, post_sale_yr) %>%
  filter(n > 1) %>%
  inner_join(select(sales_contents, puri, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = c("target_puri" = "puri")) %>%
  arrange(post_puri)

# Keep only those with exactly one match
exact_post_match <- sales_post_join %>%
  anti_join(multiple_post_match, by = "post_puri")

failed_post_match %>%
  mutate(db = str_extract(puri, "^[A-Z]+")) %>%
  filter(post_sale_yr < 2020) %>%
  ggplot(aes(x = post_sale_yr, fill = db)) +
  geom_histogram(binwidth = 3) +
  labs(title = "Failed post_sale matches")
ggsave("~/Desktop/failed_post_sale.png")

failed_prev_match %>%
  mutate(db = str_extract(puri, "^[A-Z]+")) %>%
  ggplot(aes(x = prev_sale_year, fill = db)) +
  geom_histogram(binwidth = 3) +
  labs(title = "Failed prev_sale matches")
ggsave("~/Desktop/failed_prev_sale.png")

make_report(failed_post_match)
make_report(failed_prev_match)

all_oos_lots <- gs_read(gs_url("https://docs.google.com/spreadsheets/d/1CyUvyNcEeFgzTVmUpRvrVjZ7SDsILCSfCaETzU_T3Yk"), ws = "post sales")

inscope_failed_post_match <- failed_post_match %>%
  anti_join(all_oos_lots, by = c("puri", "post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc"))


# Create transaction graph
# For any given record, it is not guaranteed that it has prev/post references
# to every single sale that may have concerned the same object. It is
# necessary to find all records that share a link with each other across any
# number of siblings. In other words, we want to identify all the connected
# components of the graph representing the prev/post connections between each
# sales contents records. This is a simple network analysis task.

library(igraph)

# Merge each of the exact match tables into one large adjacency list
message("- Identify connected components in graph of related sales")
transaction_edgelist <- bind_rows(
  select(exact_post_match, source = post_puri, target = target_puri),
  select(exact_prev_match, source = prev_puri, target = target_puri))

transaction_nodes <- sales_contents %>%
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

# Prioritized failed prev/post sale matches based on which ones would otherwise
# have no matches whatsoever.
otherwise_unreached_post_sales <- failed_post_match %>%
  anti_join(all_oos_lots, by = c("puri", "post_sale_yr", "post_sale_mo", "post_sale_day", "post_sale_loc")) %>%
  mutate(has_other_match = puri %in% transaction_nodes$name) %>%
  arrange(has_other_match)

