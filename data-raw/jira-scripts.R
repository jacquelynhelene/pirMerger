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

generics <- gs_read(gs_url("https://docs.google.com/spreadsheets/d/1yp7UkDk000mVQAM2vgpClA9wzKqQx4-HUeI26_AZnDE"), ws = "generic_authorities_queries_label_reconciled.csv", col_types = paste0(rep("c", 86), collapse = ""))

ics <- c("star_record_no",
         "artist_authority",
         "variant_names",
         "nationality",
         "artist_early",
         "artist_late",
         "century_active",
         "active_city_date",
         "subjects_painted",
         "notes",
         "num_of_records",
         "ulan_id",
         "artist_authority_clean")

# Prioritize Knoedler records first
k_generics_count <- knoedler_artists %>%
  filter(artist_authority %in% generics$artist_authority) %>%
  count(artist_authority, sort = TRUE) %>%
  rename(k_count = n)

mj <- generics %>%
  norm_vars(base_names = c("Vocab_ID", "URL", "Score", "type", "names", "nationalities", "roles"), n_reps = 10, idcols = ics) %>%
  left_join(k_generics_count, by = "artist_authority") %>%
  arrange(desc(k_count), desc(as.integer(num_of_records)), artist_authority, type, desc(Score)) %>%
  group_by(artist_authority) %>%
  mutate(is_first = row_number() == 1) %>%
  ungroup() %>%
  mutate_at(vars(one_of(setdiff(ics, c("star_record_no", "k_count")))), funs(case_when(is_first ~ ., TRUE ~ NA_character_))) %>%
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
  select(star_record_no, art_authority_1, art_authority_2, art_authority_3, art_authority_4, art_authority_5) %>%
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
