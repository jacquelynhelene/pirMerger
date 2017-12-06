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
