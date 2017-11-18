
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
  select(puri, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day, title, title_modifier, lot_notes)

applicable_artists <- sales_contents_artists %>%
  filter(puri %in% sales_contents_duplicates$puri) %>%
  group_by(puri) %>%
  summarize(artist_name = rollup(artist_name)) %>%
  assert(is_uniq, puri)

sales_contents_duplicates <- sales_contents_duplicates %>%
  left_join(applicable_artists, by = "puri") %>%
  assert(is_uniq, puri)
make_report(sales_contents_duplicates)
