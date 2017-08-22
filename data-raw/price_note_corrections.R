

price_notes <- sales_contents_prices %>%
  inner_join(select(sales_contents, star_record_no, catalog_number, lot_number), by = "star_record_no") %>%
  filter(str_detect(price_note, regex("(for|pour|für)", ignore_case = TRUE))) %>%
  mutate(transaction_id = group_indices(., catalog_number, price_note)) %>%
  add_count(transaction_id) %>%
  arrange(desc(n, catalog_number))


eric <- read_csv("~/Downloads/error_paired_sales_2017-08-01.csv", locale = locale(encoding = "latin1")) %>%
  # Clean some bad character encoding due to Excel
  mutate(adjusted_note = str_replace_all(price_note, c("fÌ_r" = "für", "GemÌ_lde" = "Gemälde")))

ej <- eric %>%
  select(-price_note) %>%
  left_join(price_notes, by = c("star_record_no", "catalog_number", "lot_number"))
