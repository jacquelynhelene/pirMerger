library(pirMerger)
library(stringr)

sales_contents <- get_data("scratch/intermediate_data", "sales_contents")
sales_contents_prices <- get_data("scratch/intermediate_data/", "sales_contents_prices")

price_notes <- sales_contents_prices %>%
  inner_join(select(sales_contents, star_record_no, catalog_number, lot_number), by = "star_record_no") %>%
  filter(str_detect(price_note, regex("(for|pour|für)", ignore_case = TRUE))) %>%
  mutate(transaction_id = group_indices(., catalog_number, price_note)) %>%
  add_count(transaction_id) %>%
  arrange(desc(n))


eric <- read_csv("~/Downloads/error_paired_sales_2017-08-01.csv", locale = locale(encoding = "latin1")) %>%
  # Clean some bad character encoding due to Excel
  mutate(price_note = str_replace_all(price_note, c("fÌ_r" = "für", "GemÌ_lde" = "Gemälde"))) %>%
  # Ignore any records still requiring manual edits
  filter(is.na(star_edit)) %>%
  select(-star_edit) %>%
  mutate(lot_number = str_pad(lot_number, 4, side = "left", pad = "0")) %>%
  left_join(select(old_paired_sales, star_record_no, puri))

# Expand notes
expanded_notes <- eric %>%
  single_separate("absent_lots", sep = ";") %>%
  gather(copy, new_lot, contains("absent_lots"), na.rm = TRUE) %>%
  select(-copy) %>%
  left_join(select(sales_contents, star_record_no, source_lot_sale_year = lot_sale_year, source_lot_sale_month = lot_sale_month, source_lot_sale_day = lot_sale_day), by = "star_record_no") %>%
  rename(original_srn = star_record_no, original_lot = lot_number) %>%
  left_join(select(sales_contents, star_record_no, catalog_number, lot_number, lot_sale_year, lot_sale_month, lot_sale_day), by = c("catalog_number" = "catalog_number", "new_lot" = "lot_number")) %>%
  mutate(match_id = group_indices(., catalog_number, new_lot)) %>%
  add_count(match_id) %>%
  arrange(desc(n)) %>%
  select(
    source_star_record_no = original_srn,
    source_catalog_number = catalog_number,
    source_lot_number = original_lot,
    source_price_note = price_note,
    source_lot_sale_year,
    source_lot_sale_month,
    source_lot_sale_day,
    candidate_star_record_no = star_record_no,
    candidate_lot_number = new_lot,
    lot_sale_year,
    lot_sale_month,
    lot_sale_day,
    match_id,
    n)

# Exactly one match found for candidate lots. These can be reincorporated into
# Eric's existing list of edits
exact_matches <- expanded_notes %>%
  filter(n == 1 & !is.na(candidate_star_record_no))

# Candidate lots identified by eric that have no matches in the STAR db
no_matches <- expanded_notes %>%
  filter(is.na(candidate_star_record_no)) %>%
  arrange(source_star_record_no)

# Price notes that return multiple possible matches. Editor will decide which
# candidate matches are the correct ones, and their decisions will be returned
# to the list of other exact matches.
decision_notes <- expanded_notes %>%
  filter(n > 1) %>%
  arrange(desc(n), source_star_record_no)
