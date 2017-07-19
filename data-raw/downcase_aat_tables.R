# one-off script to set matching terms for knoedler/sc reconciliation tables to
# lowercase and concatenate existing AAT ids

secret::update_secret("data_definitions", value = paste0(read_clip(), collapse = "\n"))
library(tidyverse)

k_aat <- readRDS("scratch/source_data/raw_knoedler_subject_aat.rds")
sc_aat <- readRDS("scratch/source_data/raw_sales_contents_subject_aat.rds")

k_class <- k_aat %>%
  single_separate("subject") %>%
  single_separate("depicts") %>%
  single_separate("classified_as") %>%
  gather(classno, classified_as, dplyr::contains("classified_as")) %>%
  select(subject = knoedler_subject, genre = knoedler_genre, k_classified_as = classified_as) %>%
  mutate_at(vars(genre, subject), tolower) %>%
  mutate_at(vars(k_classified_as), as.integer)


sc_class <- sc_aat %>%
  single_separate("subject") %>%
  single_separate("depicts") %>%
  single_separate("classified_as") %>%
  gather(classno, classified_as, dplyr::contains("classified_as")) %>%
  select(subject = sales_contents_subject, genre = sales_contents_genre, sc_classified_as = classified_as) %>%
  mutate_at(vars(genre, subject), tolower) %>%
  mutate_at(vars(sc_classified_as), as.integer)


checktable <- sc_class %>%
  full_join(k_class, by = c("genre","subject")) %>%
  filter(!is.na(sc_classified_as) & !is.na(k_classified_as))

sc_order <- sc_aat %>%
  mutate_at(vars(sales_contents_subject, sales_contents_genre), tolower) %>%
  group_by(sales_contents_subject, sales_contents_genre) %>%
  summarize(count = sum(count))

lc_sc_aat <- sc_aat %>%
  mutate_at(vars(sales_contents_subject, sales_contents_genre), tolower) %>%
  group_by(sales_contents_subject, sales_contents_genre) %>%
  summarize_at(vars(subject, style, classified_as, depicts, notes), funs(paste0(na.omit(unique(.)), collapse = ";"))) %>%
  left_join(sc_order) %>%
  arrange(desc(count)) %>%
  select(sales_contents_subject, sales_contents_genre, count, everything())

write_clip(lc_sc_aat)
