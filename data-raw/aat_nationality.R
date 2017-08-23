# Produce a table of "anonymous" nationalities to be reconciled to AAT

library(pirMerger)
library(tidyverse)
library(stringr)
library(forcats)

at <- get_data("scratch/intermediate_data", "artists_authority")
ow <- get_data("scratch/intermediate_data", "owners_authority")
sc <- get_data("scratch/source_data", "raw_sales_contents")
kk <- get_data("scratch/source_data", "raw_knoedler")

art_nat <- at %>%
  pull(nationality) %>%
  na.omit()

own_nat <- ow %>%
  pull(nationality) %>%
  na.omit()

nat_id_pattern <- "^\\[[^\\.]"

sc_buy_nat <- sc %>%
  norm_vars(base_names = "buy_auth_name", n_reps = 5, idcols = "star_record_no") %>%
  filter(str_detect(buy_auth_name, nat_id_pattern)) %>%
  pull(buy_auth_name)

sc_art_nat <- sc %>%
  norm_vars(base_names = "art_authority", n_reps = 5, idcols = "star_record_no") %>%
  filter(str_detect(art_authority, nat_id_pattern)) %>%
  pull(art_authority)

sc_nat <- sc %>%
  norm_vars(base_names = "nationality", n_reps = 5, idcols = "star_record_no") %>%
  pull(nationality)

sc_sell_nat <- sc %>%
  norm_vars(base_names = "sell_auth_name", n_reps = 5, idcols = "star_record_no") %>%
  filter(str_detect(sell_auth_name, nat_id_pattern)) %>%
  pull(sell_auth_name)

k_art_nat <- kk %>%
  norm_vars(base_names = "art_authority", n_reps = 2, idcols = "star_record_no") %>%
  filter(str_detect(art_authority, nat_id_pattern)) %>%
  pull(art_authority)

k_nat <- kk %>%
  norm_vars(base_names, "nationality", n_reps = 2, idcols = "star_record_no") %>%
  pull(nationality)

k_buy_nat <- kk %>%
  norm_vars(base_names = "buy_auth_name", n_reps = 2, idcols = "star_record_no") %>%
  filter(str_detect(buy_auth_name, nat_id_pattern)) %>%
  pull(buy_auth_name)

k_sell_nat <- kk %>%
  norm_vars(base_names = "sell_auth_name", n_reps = 2, idcols = "star_record_no") %>%
  filter(str_detect(sell_auth_name, nat_id_pattern)) %>%
  pull(sell_auth_name)

all_nationalities <- c(sc_nat, art_nat, own_nat, sc_buy_nat, sc_art_nat, sc_sell_nat, k_art_nat, k_buy_nat, k_sell_nat)

aat_nationalities <- fct_count(all_nationalities, sort = TRUE)

# Reimport existing work onto new table

current_nats <- read_csv("~/Downloads/Knoedler & Sales Catalogs nationality - AAT - Sheet1.csv", col_types = "cicciic") %>%
  select(-n)

reupload_nats <- aat_nationalities %>%
  left_join(current_nats, by = "f")

missing_nats <- current_nats %>%
  anti_join(aat_nationalities, by = "f")

