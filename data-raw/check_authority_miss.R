aa <- get_data(intermediate_data_dir, "artists_authority")
oa <- get_data(intermediate_data_dir, "owners_authority")
k <- get_data(intermediate_data_dir, "knoedler_artists")
ks <- get_data(intermediate_data_dir, "knoedler_sellers")
kb <- get_data(intermediate_data_dir, "knoedler_buyers")

k_artist_miss <- k %>%
  filter(art_authority != "NEW") %>%
  anti_join(aa, by = c("art_authority" = "artist_authority"))

k_seller_miss <- ks %>%
  filter(sell_auth_name != "NEW") %>%
  anti_join(oa, by = c("sell_auth_name" = "owner_authority"))

k_buyer_miss <- kb %>%
  filter(buy_auth_name != "NEW") %>%
  anti_join(oa, by = c("buy_auth_name" = "owner_authority"))
