# Knoedler Top Level ----

#' Produce Knoedler table from raw table.
#'
#' This is a two-stage process. First, the raw, mostly denormalized exports are
#' read in, and then normalized into a series of tables. Secondly, IDs for
#' artists, buyers, sellers, objects, and transaction & inventory events are
#' generated and attached to the reamining core `knoedler` table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_knoedler_ids <- function(raw_knoedler, knoedler_stocknumber_concordance) {
  knoedler <- raw_knoedler %>%
    # Convert numeric strings into integers
    mutate_at(vars(stock_book_no, page_number, row_number, dplyr::contains("entry_date"), dplyr::contains("sale_date")), funs(as.integer))

  # Create unique object ids, calculate order of events, and finally produce
  # unique transaction ids. Must happen in this order. Must be performed before
  # splitting out purchase, inventory, and sale events
  knoedler <- knoedler %>%
    pipe_message("- Identifying objects") %>%
    identify_knoedler_objects(knoedler_stocknumber_concordance) %>%
    pipe_message("- Ordering object events") %>%
    order_knoedler_object_events() %>%
    pipe_message("- Identifying transactions") %>%
    identify_knoedler_transactions() %>%
    # Where genre or object type is not identified, set to NA
    mutate_at(vars(genre, object_type), funs(na_if(., "[not identified]"))) %>%
    # Where month or day components of entry or sale dates are 0, set to NA
    mutate_at(vars(dplyr::contains("day"), dplyr::contains("month")), funs(na_if(., 0)))

  knoedler
}

# Remove columns now made redundant by other relational tables
produce_knoedler <- function(knoedler_tmp) {
  knoedler_tmp %>%
    select(-(artist_name_1:artist_ulan_id_2)) %>%
    select(-(seller_name_1:seller_ulan_id_2)) %>%
    select(-(joint_own_1:joint_ulan_id_4)) %>%
    select(-(buyer_name_1:buyer_ulan_id_2)) %>%
    select(-(purch_amount:knoedpurch_note)) %>%
    select(-(entry_date_year:entry_date_day)) %>%
    select(-(sale_date_year:knoedshare_note), -transaction) %>%
    select(-pres_own_ulan_id)
}

# Returns the ULAN ID for knoedler
knoedler_firm_id <- function() 500304270

# Call general dimension extraction function to parse dimension strings from
# Knoedler, and assign dimension types based on [width] x [height] order
# assumption that holds for Knoedler (but does not for more heterogeneous data
# like Sales Contents)
produce_knoedler_dimensions <- function(kdf, dimensions_aat, units_aat) {
  kdf %>%
    general_dimension_extraction("dimensions", "star_record_no") %>%
    mutate(
      # Assign dimension unit based on the extracted dimension character,
      # defaulting to inches (a valid default for Knoedler only)
      dimension_unit = case_when(
        # Default to feet
        dim_c1 == "cm" ~ "centimeters",
        TRUE ~ "feet"),
      # Assign dimension type based on extracted dimension character, falling
      # back when necessary to the order in which dimensions were listed
      dimension_type = case_when(
        is.na(dimtype) & dimension_order == 1 ~ "width",
        is.na(dimtype) & dimension_order == 2 ~ "height",
        is.na(dimtype) & dimension_order > 2 ~ NA_character_,
        dimtype == "h" ~ "height",
        dimtype == "w" ~ "width",
        dimtype == "d" ~ "diameter",
        dimtype == "l" ~ "length",
        TRUE ~ NA_character_
      )
    ) %>%
    # Join AAT ids for dimension types and distance units
    inner_join(dimensions_aat, by = c("dimension_type" = "dimension")) %>%
    inner_join(units_aat, by = c("dimension_unit" = "unit")) %>%
    # Return final table with dimension valu, unit, unit aat, dimension type,
    # and dimension aat, which can be joined to orginal records.
    select(star_record_no, dimension_value = decimalized_dim_value, dimension_unit,
           dimension_unit_aat = unit_aat, dimension_type,
           dimension_type_aat = dimension_aat)
}

# Purchase Events ----

# Pull the relevant information about Knoedler's intake of objects, and
# structure into tables describing the sellers, buyers (Knoedler and joint
# owners), payments transferred, and dates of the purchase events
produce_knoedler_purchase_info <- function(knoedler, currency_aat) {
  knoedler_purchase_info <- knoedler %>%
    filter(!is.na(purchase_event_id)) %>%
    group_by(purchase_event_id) %>%
    # Flag when any of the amounts or currencies are inconsistent across mutliple records of a transaction
    mutate_at(vars(purch_amount, purch_currency, knoedpurch_amt, knoedpurch_curr), funs(inconsistent = n_distinct(., na.rm = TRUE) > 1)) %>%
    summarize_at(
      vars(purch_amount,
           purch_currency,
           knoedpurch_amt,
           knoedpurch_curr,
           entry_date_year,
           entry_date_month,
           entry_date_day,
           contains("inconsistent")),
      funs(pick))

  # Parse purchase amount and join the results
  parsed_purchase_amounts <- parse_prices(currency_aat, knoedler_purchase_info, amount_col_name = "purch_amount", currency_col_name = "purch_currency", id_col_name = "purchase_event_id", decimalized_col_name = "decimalized_purch_amount", aat_col_name = "purch_currency_aat", amonsieurx = TRUE, replace_slashes = TRUE)

  # Parse Knoedler's share of purchase amount and join the results
  parsed_knoedpurch_amounts <- parse_prices(currency_aat, knoedler_purchase_info, amount_col_name = "knoedpurch_amt", currency_col_name = "knoedpurch_curr", id_col_name = "purchase_event_id", decimalized_col_name = "decimalized_knoedpurch_amount", aat_col_name = "knoedpurch_currency_aat", amonsieurx = TRUE, replace_slashes = TRUE)

  knoedler_purchase_info %>%
    left_join(parsed_purchase_amounts, by = "purchase_event_id") %>%
    left_join(parsed_knoedpurch_amounts, by = "purchase_event_id")
}

# Identify from whom custody is being transferred (includes no share information)
produce_knoedler_purchase_sellers <- function(knoedler, knoedler_sellers) {
  knoedler %>%
    filter(!is.na(purchase_event_id)) %>%
    select(star_record_no, purchase_event_id) %>%
    left_join(knoedler_sellers, by = "star_record_no") %>%
    select(-star_record_no) %>%
    distinct() %>%
    select(purchase_event_id, purchase_seller_name = seller_name, purchase_seller_loc = seller_loc, purchase_seller_auth_name = sell_auth_name, purchase_seller_auth_loc = sell_auth_loc, purchase_seller_ulan_id = seller_ulan_id, purchase_seller_uid = seller_uid)
}

# Identify to whom custody is being transferred (includes share information)
produce_knoedler_purchase_buyers <- function(knoedler, knoedler_joint_owners) {
  knoedler %>%
    filter(!is.na(purchase_event_id)) %>%
    select(star_record_no, purchase_event_id) %>%
    inner_join(knoedler_joint_owners, by = "star_record_no") %>%
    select(-star_record_no) %>%
    distinct() %>%
    mutate(parsed_share = parse_fraction(joint_own_sh)) %>%
    group_by(purchase_event_id) %>%
    mutate(
      remainder = 1 - sum(na.omit(parsed_share)),
      full_parsed_share = if_else(is.na(parsed_share), remainder, parsed_share)) %>%
    ungroup() %>%
    select(purchase_event_id, purchase_buyer_name = joint_own, purchase_buyer_share = full_parsed_share, purchase_buyer_ulan_id = joint_ulan_id, purchase_buyer_uid = joint_owner_uid)
}

# Pull the dates on which Knoedler inventoried an object
produce_knoedler_inventories <- function(knoedler) {
  knoedler %>%
    filter(!is.na(inventory_event_id)) %>%
    select(inventory_event_id, inventory_year = entry_date_year, inventory_month = entry_date_month, inventory_day = entry_date_day) %>%
    distinct()
}

# Pull the relevant information about Knoedler's outflow of objects, and
# structure into tables describing the sellers (Knoedler and joint owners),
# buyers, payments transferred, and dates of the purchase events
produce_knoedler_sales <- function(knoedler) {
  knoedler_sales <- knoedler %>%
    filter(!is.na(sale_event_id))
}

produce_knoedler_sale_info <- function(knoedler_sales, currency_aat) {
  knoedler_sale_info <- knoedler_sales %>%
    group_by(sale_event_id) %>%
    # Flag when any of the amounts or currencies are inconsistent across mutliple records of a transaction
    mutate_at(vars(transaction, knoedshare_amt, knoedshare_curr, price_amount, price_currency), funs(inconsistent = n_distinct(., na.rm = TRUE) > 1)) %>%
    summarize_at(
      vars(transaction,
           sale_date_year,
           sale_date_month,
           sale_date_day,
           knoedshare_amt,
           knoedshare_curr,
           knoedshare_note,
           price_amount,
           price_currency,
           contains("inconsistent")),
      funs(pick))

  # Parse sale amount and join the results
  parsed_sale_amounts <- parse_prices(currency_aat, knoedler_sale_info, amount_col_name = "price_amount", currency_col_name = "price_currency", id_col_name = "sale_event_id", decimalized_col_name = "decimalized_price_amount", aat_col_name = "price_currency_aat", amonsieurx = TRUE, replace_slashes = TRUE)

  # Parse Knoedler's share of purchase amount and join the results
  parsed_knoedshare_amounts <- parse_prices(currency_aat, knoedler_sale_info, amount_col_name = "knoedshare_amt", currency_col_name = "knoedshare_curr", id_col_name = "sale_event_id", decimalized_col_name = "decimalized_knoedshare_amount", aat_col_name = "knoedshare_currency_aat", amonsieurx = TRUE, replace_slashes = TRUE)

  knoedler_sale_info %>%
    left_join(parsed_sale_amounts, by = "sale_event_id") %>%
    left_join(parsed_knoedshare_amounts, by = "sale_event_id")
}

# Identify to whom custody is being transferred (includes no share information)
produce_knoedler_sale_buyers <- function(knoedler_sales, knoedler_buyers) {
  knoedler_sales %>%
    filter(!is.na(sale_event_id)) %>%
    select(star_record_no, sale_event_id) %>%
    left_join(knoedler_buyers, by = "star_record_no") %>%
    select(-star_record_no) %>%
    distinct() %>%
    select(sale_event_id, sale_buyer_name = buyer_name, sale_buyer_loc = buyer_loc, sale_buyer_auth_name = buy_auth_name, sale_buyer_auth_loc = buy_auth_addr, sale_buyer_ulan_id = buyer_ulan_id, sale_buyer_uid = buyer_uid)
}

# Identify from whom custody is being transferred (includes share information)
produce_knoedler_sale_sellers <- function(knoedler_sales, knoedler_joint_owners) {
  knoedler_sales %>%
    filter(!is.na(sale_event_id)) %>%
    select(star_record_no, sale_event_id) %>%
    inner_join(knoedler_joint_owners, by = "star_record_no") %>%
    select(-star_record_no) %>%
    distinct() %>%
    mutate(parsed_share = parse_fraction(joint_own_sh)) %>%
    group_by(sale_event_id) %>%
    mutate(
      remainder = 1 - sum(na.omit(parsed_share)),
      full_parsed_share = if_else(is.na(parsed_share), remainder, parsed_share)) %>%
    ungroup() %>%
    select(sale_event_id, sale_seller_name = joint_own, sale_seller_share = full_parsed_share, sale_seller_ulan_id = joint_ulan_id, sale_seller_uid = joint_owner_uid)
}

identify_knoedler_transactions <- function(df) {
  df %>%
    # Detect the number of artworks between which a given purchase/sale amt. was
    # split, first by standardizing all purchase/price notes, then finding matches.
    mutate_at(
      vars(purch_note, knoedpurch_note, price_note),
      funs(working =
        str_replace_all(., c(
          "(?:shared[,;])|(?:[;,] shared)|(?:shared)" = "",
          " ([&-]) " = "\\1",
          "&" = "-")) %>%
          str_trim() %>%
          na_if(""))) %>%
    # Prefer the purch note over the knoedpurch note, only falling back to
    # knoedpurch when reuglar note is NA
    mutate(purch_note_working = if_else(is.na(purch_note_working), knoedpurch_note_working, purch_note_working)) %>%
    # Only keep those notes that refer to prices paid "for" n objects
    mutate_at(vars(purch_note_working, price_note_working),
              funs(if_else(str_detect(., regex("for", ignore_case = TRUE)), ., NA_character_))) %>%
    # Fill in null purchase notes with dummy ids
    mutate_at(vars(purch_note_working, price_note_working), funs(if_else(is.na(.), as.character(seq_along(.)), .))) %>%
    # Produce a unique id for unique pairings of purchase notes
    mutate(
      entry_event_id = paste("k", "entry", group_indices(., stock_book_no, purch_note_working), sep = "-"),
      sale_event_id = paste("k", "sale", group_indices(., stock_book_no, price_note_working), sep = "-"),
      # No sale_event_id in the case that the record is not showing any sold
      # info, and has no transaction date info or price info.
      sale_event_id = ifelse(transaction == "Unsold" & is.na(sale_date_year) & is.na(price_amount), NA_character_, sale_event_id)) %>%
    # Calculate the order of events
    order_knoedler_object_events() %>%
    # If the entry event was the first of that object's lifetime, we assume it
    # was a purchase. If not the first entry, then we assume it constitutes an
    # inventory-taking event
    group_by(object_id) %>%
    mutate(purchase_event_id = if_else(event_order == 1, str_replace(entry_event_id, "entry", "purchase"), NA_character_)) %>%
    ungroup() %>%
    # If this is an inventory event, then label it based on an entirely new
    # incrementing counter, as inventories are not to be connected by purchase
    # notes in the same way that purchase_event_id is,
    mutate(
      inventory_event_id = if_else(is.na(purchase_event_id), paste("k-inventory", seq_along(purchase_event_id), sep = "-"), NA_character_)) %>%
    select(-purch_note_working, -knoedpurch_note_working, -price_note_working, -entry_event_id) %>%
    ungroup()
}

#' Editors have compiled links between stocknumbers that actually represent the
#' same object. We compose a graph of these relationships, identify connected
#' components that represnent all the stock numbers related to a single object,
#' and then produce a lookup table pairing each stock number with a UID for its
#' group
#' @import igraph
produce_knoedler_stocknumber_concordance <- function(raw_knoedler_stocknumber_concordance) {
  # Produce a 'long' table from the 'wide' version entered by editors
  knoedler_stocknumber_concordance <- raw_knoedler_stocknumber_concordance %>%
    select(contains("sn")) %>%
    mutate(prime_stock_number = sn1) %>%
    gather(number_index, stock_number, contains("sn"), na.rm = TRUE) %>%
    select(source = prime_stock_number, target = stock_number) %>%
    na.omit()

  # Create a graph from this edgelist
  sn_graph <- igraph::graph_from_data_frame(knoedler_stocknumber_concordance, directed = FALSE) %>%
    igraph::simplify()

  # Identify components and produce a lookup table
  sn_components <- igraph::components(sn_graph)
  igraph::V(sn_graph)$component <- sn_components$membership
  knoedler_stocknumber_concordance <- igraph::as_data_frame(sn_graph, what = "vertices") %>%
    rename(knoedler_number = name)

  knoedler_stocknumber_concordance
}

# Produce unique ids for knoedler objects based on their stock numbers
identify_knoedler_objects <- function(df, knoedler_stocknumber_concordance) {
  df %>%
    left_join(knoedler_stocknumber_concordance, by = c("knoedler_number")) %>%
    # Because some of the knoedler stock numbers changed or were re-used, we
    # will consult against a stock number concordance that we can use to create
    # a "functional" stock number - an identifier that connects objects even
    # when their nominal stock numbers are different. Those entries without any
    # stock nubmers at all are assumed to be standalone objects, and given a
    # unique id.
    mutate(
      prepped_sn = case_when(
        # When there is no number, generate a unique ID
        is.na(knoedler_number) ~ paste("gennum", as.character(seq_along(star_record_no)), sep = "-"),
        # When there is a number that has a prime # replacement from the
        # concordance, use that prime #
        !is.na(component) ~ paste("componentnum", component, sep = "-"),
        # When the original number has no recorded changes, group based on that
        # original number
        TRUE ~ paste("orignnum", knoedler_number, sep = "-"))) %>%
    mutate(object_id = paste("k", "object", group_indices(., prepped_sn), sep = "-")) %>%
    select(-component, -prepped_sn)
}

# For a given object_id, attempt to discern an event order, which can be useful
# for discerning timespand boundaries as well as figuring out when an object
# first entered, and then finally left, knoedler's collection.
#
# This is called from within identify_knoedler_transactions because it is a
# prerequisite to discerning which entries represent purchases by Knoedler vs.
# inventory events by Knoedler
order_knoedler_object_events <- function(df) {
  df %>%
    # Use the entry date as the primary index of event date, falling back to the sale date if the entry date is not available.
    mutate(
      event_year = case_when(
        is.na(entry_date_year) ~ sale_date_year,
        TRUE ~ entry_date_year),
      event_month = case_when(
        is.na(entry_date_month) ~ sale_date_month,
        TRUE ~ entry_date_month),
      event_day = case_when(
        is.na(entry_date_day) ~ sale_date_day,
        TRUE ~ entry_date_day)) %>%
    # Produce an index per object_id based on this event year/month/day, falling
    # back to position in stock book if there is no year.
    group_by(object_id) %>%
    arrange(event_year, event_month, event_day, stock_book_no, page_number, row_number, .by_group = TRUE) %>%
    mutate(event_order = seq_along(star_record_no)) %>%
    ungroup() %>%
    # Remove intermediate columns
    select(-event_year, -event_month, -event_day)
}

# Harmonize monetary amounts and currencies
parse_knoedler_monetary_amounts <- function(df) {
  df %>%
    # Pull out numbers (including decimals) from price amount columns. This strips
    # off editorial brackets, as well as any trailing numbers such as shillings
    # and pence. Also applies the amonsieurx transformation.
    mutate(deciphered_purch = amonsieurx(purch_amount),
           purch_amount = ifelse(deciphered_purch == "", purch_amount, deciphered_purch)) %>%
    select(-deciphered_purch) %>%
    mutate_at(vars(purch_amount, knoedpurch_amt, price_amount), funs(as.numeric(str_match(., "(\\d+\\.?\\d*)")[,2])))
}

# AAT Tables ----


produce_knoedler_materials_object_aat <- function(raw_knoedler_materials_aat, kdf) {
  raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_materials = made_of_materials) %>%
    single_separate(source_col = "aat_materials") %>%
    gather(gcol, aat_materials, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_materials), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_materials_support_aat <- function(raw_knoedler_materials_aat, kdf) {
  raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_support = made_of_support) %>%
    single_separate(source_col = "aat_support") %>%
    gather(gcol, aat_support, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_support), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_materials_classified_as_aat <- function(raw_knoedler_materials_aat, kdf) {
  raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, classified_as_1, classified_as_2) %>%
    single_separate(source_col = "classified_as_1") %>%
    single_separate(source_col = "classified_as_2") %>%
    gather(gcol, aat_classified_as, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_classified_as), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_materials_technique_aat <- function(raw_knoedler_materials_aat, kdf) {
  raw_knoedler_materials_aat %>%
    select(materials = knoedler_materials, object_type = knoedler_object_type, aat_technique = technique) %>%
    single_separate(source_col = "aat_technique") %>%
    gather(gcol, aat_technique, -materials, -object_type, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_technique), as.integer) %>%
    left_join(select(kdf, star_record_no, object_type, materials), by = c("object_type", "materials")) %>%
    select(-object_type, -materials) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_subject_aat <- function(raw_knoedler_subjects_aat, kdf) {
  raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, aat_subject = subject) %>%
    single_separate(source_col = "aat_subject") %>%
    gather(gcol, aat_subject, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_subject), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_style_aat <- function(raw_knoedler_subjects_aat, kdf) {
  raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, aat_style = style) %>%
    single_separate(source_col = "aat_style") %>%
    gather(gcol, aat_style, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(aat_style), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_subject_classified_as_aat <- function(raw_knoedler_subjects_aat, kdf) {
  raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, subject_classified_as = classified_as) %>%
    single_separate(source_col = "subject_classified_as") %>%
    gather(gcol, subject_classified_as, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(subject_classified_as), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_depicts_aat <- function(raw_knoedler_subjects_aat, kdf) {
  raw_knoedler_subjects_aat %>%
    select(subject = knoedler_subject, genre = knoedler_genre, depicts_aat = depicts) %>%
    single_separate(source_col = "depicts_aat") %>%
    gather(gcol, depicts_aat, -subject, -genre, na.rm = TRUE) %>%
    select(-gcol) %>%
    mutate_at(vars(depicts_aat), as.integer) %>%
    left_join(select(kdf, star_record_no, subject, genre), by = c("subject", "genre")) %>%
    select(-subject, -genre) %>%
    filter(!is.na(star_record_no))
}

produce_knoedler_present_owner_ulan <- function(raw_knoedler_present_owner_ulan) {
  kdf %>%
    select(-present_own_ulan_id) %>%
    left_join(raw_knoedler_present_owner_ulan, by = "star_record_no")
}

produce_knoedler_owners_lookup <- function(knoedler_owner_uids) {
  knoedler_owner_uids %>%
    group_by(person_uid) %>%
    summarize(owner_label = if_else(all(is.na(owner_auth)), pick(owner_name), pick(owner_auth)))
}

# People ----

# Knoedler-specific process for flagging records with a UID process - should a
# UID be generated based on a ULAN id? The auth name? The verbatim name? Or
# uniquely generated (for all anonymous people)?
identify_knoedler_id_process <- function(person_df) {
  mutate(person_df,
         is_bracketed = str_detect(person_auth, "^\\["),
         is_anon_collex = person_auth %in% c("Anonymous Collection"),
         is_new = person_auth == "NEW" | is.na(person_auth),
         is_anon =  is_anon_collex | is_bracketed,
         is_ulan = !is.na(person_ulan) & !is_anon,
         is_known = !is.na(person_auth) & !is_anon & !is_new,
         id_process = case_when(
           is_ulan ~ "from_ulan",
           is_new ~ "from_name",
           is_anon ~ "from_nothing",
           is_known ~ "from_auth")) %>%
    select(source_record_id, source_document_id, person_name, person_auth, person_ulan, id_process) %>%
    assertr::assert(assertr::not_na, id_process)
}

produce_knoedler_artists_tmp <- function(raw_knoedler) {
  raw_knoedler %>%
    norm_vars(base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "star_rec_no", "artist_ulan_id"), n_reps = 2, idcols = "star_record_no") %>%
    rename(artist_star_record_no = star_rec_no) %>%
    # Join ulan ids to this list
    rename(artist_authority = art_authority, artist_nationality = nationality, artist_attribution_mod = attribution_mod)
}

produce_knoedler_artists_lookup <- function(knoedler_artists_tmp) {
  select(knoedler_artists_tmp,
         source_record_id = star_record_no,
         person_name = artist_name,
         person_auth = artist_authority,
         person_ulan = artist_ulan_id) %>%
    add_column(source_document_id = "KNOEDLER") %>%
    identify_knoedler_id_process()
}

produce_knoedler_artists <- function(knoedler_artists_tmp, union_person_ids) {
  upi_subset <- union_person_ids %>%
    filter(source_db == "knoedler_artists") %>%
    select(-source_db, -source_document_id) %>%
    rename(artist_uid = person_uid)

  left_join(knoedler_artists_tmp,
            upi_subset,
            by = c("star_record_no" = "source_record_id",
                   "artist_name" = "person_name",
                   "artist_authority" = "person_auth",
                   "artist_ulan_id" = "person_ulan"))
}

produce_knoedler_sellers_tmp <- function(raw_knoedler) {
  raw_knoedler %>%
    norm_vars(base_names = c("seller_name", "seller_loc", "sell_auth_name", "sell_auth_loc", "seller_ulan_id"), n_reps = 2, idcols = "star_record_no")
}

produce_knoedler_sellers_lookup <- function(knoedler_sellers_tmp) {
  select(knoedler_sellers_tmp,
         source_record_id = star_record_no,
         person_name = seller_name,
         person_auth = sell_auth_name,
         person_ulan = seller_ulan_id) %>%
    add_column(source_document_id = "KNOEDLER") %>%
    identify_knoedler_id_process()
}

produce_knoedler_sellers <- function(knoedler_sellers_tmp, union_person_ids) {
  seller_ids <- union_person_ids %>%
    filter(source_db == "knoedler_sellers") %>%
    select(-source_db, -source_document_id) %>%
    rename(seller_uid = person_uid)

  left_join(knoedler_sellers_tmp,
            seller_ids,
            by = c(
               "star_record_no" = "source_record_id",
              "seller_name" = "person_name",
              "sell_auth_name" = "person_auth",
              "seller_ulan_id" = "person_ulan"
            ))
}

produce_knoedler_buyers_tmp <- function(raw_knoedler) {
  raw_knoedler %>%
    # As Knoedler is technically one of the joint owners, they need to be
    # present in every sale
    norm_vars(base_names = c("buyer_name", "buyer_loc", "buy_auth_name", "buy_auth_addr", "buyer_ulan_id"), n_reps = 2, idcols = "star_record_no")
}

produce_knoedler_buyers_lookup <- function(knoedler_buyers_tmp) {
  select(knoedler_buyers_tmp,
         source_record_id = star_record_no,
         person_name = buyer_name,
         person_auth = buy_auth_name,
         person_ulan = buyer_ulan_id) %>%
    add_column(source_document_id = "KNOEDLER") %>%
    identify_knoedler_id_process()
}

produce_knoedler_buyers <- function(knoedler_buyers_tmp, union_person_ids) {
  buyer_ids <- union_person_ids %>%
    filter(source_db == "knoedler_buyers") %>%
    select(-source_db, -source_document_id) %>%
    rename(buyer_uid = person_uid)

  left_join(knoedler_buyers_tmp,
            buyer_ids,
            by = c(
              "star_record_no" = "source_record_id",
              "buyer_name" = "person_name",
              "buy_auth_name" = "person_auth",
              "buyer_ulan_id" = "person_ulan"
            ))
}

produce_knoedler_joint_owners_tmp <- function(raw_knoedler) {
  raw_knoedler %>%
  # As Knoedler is technically one of the joint owners, they need to be
  # present in every sale
  mutate(
    joint_own_5 = "Knoedler",
    joint_own_sh_5 = NA_character_,
    joint_ulan_id_5 = knoedler_firm_id()) %>%
    norm_vars(base_names = c("joint_own", "joint_own_sh", "joint_ulan_id"), n_reps = 5, idcols = "star_record_no")
}

produce_knoedler_joint_owners_lookup <- function(knoedler_joint_owners_tmp) {
  select(knoedler_joint_owners_tmp,
         source_record_id = star_record_no,
         person_auth = joint_own,
         person_ulan = joint_ulan_id) %>%
    add_column(
      person_name = NA_character_,
      source_document_id = "KNOEDLER") %>%
    identify_knoedler_id_process()
}

produce_knoedler_joint_owners <- function(knoedler_joint_owners_tmp, union_person_ids) {
  joint_owner_ids <- union_person_ids %>%
    filter(source_db == "knoedler_joint_owners") %>%
    select(-source_db, -source_document_id, -person_name) %>%
    rename(joint_owner_uid = person_uid)

  left_join(knoedler_joint_owners_tmp,
            joint_owner_ids,
            by = c(
              "star_record_no" = "source_record_id",
              "joint_own" = "person_auth",
              "joint_ulan_id" = "person_ulan"
            ))
}

# Joined Table ----

#' Produce a joined Knoedler table
#'
#' @param source_dir Where to load preprocessed Knoedler files.
#' @param target_dir Where to save the fully joined Knoedler table.
#'
#' @return A data frame.
#'
#' @export
produce_joined_knoedler <- function(knoedler,
                                    knoedler_artists,
                                    knoedler_buyers,
                                    knoedler_sellers,
                                    knoedler_joint_owners,
                                    knoedler_purchase_info,
                                    knoedler_purchase_buyers,
                                    knoedler_purchase_sellers,
                                    knoedler_inventory_events,
                                    knoedler_sale_info,
                                    knoedler_sale_buyers,
                                    knoedler_sale_sellers,
                                    knoedler_materials_classified_as_aat,
                                    knoedler_materials_object_aat,
                                    knoedler_materials_support_aat,
                                    knoedler_materials_technique_aat,
                                    knoedler_subject_aat,
                                    knoedler_style_aat,
                                    knoedler_subject_classified_as_aat,
                                    knoedler_depicts_aat,
                                    currency_aat,
                                    knoedler_dimensions,
                                    knoedler_present_owner_ulan) {

  knoedler_name_order <- c(
    "star_record_no",
    "pi_record_no",
    "stock_book_no",
    "knoedler_number",
    "page_number",
    "row_number",
    "consign_no",
    "consign_name",
    "consign_loc",
    "title",
    "description",
    "artist_name_1",
    "artist_authority_1",
    "artist_nationality_1",
    "artist_attribution_mod_1",
    "artist_ulan_id_1",
    "artist_name_2",
    "artist_authority_2",
    "artist_nationality_2",
    "artist_attribution_mod_2",
    "artist_ulan_id_2",
    "subject",
    "genre",
    "depicts_aat_1",
    "subject_classified_as_1",
    "subject_classified_as_2",
    "subject_classified_as_3",
    "aat_style_1",
    "aat_subject",
    "object_type",
    "materials",
    "aat_classified_as_1",
    "aat_classified_as_2",
    "aat_classified_as_3",
    "aat_support_1",
    "aat_support_2",
    "aat_materials_1",
    "aat_materials_2",
    "aat_materials_3",
    "aat_technique_1",
    "dimensions",
    "entry_date_year",
    "entry_date_month",
    "entry_date_day",
    "sale_date_year",
    "sale_date_month",
    "sale_date_day",
    "purch_amount",
    "purch_currency",
    "purch_currency_aat",
    "purch_note",
    "knoedpurch_amt",
    "knoedpurch_curr",
    "knoedpurch_curr_aat",
    "knoedpurch_note",
    "price_amount",
    "price_currency",
    "price_currency_aat",
    "price_note",
    "knoedshare_amt",
    "knoedshare_curr",
    "knoedpurch_curr_aat",
    "knoedshare_note",
    "transaction",
    "folio",
    "verbatim_notes",
    "main_heading",
    "subheading",
    "object_id",
    "inventory_or_purchase_id",
    "sale_transaction_id",
    "event_order",
    "joint_own_1",
    "joint_own_sh_1",
    "joint_own_2",
    "joint_own_sh_2",
    "joint_own_3",
    "joint_own_sh_3",
    "joint_own_4",
    "joint_own_sh_4",
    "buyer_name_1",
    "buyer_loc_1",
    "buy_auth_name_1",
    "buy_auth_addr_1",
    "buyer_ulan_id_1",
    "buyer_name_2",
    "buyer_loc_2",
    "buy_auth_name_2",
    "buy_auth_addr_2",
    "buyer_ulan_id_2",
    "seller_name_1",
    "seller_loc_1",
    "sell_auth_name_1",
    "sell_auth_loc_1",
    "seller_ulan_id_1",
    "seller_name_2",
    "seller_loc_2",
    "sell_auth_name_2",
    "sell_auth_loc_2",
    "seller_ulan_id_2"
  )

  message("- Merge knoedler purchase data into single table")
  knoedler_purchases <- knoedler_purchase_info %>%
    left_join(spread_out(knoedler_purchase_sellers, "purchase_event_id"), by = "purchase_event_id") %>%
    left_join(spread_out(knoedler_purchase_buyers, "purchase_event_id"), by = "purchase_event_id")

  message("- Merge knoedler sales data into single table")
  knoedler_sales <- knoedler_sale_info %>%
    left_join(spread_out(knoedler_sale_sellers, "sale_event_id"), by = "sale_event_id") %>%
    left_join(spread_out(knoedler_sale_buyers, "sale_event_id"), by = "sale_event_id")

    joined_knoedler <- knoedler %>%
    pipe_message("- Join spread knoedler_artists to knoedler") %>%
    left_join(spread_out(knoedler_artists, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join knoedler_purchases to knoedler") %>%
    left_join(knoedler_purchases, by = "purchase_event_id") %>%
    pipe_message("- Join knoedler_inventory_events to knoedler") %>%
    left_join(knoedler_inventory_events, by = "inventory_event_id") %>%
    pipe_message("- Joint knoedler_sales to knoedler") %>%
    left_join(knoedler_sales, by = "sale_event_id") %>%
    pipe_message("- Join knoedler_dimensions to knoedler") %>%
    left_join(spread_out(knoedler_dimensions, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_classified_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_classified_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_support_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_support_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_object_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_object_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_materials_technique_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_materials_technique_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_depicts_aat to knoedler") %>%
    left_join(spread_out(knoedler_depicts_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_subject_classified_as_aat to knoedler") %>%
    left_join(spread_out(knoedler_subject_classified_as_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_style_aat to knoedler") %>%
    left_join(spread_out(knoedler_style_aat, "star_record_no"), by = "star_record_no") %>%
    pipe_message("- Join spread knoedler_subject_aat to knoedler") %>%
    left_join(spread_out(knoedler_subject_aat, "star_record_no"), by = "star_record_no")
  joined_knoedler %>%
    pipe_message("- Join knoedler_present_owner_ulan to knoedler") %>%
    left_join(knoedler_present_owner_ulan, by = "star_record_no")
}
