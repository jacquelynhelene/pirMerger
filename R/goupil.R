#' Produce Goupil table from raw table.
#'
#' This is a two-stage process. First, the raw, mostly denormalized exports are
#' read in, and then normalized into a series of tables. Secondly, IDs for
#' artists, buyers, sellers, objects, and transaction & inventory events are
#' generated and attached to the reamining core `goupil` table.
#'
#' @param source_dir Path where source RDS files are found
#' @param target_dir Path where resulting RDS files are saved
#'
#' @importFrom rematch2 re_match bind_re_match
#'
#' @export
produce_goupil <- function(source_dir, target_dir) {
  raw_goupil <- get_data(source_dir, "raw_goupil")

  goupil <- raw_goupil %>%
    select(-star_record_no)

  # --- The following functions break out tables for multiply-ocurring fields,
  # writing them as normalized tables with keys to join back on to the core
  # `goupil` table ---
  message("- Extracting goupil _artists")
  goupil_artists <- norm_vars(goupil, base_names = c("artist_name", "art_authority", "nationality", "attribution_mod", "star_rec_no", "artist_ulan_id"), n_reps = 2, idcols = "star_record_no") %>%
    rename(artist_star_record_no = star_rec_no) %>%
    # Join ulan ids to this list
    rename(artist_authority = art_authority, artist_nationality = nationality, artist_attribution_mod = attribution_mod)
  goupil <- goupil %>%
    select(-(artist_name_1:artist_ulan_id_2))
  save_data(target_dir, knoedler_artists)

  save_data(target_dir, goupil)
}