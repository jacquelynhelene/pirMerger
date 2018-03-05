#' Collect all AAT ids used across GPI datasets and produce list of unique terms
#'
#' @export
produce_union_aat <- function(source_dir) {

  knoedler_materials_technique_as_aat <- get_data(source_dir, "knoedler_materials_technique_as_aat")
  knoedler_style_aat <- get_data(source_dir, "knoedler_style_aat")
  knoedler_depicts_aat <- get_data(source_dir, "knoedler_depicts_aat")
  knoedler_subject_aat <- get_data(source_dir, "knoedler_subject_aat")
  knoedler_materials_object_aat <- get_data(source_dir, "knoedler_materials_object_aat")
  knoedler_materials_support_aat <- get_data(source_dir, "knoedler_materials_support_aat")
  knoedler_materials_classified_as_aat <- get_data(source_dir, "knoedler_materials_classified_as_aat")
  knoedler_subject_classified_as_aat <- get_data(source_dir, "knoedler_subject_classified_as_aat")

  all_knoedler_aat_ids <- c(
    pull(knoedler_materials_technique_as_aat, aat_technique),
    pull(knoedler_style_aat, aat_style),
    pull(knoedler_depicts_aat, depicts_aat),
    pull(knoedler_subject_aat, aat_subject),
    pull(knoedler_materials_object_aat, aat_materials),
    pull(knoedler_materials_support_aat, aat_support),
    pull(knoedler_materials_classified_as_aat, aat_classified_as),
    pull(knoedler_subject_classified_as_aat, subject_classified_as))

  unique_knoedler_aat_ids <- unique(all_knoedler_aat_ids)

  return(unique_knoedler_aat_ids)
}

# SQL Export helpers ----

handle_column <- function(name, type, is_p, is_u, is_nn) {
  p <- if_else(is_p, " PRIMARY KEY", "")
  u <- if_else(is_u, " UNIQUE", "")
  nn <- if_else(is_nn, " NOT NULL", "")
  str_interp("${name} ${type}${p}${u}${nn}")
}

format_pf_key <- function(db, df, tbl_name, p_key, u_keys, nn_keys, f_keys) {

  cnames <-  names(df)
  ctypes <- map_chr(df, dbDataType, db = db)

  all_fields <- map2_chr(cnames, ctypes, function(name, type) {
    handle_column(name, type,
                  is_p = name %in% p_key,
                  is_u = name %in% u_keys,
                  is_nn = name %in% nn_keys)
  }) %>%
    paste0("\t", ., collapse = ",\n")

  foreign_key <- ""
  if (!is.null(f_keys)) {
    foreign_key <- map_chr(f_keys, function(x) {
      f_key <- x$f_key
      parent_tbl_name <- x$parent_tbl_name
      parent_f_key <- x$parent_f_key
      str_interp("\tFOREIGN KEY (${f_key}) REFERENCES ${parent_tbl_name}(${parent_f_key})")
    }) %>%
      paste0(collapse = ",\n") %>%
      paste0(",\n", .)
  }

  str_interp("CREATE TABLE ${tbl_name}\n(\n${all_fields}${foreign_key}\n)")
}

# Builds indexes on foreign keys
build_indexes <- function(tbl_name, f_keys) {
  index_calls <- f_keys %>%
    map("f_key") %>%
    map_chr(function(key) str_interp("CREATE INDEX index_${tbl_name}_${key} on ${tbl_name}(${key})"))
  walk(index_calls, message)
  index_calls
}

write_tbl_key <- function(db, df, tbl_name, p_key = NULL, u_keys = NULL, nn_keys = NULL, f_keys = NULL) {
  command <- format_pf_key(db, df, tbl_name, p_key, u_keys, nn_keys, f_keys)
  message(command)
  dbExecute(db, command)
  build_indexes(tbl_name, f_keys) %>%
    walk(~ dbExecute(db, .x))
  dbWriteTable(db, tbl_name, df, append = TRUE, overwrite = FALSE)
}

# Use schemacrawler (https://www.schemacrawler.com/diagramming.html) to generate
# a PDF displaying the sqlite schema
produce_db_schema <- function(dbpath, outpath) {
  system2("schemacrawler.sh",
          args = c(
            "-server sqlite",
            str_interp("-database ${dbpath}"),
            "-command schema",
            "-outputformat pdf",
            str_interp("-outputfile ${outpath}"),
            "-password",
            "-infolevel maximum"))
}

db_setup <- function(dbpath, foreign_key_check = TRUE) {
  unlink(dbpath)
  db <- dbConnect(RSQLite::SQLite(), dbpath)

  if (foreign_key_check) {
    # Enforce foreign key constraints
    dbExecute(db, "PRAGMA foreign_keys = ON")
    stopifnot(dbGetQuery(db, "PRAGMA foreign_keys")[["foreign_keys"]][1] == 1)
  }

  return(db)
}

# Cleanup the db before finishing
db_cleanup <- function(db) {
  message("Vacuuming database...")
  dbExecute(db, "VACUUM")
  dbDisconnect(db)
}
