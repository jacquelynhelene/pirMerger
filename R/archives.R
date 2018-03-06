produce_archival_descriptions <- function(raw_archival_descriptions) {
  raw_archival_descriptions %>%
    select(-original_file_name, -star_record_no) %>%
    rename(description_puri = persistent_uid)
}

produce_archives_sqlite <- function(dbpath,
                                    archival_descriptions) {

  adb <- db_setup(dbpath)

  write_tbl_key(adb, archival_descriptions, "archival_descriptions",
                p_key = "description_puri")

  db_cleanup(adb)
}
