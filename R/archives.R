produce_archival_descriptions_ids <- function(raw_archival_descriptions) {
  raw_archival_descriptions %>%
    select(-original_file_name, -star_record_no) %>%
    rename(description_puri = persistent_uid)
}

produce_archival_descriptions <- function(archival_descriptions_ids) {
  archival_descriptions_ids %>%
    select(-(owner_name_1:owner_name_occu_5)) %>%
    select(-(benef_name_1:benef_dates_12)) %>%
    select(-(rel_doc_type_1:rel_doc_comm_4))
}

produce_archival_descriptions_owner_names <- function(archival_descriptions_ids) {
  norm_vars(archival_descriptions_ids, base_names = c("owner_name", "owner_name_mod", "owner_name_life", "owner_name_occu"), n_reps = 5, idcols = "description_puri")
}

produce_archival_descriptions_benefactors <- function(archival_descriptions_ids) {
  norm_vars(archival_descriptions_ids, base_names = c("benef_name", "benef_dates"), n_reps = 12, idcols = "description_puri")
}

produce_archival_descriptions_rel_docs <- function(archival_descriptions_ids) {
  norm_vars(archival_descriptions_ids, base_names = c("rel_doc_type", "rel_doc_date", "rel_doc_page", "rel_doc_comm"), n_reps = 4, idcols = "description_puri")
}


produce_archives_sqlite <- function(dbpath,
                                    archival_descriptions,
                                    archival_descriptions_owner_names,
                                    archival_descriptions_benefactors,
                                    archival_descriptions_rel_docs) {

  adb <- db_setup(dbpath)

  desc_key_single <- list(
    f_key = "description_puri",
    parent_f_key = "description_puri",
    parent_tbl_name = "archival_descriptions"
  )
  desc_key = list(desc_key_single)

  write_tbl_key(adb, archival_descriptions, "archival_descriptions",
                p_key = "description_puri")

  write_tbl_key(adb, archival_descriptions_owner_names, "archival_descriptions_owner_names",
                nn_keys = "description_puri",
                f_keys = desc_key)

  write_tbl_key(adb, archival_descriptions_benefactors, "archival_descriptions_benefactors",
                nn_keys = "description_puri",
                f_keys = desc_key)

  write_tbl_key(adb, archival_descriptions_rel_docs, "archival_descriptions_rel_docs",
                nn_keys = "description_puri",
                f_keys = desc_key)

  db_cleanup(adb)
}
