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
