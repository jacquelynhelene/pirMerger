devtools::document()
devtools::install()
library(pirMerger)
library(testthat)

# Read exported data files ----

working_dir <- paste(tempdir(), "pirdata", sep = "/")
dir.create(working_dir)
working_dict <- "data_definitions.yml"

required_rds <- c(
  "raw_artists_authority.rds",
  "raw_exchange_rates.rds",
  "raw_gpi_dimensions_aat.rds",
  "raw_gpi_units_aat.rds",
  "raw_knoedler_materials_aat.rds",
  "raw_knoedler_stocknumber_concordance.rds",
  "raw_knoedler_subject_aat.rds",
  "raw_knoedler.rds",
  "raw_owners_authority.rds",
  "raw_sales_catalogs_info.rds",
  "raw_sales_catalogs_loccodes.rds",
  "raw_sales_contents_format_aat.rds",
  "raw_sales_contents_materials_aat.rds",
  "raw_sales_contents_subject_aat.rds",
  "raw_sales_contents.rds",
  "raw_sales_descriptions.rds",
  "raw_us_cpi.rds"
)

repo_path <- pull_star_exports(out_dir = working_dir, export_repo = "ssh://git@stash.getty.edu:7999/griis/getty-provenance-index.git")
read_all_exports(out_dir = working_dir, data_dict = working_dict, repo_path = repo_path)
read_all_concordances(out_dir = working_dir, data_dict = working_dict)
dir(working_dir)
testthat::expect_equivalent(dir(working_dir, "*.rds"), required_rds)
