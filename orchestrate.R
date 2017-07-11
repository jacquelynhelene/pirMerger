devtools::document()
devtools::install()
library(pirMerger)
library(testthat)

# Read exported data files ----

working_dir <- "scratch"
unlink(working_dir, recursive = TRUE)
repo_data_dir <- paste(working_dir, "repo_data", sep = "/")
source_data_dir <- paste(working_dir, "source_data", sep = "/")
intermediate_data_dir <- paste(working_dir, "intermediate_data", sep = "/")
pipeline_data_dir <- paste(working_dir, "pipeline_data", sep = "/")
dir.create(working_dir)
dir.create(repo_data_dir)
dir.create(source_data_dir)
dir.create(intermediate_data_dir)
dir.create(pipeline_data_dir)

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

# Pull the current STAR exports and extract
pull_star_exports(out_dir = repo_data_dir, export_repo = "ssh://git@stash.getty.edu:7999/griis/getty-provenance-index.git")
expect_true(dir.exists(paste(repo_data_dir, "csv", sep = "/")))

# Process export files and Google Sheets files into dataframes
read_all_exports(out_dir = source_data_dir, data_dict = working_dict, repo_path = repo_data_dir)
read_all_concordances(out_dir = source_data_dir, data_dict = working_dict)
expect_equivalent(dir(source_data_dir, "*.rds"), required_rds)

# Process artist authority table

