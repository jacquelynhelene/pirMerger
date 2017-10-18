devtools::document()
devtools::install()
library(pirMerger)
library(testthat)

# Read exported data files ----

working_dir <- "scratch"
repo_data_dir <- paste(working_dir, "repo_data", sep = "/")
source_data_dir <- paste(working_dir, "source_data", sep = "/")
intermediate_data_dir <- paste(working_dir, "intermediate_data", sep = "/")
pipeline_data_dir <- paste(working_dir, "pipeline_data", sep = "/")

unlink(working_dir, recursive = TRUE)
dir.create(working_dir)
dir.create(repo_data_dir)
dir.create(source_data_dir)
dir.create(intermediate_data_dir)
dir.create(pipeline_data_dir)

working_dict <- secret::get_secret("working_dict")
repo_url <- secret::get_secret("repo_url")

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
  "raw_us_cpi.rds",
  "raw_sales_contents_auth_currencies.rds",
  "raw_currencies_aat.rds",
  "raw_dimensions_aat.rds",
  "raw_units_aat.rds"
)

# Pull the current STAR exports and extract
pull_star_exports(out_dir = repo_data_dir, export_repo = repo_url)
expect_true(dir.exists(paste(repo_data_dir, "csv", sep = "/")))

# Process export files and Google Sheets files into dataframes
read_all_exports(out_dir = source_data_dir, data_dict = working_dict, repo_path = repo_data_dir)
read_all_concordances(out_dir = source_data_dir, data_dict = working_dict)
#expect_equivalent(sort(dir(source_data_dir, "*.rds")), sort(required_rds))

# Process singleton tables that are needed for a variety of datasets
produce_currency_ids(source_data_dir, intermediate_data_dir)
#expect_true(file.exists(paste(intermediate_data_dir, "currency_aat.rds", sep = "/")))
produce_exchange_rates(source_data_dir, intermediate_data_dir)
produce_cpi(source_data_dir, intermediate_data_dir)

# Process artist authority table
produce_artists_authority(source_data_dir, intermediate_data_dir)
#expect_true(file.exists(paste(intermediate_data_dir, "artists_authority.rds", sep = "/")))
produce_ulan_derivative_artists(intermediate_data_dir, "../getty-provenance-index")

# Process owner authority table
produce_owners_authority(source_data_dir, intermediate_data_dir)
#expect_true(file.exists(paste(intermediate_data_dir, "owners_authority.rds", sep = "/")))
produce_ulan_derivative_owners(intermediate_data_dir, "../getty-provenance-index")

# Process Knoedler and its tables
required_knoedler <- c(
  "knoedler.rds",
  "knoedler_stocknumber_concordance.rds",
  "knoedler_artists.rds",
  "knoedler_buyers.rds",
  "knoedler_joint_owners.rds",
  "knoedler_sellers.rds",
  "knoedler_materials_classified_as_aat.rds",
  "knoedler_materials_object_aat.rds",
  "knoedler_materials_support_aat.rds",
  "knoedler_materials_technique_as_aat.rds",
  "knoedler_subject_aat.rds",
  "knoedler_style_aat.rds",
  "knoedler_subject_classified_as_aat.rds",
  "knoedler_depicts_aat.rds",
  "knoedler_dimensions.rds"
)
produce_knoedler(source_data_dir, intermediate_data_dir)
#expect_equivalent(sort(dir(intermediate_data_dir, pattern = "knoedler.*.rds")), sort(required_knoedler))
jk <- produce_joined_knoedler(intermediate_data_dir, pipeline_data_dir)
union_aat_ids <- produce_union_aat(intermediate_data_dir)
write_lines(union_aat_ids, path = "../pirdata/unique_aat_ids.txt", na = "")
write_csv(jk, path = "../pirdata/knoedler_join.csv", na = "")

produce_sales_contents(source_data_dir, intermediate_data_dir)
produce_sales_descriptions(source_data_dir, intermediate_data_dir)
produce_sales_catalogs_info(source_data_dir, intermediate_data_dir)
