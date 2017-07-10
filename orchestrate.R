library(pirMerger)

working_dir <- tempdir()

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

read_all_exports(outdir = working_dir)
read_all_concordances(outdir = working_dir)

expect_equivalent(dir("data-raw", "*.rds"), required_rds)