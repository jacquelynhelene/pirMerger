refresh_definitions <- function() {
  current_raw_data <- readLines("data_definitions.yml")
  secret::update_secret("raw_data", value = current_raw_data, vault = "secret")
}

write_definitions <- function() {
  raw_data_text <- secret::get_secret("raw_data", vault = "secret")
  writeLines(raw_data_definitions, "raw_data.yml")
}
