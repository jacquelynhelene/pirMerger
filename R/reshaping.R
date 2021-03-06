#' Wapper of unite/gather/separate to reshape groups of denormalized columns.
#'
#' Specify the base names of the columns that are repeated (e.g. \code{c("a",
#' "b", "c")}), along with the number of times that those columns are repeated.
#'
#' @param data A data frame
#' @param newcol A temporary column name in which new values will be stored
#' @param base_names Quoted column names that are repeated as a unit. The
#'   parameter \code{n_reps} will be treated as a suffix for uniting, gathering,
#'   and separating these columns as groups.
#' @param n_reps Number of times the \code{base_names} repeat.
#' @param n_sep_char Delimiter that separates \code{base_names} from \code{n_reps}
#' @param idcols Quoted names of columns that should be kept as identifiers
#'   when gathering these column groups.
#'
#' @export
norm_vars <- function(data, newcol = "group_no", base_names, n_reps, n_sep_char = "_", idcols, check_names = TRUE, ...) {

  if (check_names) {
    # Validate that the names are indeed in sequence in the original table
    candidate_names <- paste(rep(base_names, times = n_reps), rep(seq_len(n_reps), each = length(base_names)), sep = n_sep_char)
    match_indices <- match(candidate_names, names(data))
    if (!identical(seq(from = first(match_indices), length.out = length(match_indices)), match_indices)) {
      try_names <- names(data)[first(match_indices):last(match_indices)]
      stop("Provided column name sequence is not contiguous in source data.\nPresent in source but not target: ", paste(setdiff(try_names, candidate_names), collapse = ", "))
    }
  }

  dd <- reduce(seq_len(n_reps), function(x, y) {
    unite_(x, col = paste0(newcol, y), from = paste(base_names, y, sep = n_sep_char), sep = "Ω")
  }, .init = data) %>%
    select(one_of(c(idcols, paste0(newcol, seq_len(n_reps))))) %>%
    gather_(key_col = paste0(newcol, "_no"), value_col = "values", gather_cols = paste0(newcol, seq_len(n_reps)), na.rm = TRUE) %>%
    # select_(paste0("-", newcol, "_no")) %>%
    separate_("values", into = base_names, sep = "Ω", convert = TRUE)

  valid_cases <- pmap_lgl(map(dd[, base_names], function(x) !is.na(x)), any)

  dd %>%
    filter(valid_cases) %>%
    select(-group_no_no)
}

#' Join a forign table multiple times, once for each reptition of a specified column
#'
#' @param df Dataframe. The table on the left side of each join
#' @param jdf Dataframe. The table on the right side of each join
#' @param from Character. Quoted column name with the foreign key in the data frame to be joined
#' @param to Character. Quoted column name with the foreign key in the source data
#' @param n_reps Integer. Number of times `to` is repeated
#' @param prefix
#'
#' @return A dataframe
multi_join <- function(df, jdf, from, to, n_reps, prefix) {
  reduce(seq_len(n_reps), function(x, y) {
    message("Joining to ")
    target_names <- paste(names(jdf), prefix, y, sep = "_")
    rn_jdf <- rename_(jdf, .dots = set_names(names(jdf), target_names))
    res <- left_join(x, rn_jdf, by = set_names(paste(to, prefix, y, sep = "_"), paste(from, y, sep = "_")))

    stopifnot(all(targetnames %in% names(res)))
  }, .init = df)
}

#' Take a long table of multiple linked columns and reshape to a wide table
#'
#' @param df Data frame.
#' @param idcol Quoted name of a single id column.
#'
#' @return Data frame.
#'
#' @export
spread_out <- function(df, idcol) {
  # If this is an empty data frame, return the original one - otherwise the code
  # below errors on the assumption that there is at least 1 row.
  if (nrow(df) < 1)
    return(df)

  united_cols <- setdiff(names(df), idcol)

  almost_wide <- df %>%
    unite_(col = "tempcol", from = united_cols, sep = "Ω") %>%
    group_by_(.dots = idcol) %>%
    mutate(group_index = seq_along(tempcol)) %>%
    spread(group_index, tempcol) %>%
    ungroup()

  n_new <- ncol(almost_wide) - 1

  multi_separate(almost_wide, n_reps = n_new, into = united_cols, convert = TRUE, fill = "right", sep = "Ω")
}

#' Repeatedly separate single fields into multiple ones
#'
#' This is useful when spreading apart a column that had been previously merged so as to be easier to repeatedly join using multi_join
#'
#' @param df Dataframe.
#' @param n_reps Integer. Number of times said column is repeated
#' @param into Character. Quoted names of columns (without any integer suffixes) to be produced after splitting
#' @param ... Further arguments passed to separate_
#'
#' @return A dataframe.
#' @export
multi_separate <- function(df, n_reps, into, ...) {
  reduce(seq_len(n_reps), function(x, y) {
    from_name = as.character(y)
    into_names <- paste(into, y, sep = "_")
    separate_(x, col = from_name, into = into_names, ...)
  }, .init = df)
}

#' Detect how many columns to split a single col into
#'
#' @param df Data frame.
#' @param source_col Quoted name of source column
#' @param sep Character used as a delimiter within the column
#'
#' @return A data frame.
#' @export
single_separate <- function(df, source_col, sep = ";") {
  stopifnot(source_col %in% names(df))

  separations_count <- stringr::str_count(df[[source_col]], pattern = sep)

  if (all(is.na(separations_count)))
    return(df)

  max_separations <- max(separations_count, na.rm = TRUE)

  if (max_separations == 0)
    return(df)

  target_separations <- max_separations + 1

  into_names <- paste(source_col, seq_len(target_separations), sep = "_")

  df %>%
    separate_(col = source_col, into = into_names, sep = sep, extra = "merge", fill = "right") %>%
    mutate_at(vars(one_of(into_names)), str_trim)
}
