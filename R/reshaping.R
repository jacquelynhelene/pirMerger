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
norm_vars <- function(data, newcol = "group_no", base_names, n_reps, n_sep_char = "_", idcols, ...) {

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

  df %>%
    separate_(col = source_col, into = paste(source_col, seq_len(target_separations), sep = "_"), sep = sep, extra = "merge", fill = "right")
}
