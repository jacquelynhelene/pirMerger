#' Join a forign table multiple times, once for each reptition of a specified column
#'
#' @param df Dataframe. The table on the left side of each join
#' @param jdf Dataframe. The table on the right side of each join
#' @param from Character. Quoted column name with the foreign key in the data frame to be joined
#' @param to Character. Quoted column name with the foreign key in the source data
#' @param n_reps Integer. Number of times `to` is repeated
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
