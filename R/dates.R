#' Diff years
#'
#' Calculate difference in years between dates
#'
#' @param first start date
#' @param last end date
#'
#' @importFrom lubridate as.duration dyears
#' @export


diff_years <- function(first, last){
  lubridate::as.duration(lubridate::interval(first, last)) / lubridate::dyears(1)
}

#' As a date
#'
#' format number to date with origin = "1970-01-01"
#'
#' @param x date
#'
#' @export

as_a_date <- function(x){
  as.Date(x, origin = "1970-01-01")
}

#' Substitute leap year
#'
#' @param x numeric date
#' @importFrom stringr str_sub

sub_leap <- function(x){
  stringr::str_sub(x, 5, 8) <- "0301"
  as.integer(x)
}

make_date_logical <- function(x){
  x <- as.integer(x)
  dplyr::case_when(
    is.na(lubridate::ymd(x, quiet = TRUE)) & stringr::str_sub(x, 5,8) == "0229" ~ sub_leap(x),
    TRUE ~x
  )
}

#' Construct logical date
#'
#' Transform a numeric value (YYYYMMDD) to a logical numeric date.
#' The function sets February 29th, on non leap years as Mars 1st.
#' Dates without months and days are set to mid year of mid month.
#'
#' @param x vector of numerical dates
#' @param month replacement for dates with missing month and day
#' @param day replacement for dates with missing day
#'
#' @importFrom dplyr case_when
#'
#' @examples
#' construct_date(18810105)
#' construct_date(19800000)
#' construct_date(19810229)
#'
#' @export

construct_date <- function(x, month = 701, day = 15){
  x <- as.integer(x)
  y <- case_when(
    x %% 10000 == 0 ~ as.integer(x + month), # no month
    x %% 10000 > 0 & x %% 100 == 0 ~ as.integer(x + day), # no day
    TRUE ~ as.integer(x)
  )
  make_date_logical(y)
}


fill_var <- function(d, var, up, idvar){
  list_df <- d[, list(y=list(get(var))), by=idvar]
  unlist(parallel::mclapply(list_df$y, zoo::na.locf, na.rm=F, fromLast = up, mc.cores = 11) )
}


fill_var_win <- function(d, var, up, idvar){
  list_df <- d[, list(y=list(get(var))), by=idvar]
  unlist(furrr::future_map(list_df$y, ~zoo::na.locf(., na.rm=F, fromLast = up)))
}




#' Multicore fill missing values by group
#'
#' Fast fill in missing values by group. Splits data.frame into groups
#' and fills missing values using zoo::na.locf.
#'
#' @param d data.frame
#' @param var_names vector of variable names
#' @param idvar character with name of grouping variable
#' @param up boolean direction of fill
#'
#' @import future
#' @import data.table
#' @importFrom furrr future_map
#' @importFrom zoo na.locf
#' @importFrom purrr map
#' @importFrom tibble as_tibble
#' @importFrom dplyr bind_cols
#'
#' @examples
#' df <- data.frame(Month = 1:12, Year = c(2000, rep(NA, 11)), theid = rep(1:2, each = 12))
#' fill_split(df, "Year", "theid")
#'
#' @export

fill_split <- function(d, var_names, idvar, up = FALSE){
  os_type <- .Platform$OS.type
  sys_info <- Sys.info()["sysname"]

  # Depending on the OS, use a different parallelization approach
  if (os_type == "windows" || sys_info == "Windows") {
    # Load the furrr package
    future::plan(future::multicore, workers = (future::availableCores() - 1))
    options(future.globals.maxSize = 9e9)


    dt <- data.table::as.data.table(d)
    vars <- purrr::map(var_names, ~fill_var_win(dt, ., up, idvar))
  } else if (os_type == "unix"){
    # Load the parallel package

    dt <- data.table::as.data.table(d)
    vars <- purrr::map(var_names, ~fill_var(dt, ., up, idvar))
  } else {
    stop("OS not supported for parallel execution.")
  }


  bind_cols(
    d[ ,colnames(d)[!colnames(d) %in% var_names] ],
    as_tibble(as.data.frame(vars, col.names = var_names))
  )
}


