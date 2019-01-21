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