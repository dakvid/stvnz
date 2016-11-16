# Extract STV iteration data from original source files provided by councils and DHBs
# David Friggens, November 2016



# Setup -------------------------------------------------------------------

library(readr)
library(readxl)
library(magrittr)
library(dplyr)
library(tidyr)
library(xml2)
library(purrr)
library(assertthat)

# delete everything in "data/"

codes <- read_csv("sources/metadata_codes.csv")
ref_bodies <- read_csv("sources/metadata_bodies.csv")
ref_status <- codes %>% filter(class == "status") %>% select(code, description) %>% rename(status_code = code, status = description)
concordance_names <- read_csv("sources/concordance_names.csv")

dir_prefix <- "sources/2016/"

metadata <- read_csv(paste0(dir_prefix, "/metadata.csv"))

# initialise data frames
output_dfs <- 
  list(
    elect_races =
      data_frame(body_code = character(0),
                 race_id = integer(0),
                 race = character(0),
                 race_type_code = character(0),
                 num_seats = integer(0),
                 num_iterations = integer(0)),
    elect_states =
      data_frame(body_code = character(0),
                 race_id = integer(0),
                 candidate_id = integer(0),
                 iteration = integer(0),
                 votes = double(0),
                 status_code = character(0)),
    elect_iterations =
      data_frame(body_code = character(0),
                 race_id = integer(0),
                 iteration = integer(0),
                 quota = double(0),
                 ntv = double(0)),
    elect_candidates =
      data_frame(body_code = character(0),
                 race_id = integer(0),
                 candidate_id = integer(0),
                 candidate_name = character(0))
  )


# Function Definitions ----------------------------------------------------

get_new_race_id <- function(race_df, local_body_code) {
  if (0 == race_df %>% filter(body_code == local_body_code) %>% nrow()) {
    new_race_id <- 1
  } else {
    new_race_id <- 
      race_df %>% 
      filter(body_code == local_body_code) %>% 
      use_series(race_id) %>% 
      max() %>% 
      add(1)
  }
  return(new_race_id)
}

update_race_info <- function(race_df, new_race_id, num_iterations, md_row) {
  race_df %>%
    bind_rows(data_frame(
      body_code = md_row$local_body,
      race_id = new_race_id,
      race = md_row$race,
      race_type_code = md_row$race_type,
      num_seats = md_row$seats,
      num_iterations = num_iterations
    ))
}

extract_xml <- function(output, xelect) {
  # check inputs
  has_attr(output, "elect_races")
  has_attr(output, "elect_states")
  has_attr(output, "elect_iterations")
  has_attr(output, "elect_candidates")
  assert_that(length(xelect) == 8)
  has_attr(xelect, "file_prefix")
  has_attr(xelect, "local_body")
  # check we have metadata for this local body
  assert_that(0 <= ref_bodies %>% filter(body_code == xelect$local_body) %>% nrow())
  
  my_xml <- read_xml(paste0(dir_prefix, 
                           paste(xelect$file_prefix, xelect$format, sep = ".")))
  xml_iterations <- 
    my_xml %>% 
    xml_find_all("//iter:iteration")
  num_iterations <- xml_iterations %>% length()
  
  my_race_id <- get_new_race_id(output$elect_races, xelect$local_body)
  
  output$elect_races %<>% 
    update_race_info(my_race_id, num_iterations, xelect)
  
  
  output$elect_iterations %<>%
    bind_rows(
      xml_iterations %>% 
        map_df(~ list(iteration = xml_attr(.x, "number") %>% as.integer(),
                      quota = xml_attr(.x, "quota") %>% as.numeric() %>% round(1),
                      ntv = xml_attr(.x, "ntv") %>% as.numeric() %>% round(1))
               ) %>% 
        mutate(body_code = xelect$local_body,
               race_id = my_race_id)
    )
  
  candidate_by_iteration <- 
    xml_iterations %>% 
    map_df(
      function(my_iter) {
        my_iter %>% 
          xml_find_all("iter:candidate") %>% 
          map_df(~ list(candidate_name = xml_attr(.x, "id"),
                        votes = xml_attr(.x, "vote-count") %>% as.double(),
                        status = xml_attr(.x, "status"))) %>% 
          mutate(iteration = my_iter %>% xml_attr("number") %>% as.integer())
      }
    ) %>% 
    left_join(ref_status)
  noNA(candidate_by_iteration)
  # Important that the id is the order that they came in the race
  output$elect_candidates %<>%
    bind_rows(
      # need to sort elected and excluded differently
      bind_rows(
        candidate_by_iteration %>% 
          filter(status_code == "E") %>% 
          group_by(candidate_name, status_code) %>% 
          summarise(iteration = min(iteration)) %>% 
          ungroup() %>% 
          inner_join(candidate_by_iteration) %>% 
          arrange(iteration, desc(votes)),
        candidate_by_iteration %>% 
          filter(status_code == "X") %>% 
          group_by(candidate_name, status_code) %>% 
          summarise(iteration = min(iteration)) %>% 
          ungroup() %>% 
          inner_join(candidate_by_iteration) %>% 
          arrange(desc(iteration), desc(votes)),
        candidate_by_iteration %>% 
          filter(status_code == "W") %>% 
          select(-iteration) %>% 
          distinct()
      ) %>% 
        mutate(candidate_id = row_number()) %>% 
        select(candidate_id, candidate_name) %>% 
        mutate(body_code = xelect$local_body,
               race_id = my_race_id)
    )
  
  output$elect_states %<>%
    bind_rows(
      candidate_by_iteration %>% 
        mutate(body_code = xelect$local_body,
               race_id = my_race_id) %>% 
        inner_join(output$elect_candidates, by = c("body_code", "race_id", "candidate_name")) %>% 
        select(body_code, race_id, candidate_id, iteration, votes, status_code)
    )
  
  
  return(output)
}


extract_tsv <- function(output, telect) {
  # check inputs
  has_attr(output, "elect_races")
  has_attr(output, "elect_states")
  has_attr(output, "elect_iterations")
  has_attr(output, "elect_candidates")
  assert_that(length(telect) == 8)
  has_attr(telect, "file_prefix")
  has_attr(telect, "local_body")
  # check we have metadata for this local body
  assert_that(0 <= ref_bodies %>% filter(body_code == telect$local_body) %>% nrow())
  
  my_results <- 
    read_tsv(paste0(dir_prefix,
                    telect$file_prefix, "_results.", telect$format)) %>% 
    rename(candidate_name = CANDIDATE,
           iteration = ITER,
           votes = VOTES,
           status = STATUS,
           quota = QUOTA,
           ntv = NTV,
           keep = KEEP) %>% 
    left_join(ref_status)
  noNA(my_results)
  
  my_iterations <- 
    read_tsv(paste0(dir_prefix,
                    telect$file_prefix, "_iterations.", telect$format)) %>% 
    rename(iteration = ITER,
           candidate_name = CANDIDATE,
           votes = VOTES)
  
  num_iterations <- my_results %>% use_series(iteration) %>% max()
  
  my_race_id <- get_new_race_id(output$elect_races, telect$local_body)
  
  output$elect_races %<>% 
    update_race_info(my_race_id, num_iterations, telect)
  
  # Only quota and ntv for iterations with actions are reported
  reported_iterations <- 
    my_results %>% 
    select(iteration, quota, ntv) %>% 
    distinct()
  num_voters <- 
    my_iterations %>% 
    filter(iteration == 1) %>% 
    tally(votes) %>% 
    use_series(n)
  calculated_iterations <- 
    my_iterations %>% 
    group_by(iteration) %>% 
    tally(votes) %>% 
    mutate(quota = n / (telect$seats + 1),
           ntv = num_voters - n) %>% 
    select(-n)
  
  output$elect_iterations %<>%
    bind_rows(
      bind_rows(
        reported_iterations,
        calculated_iterations %>% 
          anti_join(reported_iterations, by = "iteration")
      ) %>% 
        mutate(body_code = telect$local_body,
               race_id = my_race_id,
               quota = round(quota, 1),
               ntv = round(ntv, 1)) %>% 
        arrange(iteration)
    )
  
  
  output$elect_candidates %<>%
    bind_rows(
      bind_rows(
        my_results %>% 
          filter(status_code == "E") %>% 
          arrange(iteration, desc(votes)),
        my_results %>% 
          filter(status_code == "X") %>% 
          arrange(desc(iteration, desc(votes))),
        my_results %>% 
          filter(status_code != "E", status_code != "X")
      ) %>% 
        select(candidate_name) %>% 
        mutate(candidate_id = row_number(),
               body_code = telect$local_body,
               race_id = my_race_id)
    )
  
  my_iterations %<>% 
    left_join(
      my_results %>% 
        select(candidate_name, iteration, status_code) %>% 
        transpose %>% 
        map_df(~ data_frame(candidate_name = .x$candidate_name,
                            iteration = .x$iteration:num_iterations,
                            status_code = .x$status_code
                            ))
    ) %>%
    replace_na(list(status_code = "H"))
  noNA(my_iterations)

  
  output$elect_states %<>%
    bind_rows(
      my_iterations %>% 
        mutate(body_code = telect$local_body,
               race_id = my_race_id) %>% 
        inner_join(output$elect_candidates, by = c("body_code", "race_id", "candidate_name")) %>% 
        select(body_code, race_id, candidate_id, iteration, votes, status_code)
    )
  

  return(output)
}


extract_xlsx <- function(output, eelect, sheet_num = 1) {
  # check inputs
  has_attr(output, "elect_races")
  has_attr(output, "elect_states")
  has_attr(output, "elect_iterations")
  has_attr(output, "elect_candidates")
  assert_that(length(eelect) == 8)
  has_attr(eelect, "file_prefix")
  has_attr(eelect, "local_body")
  # check we have metadata for this local body
  assert_that(0 <= ref_bodies %>% filter(body_code == eelect$local_body) %>% nrow())

  my_results <- 
    read_excel(paste0(dir_prefix,
                      eelect$file_prefix,".",eelect$format),
               sheet = sheet_num) %>% 
    rename(iteration = number,
           candidate_name = id,
           votes = `vote-count`) %>% 
    left_join(ref_status)
  
  num_iterations <- my_results %>% use_series(iteration) %>% max()
  
  my_race_id <- get_new_race_id(output$elect_races, eelect$local_body)
  
  output$elect_races %<>% 
    update_race_info(my_race_id, num_iterations, eelect)
  
  output$elect_iterations %<>%
    bind_rows(
      my_results %>% 
        select(iteration, quota, ntv) %>% 
        distinct() %>% 
        mutate(body_code = eelect$local_body,
               race_id = my_race_id)
    )
  
  # Important that the id is the order that they came in the race
  output$elect_candidates %<>%
    bind_rows(
      # need to sort elected and excluded differently
      bind_rows(
        my_results %>% 
          filter(status_code == "E") %>% 
          group_by(candidate_name, status_code) %>% 
          summarise(iteration = min(iteration)) %>% 
          ungroup() %>% 
          inner_join(my_results) %>% 
          arrange(iteration, desc(votes)),
        my_results %>% 
          filter(status_code == "X") %>% 
          group_by(candidate_name, status_code) %>% 
          summarise(iteration = min(iteration)) %>% 
          ungroup() %>% 
          inner_join(my_results) %>% 
          arrange(desc(iteration), desc(votes)),
        my_results %>% 
          filter(status_code == "W") %>% 
          select(-iteration) %>% 
          distinct()
      ) %>% 
        mutate(candidate_id = row_number()) %>% 
        select(candidate_id, candidate_name) %>% 
        mutate(body_code = eelect$local_body,
               race_id = my_race_id)
    )
  
  output$elect_states %<>%
    bind_rows(
      my_results %>% 
        mutate(body_code = eelect$local_body,
               race_id = my_race_id) %>% 
        left_join(output$elect_candidates) %>% 
        select(body_code, race_id, candidate_id, iteration, votes, status_code)
    )
  
  return(output)
}

# Process Data ------------------------------------------------------------

output_dfs <- 
  metadata %>% 
  filter(format == "xml") %>% 
  transpose() %>% 
  reduce(extract_xml, .init = output_dfs)
output_dfs <-
  metadata %>%
  filter(format == "tsv") %>% filter(local_body != "CDHB") %>% 
  transpose() %>%
  reduce(extract_tsv, .init = output_dfs)
output_dfs <-
  metadata %>%
  filter(format == "xlsx") %>%
  transpose() %>%
  reduce(extract_xlsx, .init = output_dfs)
