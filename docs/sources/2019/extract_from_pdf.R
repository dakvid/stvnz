# extract_from_pdf.R
# Use tabulizer to extract the two tables for each election from PDF iteration
# reports. Need to manually provide page numbers and identify table areas.

library(magrittr)
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(tabulizer)

extract_results_from_pdf <- 
  function(pdf_file, page_range) {
    extract_areas(pdf_file,
                  page_range) %>% 
      map_df(as_tibble) %>% 
      filter(V1 != "") %>% 
      set_names(head(., 1)) %>% 
      tail(-1) %>% 
      mutate(
        ITER = ITER %>% as.integer(),
        VOTES = VOTES %>% str_replace(",", "") %>% as.double(),
        NTV = NTV %>% str_replace(",", "") %>% as.double(),
        QUOTA = QUOTA %>% str_replace(",", "") %>% as.double(),
        KEEP = KEEP %>% as.double()
      )
  }

extract_iterations_from_pdf <- 
  function(pdf_file, page_range) {
    extract_areas(pdf_file,
                  page_range) %>% 
      map_df(as_tibble) %>% 
      filter(V1 != "") %>% 
      set_names(head(., 1)) %>% 
      tail(-1) %>% 
      mutate(
        ITER = ITER %>% as.integer(),
        VOTES = VOTES %>% str_replace(",", "") %>% as.double()
      )
}
