# ACOG Data
library(tidyverse) # Main Package - Loads dplyr, purrr
library(rvest)     # HTML Web Scraping
library(furrr)     # Parallel Processing using purrr (iteration)
library(xopen)     # Quickly opening URLs

# data
states_postalcodes_tbl <- read_rds("states_postalcodes_tbl.rds")

# CREATE FUCNTION TO GET ALL PROFILE INFORMATION ON THE SEARCH RESULT LIST
get_all_profile_info <- function(st_pc_url) {
  
  # get addresses
  acog_addresses <- st_pc_url |>
    # reads the web page
    read_html() |>
    html_element("#listingTable") |>
    html_elements("li") |>
    html_element("address") |>
    html_text2() |> enframe(name = "position", value = "address") |>
    mutate(address = address |>
             str_replace_all(pattern = "Get Directions", replacement = "")) |> 
    mutate(address = address |> str_remove_all(pattern = "[\\r\\n+]")) |> 
    separate(address, c("address", "Phone Number"),sep = "Phone: ",extra = "merge", fill = "right")
   
  # get profile names and title
  acog_names_title <- st_pc_url |>
    read_html() |>
    html_element("#listingTable") |>
    html_elements("li") |>
    html_element("a") |>
    html_text2() |> enframe(name = "position", value = "names_title")|>
    separate(names_title, c("Names", "Title"),sep = ", ", extra = "merge", fill = "right")
  
  # get profile urls
  acog_profile_url <- st_pc_url |>
    read_html() |>
    html_element("#listingTable") |>
    html_elements("li") |>
    html_element("a") |>
    html_attr("href") |> enframe(name = "position", value = "profile_url")

  # combine all 3 tibbles  
  combined_tbl <- left_join(acog_addresses, acog_names_title, by=join_by(position)) |> 
  left_join(acog_profile_url, by=join_by(position))
  
  return(combined_tbl)
}


# PROCESSED IN PARALLEL with furrr (5 minutes)
plan("multicore")
profile_tbl_1000 <- states_postalcodes_tbl[1:1000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_1000, "data/profile_tbl_1000.rds")


profile_tbl_2000 <- states_postalcodes_tbl[1001:2000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_2000, "data/profile_tbl_2000.rds")

profile_tbl_3000 <- states_postalcodes_tbl[2001:3000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_3000, "data/profile_tbl_3000.rds")


profile_tbl_4000 <- states_postalcodes_tbl[3001:4000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_4000, "data/profile_tbl_4000.rds")


profile_tbl_5000 <- states_postalcodes_tbl[4001:5000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_5000, "data/profile_tbl_5000.rds")


profile_tbl_6000 <- states_postalcodes_tbl[5001:6000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_6000, "data/profile_tbl_6000.rds")


profile_tbl_7000 <- states_postalcodes_tbl[6001:7000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_7000, "data/profile_tbl_7000.rds")


profile_tbl_8000 <- states_postalcodes_tbl[7001:8000, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_8000, "data/profile_tbl_8000.rds")


profile_tbl_9918 <- states_postalcodes_tbl[8001:9918, ] |>
  mutate(obgyn_profile = future_map(postalcodes_search_url, get_all_profile_info)) |>
  unnest(obgyn_profile) |> 
  # getting the insurance info from individual ob-gyn web page
  mutate(
        individual_url = str_glue("https://www.acog.org{profile_url}")
    ) 

write_rds(profile_tbl_9918, "data/profile_tbl_9918.rds")



all_profile_tbl <- profile_tbl_1000 |> 
  bind_rows(profile_tbl_2000)|> 
  bind_rows(profile_tbl_3000) |> 
  bind_rows(profile_tbl_4000) |> 
  bind_rows(profile_tbl_5000) |> 
  bind_rows(profile_tbl_6000) |> 
  bind_rows(profile_tbl_7000) |> 
  bind_rows(profile_tbl_8000) |> 
  bind_rows(profile_tbl_9918)

write_rds(all_profile_tbl, "all_profile_tbl.rds")

# output
# [1] "\r ANCHORAGE WOMEN'S CLINIC\r\n\r 3260 Providence Dr Ste 425 \r Anchorage, AK 99508-4603\r United States\n\r Phone: \r \r (907) 561-7111\r\n\r Fax: \r \r (907) 770-7891\n\r Website: \r \r anchoragewomensclinic.com\r\n\r Email: \r \r [email protected]\r\n\r \r Accepts Medicaid\n\r \r Accepts Medicare\nGet Directions\n"


get_insurance_info <- function(obgyn_url){
  
  contact_info <- obgyn_url |> 
  read_html() |> 
  html_element("address") |>
  html_text2() |> enframe(name = "ins_id", value = "ind_address") |> 
  separate(ind_address, c("Contact Info", "Insurance 1", "Insurance 2"), sep = "\\sAccepts",extra = "merge", fill = "right") |> 
  mutate(across(3:4, ~ str_remove_all(.x, "Get Directions"))) |> 
  mutate(across(2:4, ~ str_replace_all(.x, pattern = "[\\r\\n+]",replacement = ""))) |> 
    mutate(across(2:4, str_trim))
  #mutate(`Contact Info` = `Contact Info` |> str_replace_all(pattern = "\\r\\n",replacement = ""))

  return(contact_info)
}

# PROCESSED IN PARALLEL with furrr (5 minutes)
# plan("multicore")
# obgyn_profile_tbl <- all_profile_tbl |>
#   mutate(obgyn_insurance = future_map(individual_url, get_insurance_info)) |>
#   unnest(obgyn_insurance)
# 
# write_rds(obgyn_profile_tbl, "obgyn_profile.rds")


