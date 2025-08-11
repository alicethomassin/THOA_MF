# 0. setup ---------------------------------------------------------------------
library(tidyverse)
library(forcats)
library(patchwork)
data <- read.csv("../clean_data/thoa_MV11.csv")

data <- data %>% 
  rename(
    "pr_id_cat" = "id_pr_cat",
    "fa_id_cat" = "id_fa_cat",
    "lo_id_cat" = "id_lo_cat",
    "qv_id_cat" = "id_qv_cat",
    "sc_id_cat" = "id_sc_cat"
  ) %>% 
  select(-id_age_cat) %>% 
  mutate(
    id_age_cat_3 = case_when(
      id_age_cat_2 == "Adolescent" ~ "17 ans ou moins",
      id_age >= 40 & id_age < 50 ~ "40 à 49 ans",
      id_age >= 50 & id_age < 60 ~ "50 à 59 ans",
      id_age >= 60 ~ "60 ans ou plus",
      TRUE ~ id_age_cat_3
    )
  )

# 1. Identité ------------------------------------------------------------------

vars_id <- data %>% 
  select(all_of(starts_with("id_")))

M2_F2 <- M2_F1 %>% 
  rename_with(~ paste0("sc_", .x), etspps) %>% 
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>% 
  rename("id_tab_db" = "tab_db",
         "sc_commentaires" = "commentaires_scol",
         "sc_id_cat" = "id_sc_cat") %>% 
  mutate(
    across(contains("_date"), ymd),
    sc_an_diplome = factor(as.integer(sc_an_diplome)))
