# Pour la manipulation des données
library(tidyverse)

# Pour ouvrir le script sur d'autres ordis
library(here)

# Pour importer les données SAS
library(haven)

# Pour identifier les colonnes vides
library(janitor)

# SCOLARITÉ ####

M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")
## Remove empty cols ####

M2_F1 <- M2_F0 %>%
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## Rename cols ####

cols_unique <- c("commentaires_scol", "etspps")

cols_fa <- M2_F1 %>% 
  select(all_of(starts_with("fa_"))) %>% 
  colnames()

cols_ps <- M2_F1 %>% 
  select(all_of(starts_with("ps_"))) %>% 
  colnames()

M2_F2 <- M2_F1 %>% 
  rename_with(~ paste0("sc_", .x), all_of(cols_unique)) %>% 
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>% 
  rename( "id_tab_db" = "tab_db")

### Clean up
rm(list = c("cols_unique",
            "cols_fa",
            "cols_ps",
            "M2_F0",
            "M2_F1"))

## Remove duplicates ####

doublons <- M2_F2 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M2_F2 %>% 
  filter(
    str_ends(
      id_sep, regex(paste0(list_doublons, collapse = "|")))) %>% 
  separate(id_sep,
           into = c("rid", "id_link"),
           sep = "_",
           remove = FALSE) %>% 
  select(-rid)

vars_identity <- identity %>% 
  select(all_of(starts_with("id_"))) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(M2_F2), vars_identity))

clean <- full_join(
  identity %>% 
    select(all_of(vars_identity)),
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
) %>% 
  select(-id_link)

ids_TPO <- union(list_doublons, clean$id_anonymat)

M2_F3 <- bind_rows(
  M2_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>% 
  separate(id_sep,
           into = c("rid", "id_link"),
           sep = "_",
           remove = FALSE,
           fill = "right") %>%
  select(-rid) %>% 
  mutate(id_link = case_when(
    is.na(id_link) ~ id_anonymat,
    TRUE ~ id_link
  ))

### Nettoyer l'environnement 
rm(list = c("module_scolarite",
            "ids_TPO",
            "clean",
            "doublons",
            "identity",
            "M2_F0",
            "M2_F1",
            "M2_F2",
            "cols_fa",
            "cols_ps",
            "cols_unique",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

