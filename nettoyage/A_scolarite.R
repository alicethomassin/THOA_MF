# Pour la manipulation des données
library(tidyverse)

# Pour importer les données SAS
library(haven)
library(questionr)

# 1. SCOLARITÉ ####

# Fonction pour créer variable de lien pour les doublons
make_id_link <- function(df){
  
  df %>%
    separate(id_sep,
             into = c("rid", "id_link"),
             sep = "_",
             remove = FALSE,
             fill = "right") %>% 
    select(-rid) %>% 
    mutate(
      id_link = case_when(
        is.na(id_date_creation) ~ id_anonymat,
        is.na(id_link) ~ id_anonymat,
        TRUE ~ id_link
      )
    ) 
}

## 1.1 rename ####
M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")

cols_fa <- M2_F0 %>% 
  select(all_of(starts_with("fa_")),
         -fa_an_diplome) %>% 
  colnames()

cols_ps <- M2_F0 %>% 
  select(all_of(starts_with("ps_")),
         -ps_an_debut) %>% 
  colnames()

M2_F1 <- M2_F0 %>% 
  rename_with(~ paste0("sc_", .x), etspps) %>% 
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>% 
  rename("id_tab_db" = "tab_db",
         "sc_commentaires" = "commentaires_scol",
         "sc_id_cat" = "id_sc_cat",
         "sc_diplome_an" = "fa_an_diplome",
         "sc_diplome_age" = "sc_age_diplome",
         "sc_diplome_date" =  "sc_date_diplome",
         "sc_debut_an" = "ps_an_debut",
         "sc_debut_age" = "sc_age_debut",
         "sc_debut_date" = "sc_date_debut") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>%
  make_id_link(.)

## 1.2 remove duplicates ####
doublons <- M2_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

# Fonction pour trouver les lignes qui comprennent l'identité
find_identity <- function(df){
  df %>% 
    filter(
      str_ends(
        id_sep, regex(paste0(list_doublons, collapse = "|"))))
}

#identity <- M2_F1 %>% 
#  find_identity(.) %>% 
#  separate(id_sep,
#           into = c("rid", "id_link"),
#           sep = "_",
#           remove = FALSE) %>% 
#  select(-rid)
  
identity <- M2_F1 %>% 
  filter(id_link %in% list_doublons,
         !is.na(id_date_creation)) %>% 
  relocate(id_link) %>% 
  arrange(id_link)

vars_identity <- identity %>% 
  select(all_of(starts_with("id_"))) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(M2_F1), vars_identity))

clean <- full_join(
  identity %>% 
    select(all_of(vars_identity)),
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(list_doublons, clean$id_anonymat)

M2_F2 <- bind_rows(
  M2_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
)

# CLean up
rm(list = c("clean",
            "doublons",
            "identity",
            "cols_fa",
            "cols_ps",
            "ids_TPO",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

## 1.3 recoder variables ####



# 2. REDOUBLEMENT ####
M2_rdb_F0 <- read_sas("../raw_data/gb_ddb_sc_02_rdb_02.sas7bdat")

## 2.1 rename ####
cols_fa <- M2_rdb_F0 %>% 
  select(all_of(starts_with("fa_")),
         -fa_an_diplome) %>% 
  colnames()

cols_ps <- M2_rdb_F0 %>% 
  select(all_of(starts_with("ps_")),
         -ps_an_debut) %>% 
  colnames()

cols_rdb <- M2_rdb_F0 %>% 
  select(all_of(starts_with("rdb_")), -rdb_ps_redoubl) %>% 
  colnames()

cols_rd <- M2_rdb_F0 %>% 
  select(all_of(starts_with("rd_"))) %>% 
  colnames()




M2_rdb_F1 <- M2_rdb_F0 %>%  
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>%
  rename_with(~ gsub("^rdb_", "sc_rdb_", .x), all_of(cols_rdb)) %>%
  rename_with(~ gsub("^rd_", "", .x), all_of(cols_rd)) %>%
  rename("id_tab_db" = "tab_db",
         "sc_rdb_redoubl" = "rdb_ps_redoubl",
         "sc_diplome_an" = "fa_an_diplome",
         "sc_id_cat" = "id_sc_cat",
         "sc_debut_an" = "ps_an_debut",
         "sc_debut_age" = "sc_age_debut",
         "sc_debut_date" = "sc_date_debut") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>%
  make_id_link(.)

### Clean up 
rm(list = c("M2_rdb_F0",
            "cols_fa",
            "cols_ps",
            "cols_rd",
            "cols_rdb"))

## 2.2 Remove duplicates ####
common_vars <- intersect(names(M2_rdb_F1), names(M2_F2))

vars_doublons <- union("id_link", setdiff(names(M2_rdb_F1), common_vars))

doublons <- M2_rdb_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- M2_F2 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  by = "id_link"
)

# J'ai bien vérifié que celui qui n'avait pas de doublons pouvait faire parti 
# de la liste puisqu'il n'avait qu'une seule ligne pour le doublement. Je ne 
# risque donc pas de retirer une autre ligne où il aurait eu ses données 
# d'identité
ids_TPO <- unique(identity$id_link)

M2_rdb_F2 <- bind_rows(
  M2_rdb_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>%
  filter(sc_rdb_type == "P") %>% 
  mutate(id_age_cat = case_when(
    (is.na(id_age_cat) & id_age < 18) ~ "Adolescent",
    (is.na(id_age_cat) & id_age > 17) ~ "Adulte",
    TRUE ~ id_age_cat
  ))
M2_rdb_F2 %>%
  miss_var_summary() %>%
  gt()
### Nettoyer l'environnement 
rm(list = c("M2_rdb_F1",
            "doublons",
            "list_doublons",
            "identity",
            "vars_identity",
            "vars_doublons",
            "clean",
            "ids_TPO"))