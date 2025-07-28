# Pour la manipulation des données
library(tidyverse)

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

cols_fa <- M2_F1 %>% 
  select(all_of(starts_with("fa_"))) %>% 
  colnames()

cols_ps <- M2_F1 %>% 
  select(all_of(starts_with("ps_"))) %>% 
  colnames()

M2_F2 <- M2_F1 %>% 
  rename_with(~ paste0("sc_", .x), etspps) %>% 
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>% 
  rename("id_tab_db" = "tab_db",
         "sc_commentaires" = "commentaires_scol",
         "sc_id_cat" = "id_sc_cat")

### Clean up
rm(list = c("cols_fa",
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


# M1_V0 ####

M1_V0 <- M2_F3

### Nettoyer l'environnement 
rm(list = c("ids_TPO",
            "clean",
            "doublons",
            "identity",
            "M2_F2",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

# Réduire vars_PPS ####

M1_V0 <- M2_F3 %>% 
  mutate(
    sc_plan_ets1 = case_when(
      sc_plan_ets1_1 == 1 ~ "CP",
      sc_plan_ets1_2 == 1 ~ "CE1",
      sc_plan_ets1_3 == 1 ~ "CE2",
      sc_plan_ets1_4 == 1 ~ "CM1",
      sc_plan_ets1_5 == 1 ~ "CM2",
      sc_plan_ets1_6 == 1 ~ "6ème",
      sc_plan_ets1_7 == 1 ~ "5ème",
      sc_plan_ets1_8 == 1 ~ "4ème",
      sc_plan_ets1_9 == 1 ~ "3ème",
      sc_plan_ets1_12 == 1 ~ "Seconde",
      sc_plan_ets1_13 == 1 ~ "Première",
      sc_plan_ets1_14 == 1 ~ "Terminale",
      TRUE ~ NA_character_
      ),
    sc_plan_ets2 = case_when(
      sc_plan_ets2_1 == 1 ~ "CP",
      sc_plan_ets2_2 == 1 ~ "CE1",
      sc_plan_ets2_3 == 1 ~ "CE2",
      sc_plan_ets2_4 == 1 ~ "CM1",
      sc_plan_ets2_5 == 1 ~ "CM2",
      sc_plan_ets2_6 == 1 ~ "6ème",
      sc_plan_ets2_7 == 1 ~ "5ème",
      sc_plan_ets2_8 == 1 ~ "4ème",
      sc_plan_ets2_9 == 1 ~ "3ème",
      sc_plan_ets2_14 == 1 ~ "Terminale",
      TRUE ~ NA_character_
    ),
    sc_plan_ets5 = case_when(
      sc_plan_ets5_13 == 1 ~ "Première",
      sc_plan_ets5_14 == 1 ~ "Terminale",
      TRUE ~ NA_character_
    )) %>%
  mutate(
    sc_plan_ets1 = factor(
      sc_plan_ets1, 
      levels = c("CP",
                 "CE1",
                 "CE2",
                 "CM1",
                 "CM2",
                 "6ème",
                 "5ème",
                 "4ème",
                 "3ème",
                 "Seconde",
                 "Première",
                 "Terminale")),
    sc_plan_ets2 = factor(
      sc_plan_ets2, 
      levels = c("CP",
                 "CE1",
                 "CE2",
                 "CM1",
                 "CM2",
                 "6ème",
                 "5ème",
                 "4ème",
                 "3ème",
                 "Terminale")),
    sc_plan_ets5 = factor(
      sc_plan_ets5,
      levels = c("Première",
                 "Terminale")
    )
  )



# 1. Dictionnaire niveau → libellé
niveau_labels <- c(
  "1"  = "CP",
  "2"  = "CE1",
  "3"  = "CE2",
  "4"  = "CM1",
  "5"  = "CM2",
  "6"  = "6ème",
  "7"  = "5ème",
  "8"  = "4ème",
  "9"  = "3ème",
  "12" = "Seconde",
  "13" = "Première",
  "14" = "Terminale"
)

# 2. Fonction qui extrait le bon libellé à partir des colonnes binaires
recode_plan_ets <- function(df, prefix) {
  lignes <- apply(df %>% select(starts_with(prefix)), 1, function(row) {
    code <- names(row)[which(row == 1)[1]]  # premier code = celui sélectionné
    if (is.na(code)) return(NA_character_)
    niveau <- sub(".*_", "", code)          # ex : "sc_plan_ets1_4" → "4"
    niveau_labels[[niveau]] %||% NA_character_
  })
  factor(lignes, levels = niveau_labels)
}

M1_Vx <- M2_F3 %>% 
  mutate(
    sc_plan_ets1 = recode_plan_ets(., "sc_plan_ets1_"),
    sc_plan_ets2 = recode_plan_ets(., "sc_plan_ets2_"),
    sc_plan_ets5 = recode_plan_ets(., "sc_plan_ets5_")
  )







