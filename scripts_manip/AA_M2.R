# Pour la manipulation des données
library(tidyverse)

# Pour importer les données SAS
library(haven)

# Pour identifier les colonnes vides
library(janitor)

# SCOLARITÉ ####

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
         "sc_debut_date" = "sc_date_debut")

## Remove duplicates ####

doublons <- M2_F1 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M2_F1 %>% 
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

vars_doublons <- union("id_link", setdiff(names(M2_F1), vars_identity))

clean <- full_join(
  identity %>% 
    select(all_of(vars_identity)),
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
) %>% 
  select(-id_link)

ids_TPO <- union(list_doublons, clean$id_anonymat)

M2_F2 <- bind_rows(
  M2_F1 %>% 
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

# Étiquettes des variables ####

M2_F3 <- M2_F2 %>%
  {
    vars_ets <- select(., starts_with("sc_plan_ets")) %>% names()
    labels_ets <- set_names(rep("SC-20", length(vars_ets)), vars_ets)
    set_variable_labels(., !!!labels_ets)
  } %>% 
  {
    vars_notif <- select(., starts_with("sc_plan_notif")) %>% names()
    labels_notif <- set_names(rep("SC-21", length(vars_notif)), vars_notif)
    set_variable_labels(., !!!labels_notif)
  } %>% 
  set_variable_labels(
    id_nom = "ID-01",
    id_nom_jeune = "ID-02",
    id_prenom = "ID-03",
    id_sexe = "ID-04",
    id_date_nais = "ID-05",
    id_lieu_nais = "ID-06",
    id_dep_nais = "ID-06",
    id_centre1 = "ID-08",
    id_centre2 = "ID-09",
    id_centre3 = "ID-10",
    id_age = "id-05",
    id_age_cat_2 = "id-05",
    id_age_cat_3 = "id-05",
    id_anonymat = "id- Identifiant unique",
    id_link = "id- Outil doublons",
    id_sep = "id- Outil doublons",
    id_tab_db = "id- Outil doublons",
    id_type = "id- si module complété",
    sc_debut = "SC-01",
    sc_debut_autre = "SC-01",
    sc_debut_corr = "sc-01",
    sc_debut_an = "SC-02",
    sc_debut_age = "sc-02",
    sc_debut_date = "sc-02",
    sc_redoubl = "SC-03",
    sc_interromp = "SC-07",
    sc_plan = "SC-18",
    sc_plan_an = "SC-19",
    sc_etspps = "sc-20",
    sc_plan_demande = "SC-22",
    sc_fin_etudes = "SC-23",
    sc_formation = "SC-24",
    sc_formation_autre = "SC-24",
    sc_diplome = "SC-25",
    sc_diplome_autre = "SC-25",
    sc_diplome_cat = "sc-25",
    sc_diplome_an = "SC-26",
    sc_diplome_age = "sc-26",
    sc_diplome_date = "sc-26",
    sc_commentaires = "SC-27",
    sc_type = "sc- si module complété"
  )

