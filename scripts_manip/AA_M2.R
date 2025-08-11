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

# Appliquer integer ####

vars_int <- M2_F2 %>% 
  select(
    id_centre1, id_centre2, id_centre3, id_dep_nais, id_lieu_nais, id_sexe,
    id_age, id_tab_db, sc_diplome_an, sc_diplome, sc_fin_etudes, sc_formation,
    sc_debut_an, sc_debut, sc_interromp, sc_redoubl, sc_debut_corr, 
    sc_debut_age, sc_diplome_age, starts_with("sc_plan"), -sc_plan_notification_autre) %>% 
  colnames()

vars_choix_multiple <- setdiff(vars_plan, c("sc_plan", "sc_plan_an", "sc_plan_demande"))

M2_F3 <- M2_F2 %>%
  mutate(
    across(
      .cols = all_of(vars_int),
      .fns = ~ as.integer(.)
    ),
    across(
      .cols = all_of(vars_choix_multiple),
      .fns = ~ case_when(
        sc_plan == 1 & is.na(.) ~ 0L,
        sc_plan == 0 & is.na(.) ~ 777L,
        is.na(sc_plan) ~ NA_integer_,
        TRUE ~ .)
      ),
    sc_plan_demande = case_when(
      sc_plan == 1 & is.na(sc_plan_demande) ~ 555L,
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_,
      TRUE ~ sc_plan_demande
    ),
    sc_plan_an = case_when(
      sc_plan == 1 & is.na(sc_plan_an) ~ 1000L,
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_,
      TRUE ~ sc_plan_an
    )
    )


M2_F3 %>% 
  select(starts_with("id_")) %>% 
  glimpse()

no_centre <- M2_F3 %>% 
  select(starts_with("id_")) %>% 
  filter(is.na(id_centre1))

M2_F2 %>% 
  select(starts_with("id_")) %>%
  miss_var_summary() %>% 
  gt() %>% 
  tab_header(title = "Missingness of variables")

M2_ets <- M2_F3 %>% 
  select(id_anonymat, sc_plan, sc_plan_an, sc_plan_demande, all_of(vars_plan_corr))

M2_F2_ets <- M2_F2 %>% 
  select(id_anonymat, sc_plan, sc_plan_an, sc_plan_demande, all_of(vars_plan_corr))

vars_ets <- M2_F3 %>% 
  select(starts_with("sc_plan_ets")) %>% 
  names()

classe_index <- as.integer(str_extract(vars_ets, "\\d+$"))

M2_F4 <- M2_F3 %>%
  mutate(
    sc_plan_classe = pmap_int(
      .l = select(., all_of(vars_ets)),
      .f = function(...) {
        row <- c(...)  # transforme les arguments en vecteur ligne
        first_class <- classe_index[which(row == 1)]
        if (length(first_class) == 0) NA_integer_ else min(first_class)
      }
    ),
    sc_plan_nb = case_when(
      sc_plan == 1 ~ rowSums(select(., all_of(vars_ets)) == 1, na.rm = T),
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_
    )
  )

M2_F4_ets <- M2_F4 %>% 
  select(id_anonymat, sc_plan, sc_plan_nb, sc_plan_an, sc_plan_classe, sc_plan_demande, all_of(vars_plan_corr))

# Corriger NAs ####




# Étiquettes des variables ####

M2_F4 <- M2_F3 %>%
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


# Format des variables ####
M2_F3 <- M2_F2 %>% 
  mutate(
    sc_debut = factor(
      sc_debut,
      levels = c(1, 2, 999),
      labels = c("CP", "CE1", "Autre")),
    
    )

M2_F4 <- M2_F2 %>% 
  mutate(sc_debut = as.integer(sc_debut))

M2_F5 <- M2_F4 %>% 
  mutate(
    sc_debut = factor(
      sc_debut,
      levels = c(1, 2, 999),
      labels = c("CP", "CE1", "Autre")),
    
  )

M2_F6 <- M2_F4 %>% 
  mutate(sc_debut = factor(sc_debut))


M2_docu <- M2_F2 %>% 
  mutate(
    sc_debut = labelled(
      sc_debut,
      labels = c("CP"=1, "CE1"=2, "Autre"=999),
      label = "SC-01: Par quelle classe avez-vous débuté votre scolarité (en dehors de la maternelle)?")
  ) %>% 
  set_variable_labels(
    sc_debut_autre = "SC-01: Autre, précisez"
  )

M2_app <- M2_docu %>% 
  mutate(sc_debut = to_factor(sc_debut, levels = "values"))






