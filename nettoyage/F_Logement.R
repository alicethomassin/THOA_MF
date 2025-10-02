# Pour la manipulation des données
library(tidyverse)
library(questionr)
library(summarytools)
library(naniar)

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
# Importer la base
M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")

# 4. LOGEMENT ####
M5_F0 <- haven::read_sas("../raw_data/gb_ddb_id_01_lo_05.sas7bdat")

# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P5_F0 <- M5_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation, id_lieu_nais, id_dep_nais,
         setdiff(names(.), names(M2_F0)), -fa_amenag)

## 4.1 Rename cols ####
vars_sub_la <- P5_F0 %>% 
  select(all_of(starts_with("la_"))) %>% 
  colnames()

P5_F1 <- P5_F0 %>% 
  rename_with(~ gsub("^la_", "lo_", .x), all_of(vars_sub_la)) %>% 
  rename("lo_commentaires" = "commentaires_loge",
         "lo_dep_situ_centre1" = "dep_la_situ_centre1",
         "lo_dep_centre1" = "dep_centre1") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

## 4.2 Corriger doublons ####
doublons <- P5_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- P5_F1 %>% 
  filter(id_link %in% list_doublons,
         !is.na(id_date_creation)) %>% 
  relocate(id_link) %>% 
  arrange(id_link)

vars_identity <- identity %>% 
  select(all_of(starts_with("id_")), lo_dep_centre1, lo_dep_situ_centre1) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(P5_F1), vars_identity))

clean <- full_join(
  identity %>% 
    select(all_of(vars_identity)),
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(list_doublons, clean$id_anonymat)

P5_F2 <- bind_rows(
  P5_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>% 
  arrange(id_anonymat) %>% 
  mutate(
    id_lo_cat = case_when(
      id_lo_cat == "id_01" & !is.na(lo_date_creation) ~ "id_01_lo_05",
      TRUE ~ id_lo_cat
    )
  )

# CLean up
rm(list = c("clean",
            "doublons",
            "identity",
            "ids_TPO",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

## 4.3 Corriger valeurs manquantes ####
### 4.3.1 Réordonner la base ####
P5_F3 <- P5_F2 %>% 
  relocate(
    id_date_creation,
    id_anonymat, 
    id_link,
    lo_type,
    lo_an_logement,
    lo_situ_logement,
    lo_situ_dep,
    id_dep_nais,
    lo_situ_pays,
    id_lieu_nais,
    lo_dep_centre1,
    lo_dep_situ_centre1,
    lo_type_logement,
    lo_type_logement_autre,
    lo_occup_logement,
    lo_occup_logement_autre,
    lo_occup_cause_1:lo_occup_cause_999,
    lo_occup_cause_autre,
    lo_amenag_logement,
    lo_amenag_logement_precis,
    lo_emprunt_1:lo_emprunt_6,
    lo_emprunt_precis,
    lo_demenag,
    lo_demenag_cause_1:lo_demenag_cause_999,
    lo_demenag_cause_autre,
    lo_commentaires,
    lo_date_creation
  )
### 4.3.2 Créer variables ####
rcd_qcm2 <- function(df, borne, vars){
  df %>%
    mutate(
      all_na_vars = if_all({{vars}}, is.na)
    ) %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          is.na({{borne}}) ~ NA_integer_,
          !is.na({{borne}}) & all_na_vars ~ 555L,
          {{borne}} == "P" & is.na(.x) ~ 0L,
          TRUE ~ as.integer(.x)                            
        ))
      )
    ) %>% 
    select(-all_na_vars)
}

rcd_vars <- function(df, vars, type){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          is.na(.x) & !is.na({{type}}) ~ 555L,
          TRUE ~ as.integer(.x)
        ))
      )
    )
}

rcd_years <- function(df, vars, type){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          is.na(.x) & !is.na({{type}}) ~ 5555L,
          TRUE ~ as.integer(.x)
        ))
      )
    )
}

### 4.3.3 Sélectionner variables des QCM ####
vars_Q5 <- P5_F3 %>% 
  select(lo_occup_cause_1:lo_occup_cause_999) %>% 
  colnames()

vars_Q8 <- P5_F3 %>% 
  select(lo_emprunt_1:lo_emprunt_6) %>% 
  colnames()

vars_Q11 <- P5_F3 %>% 
  select(lo_demenag_cause_1:lo_demenag_cause_999) %>% 
  colnames()

### 4.3.4 Recoder ####
P5_F4 <- P5_F3 %>% 
  group_by(id_anonymat) %>% 
  rcd_vars(lo_situ_logement, lo_type) %>%
  rcd_years(lo_an_logement, lo_type) %>% 
  rcd_vars(lo_situ_dep, lo_type) %>% 
  mutate(
    lo_situ_pays = case_when(
      (is.na(lo_situ_pays) & !is.na(lo_situ_logement)) ~ lo_situ_logement,
      is.na(lo_type) ~ NA_integer_,
      TRUE ~ lo_situ_pays
    )
  ) %>% 
  rcd_vars(c(lo_type_logement, lo_occup_logement), lo_type) %>% 
  rcd_qcm2(lo_type, all_of(vars_Q5)) %>% 
  rcd_qcm2(lo_type, all_of(vars_Q8)) %>% 
  rcd_vars(c(lo_amenag_logement, lo_demenag), lo_type) %>% 
  rcd_qcm2(lo_type, all_of(vars_Q11)) %>% 
  ungroup() %>% 
  mutate(
    lo_dep_centre1 = as.integer(lo_dep_centre1),
    lo_situ_pays = as.integer(lo_situ_pays)
  )

## 4.4 Étiquettes ####
P5_F5 <- P5_F4 %>% 
  {
    labels_Q5 <- set_names(rep("LO_05", length(vars_Q5)), vars_Q5)
    set_variable_labels(., !!!labels_Q5)
  } %>%
  {
    labels_Q8 <- set_names(rep("LO_08", length(vars_Q8)), vars_Q8)
    set_variable_labels(., !!!labels_Q8)
  } %>%
  {
    labels_Q11 <- set_names(rep("LO_11", length(vars_Q11)), vars_Q11)
    set_variable_labels(., !!!labels_Q11)
  } %>%
  set_variable_labels(
    id_date_creation = "id_A",
    id_anonymat = "id_B",
    id_link = "id_C",
    id_lieu_nais = "ID_06",
    id_dep_nais = "ID_06",
    lo_an_logement = "LO_01",
    lo_situ_logement = "LO_02",
    lo_situ_dep = "LO_02",
    lo_situ_pays = "LO_02",
    lo_dep_centre1 = "lo_A",
    lo_dep_situ_centre1 = "lo_B",
    lo_type_logement = "LO_03",
    lo_type_logement_autre = "lo_03",
    lo_occup_logement = "LO_04",
    lo_occup_logement_autre = "lo_04",
    lo_occup_cause_autre = "lo_05",
    lo_amenag_logement = "LO_06",
    lo_amenag_logement_precis = "LO_07",
    lo_emprunt_precis = "LO_09",
    lo_demenag = "LO_10",
    lo_demenag_cause_autre = "lo_11",
    lo_date_creation = "lo_C",
    id_lo_cat = "lo_D"
  )

# Enregistrer ####
THOA_logement <- P5_F5
save(THOA_logement, file = "../clean_data/thoa_logement_etiquettes.RData", compress = F)
