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

vars_int <- M2_F2 %>% 
  select(
    id_centre1, id_centre2, id_centre3, id_dep_nais, id_lieu_nais, id_sexe,
    id_age, id_tab_db, sc_diplome_an, sc_diplome, sc_fin_etudes, sc_formation,
    sc_debut_an, sc_debut, sc_interromp, sc_redoubl, sc_debut_corr, 
    sc_debut_age, sc_diplome_age, starts_with("sc_plan"), -sc_plan_notification_autre) %>% 
  colnames()

vars_choix_multiple <- M2_F2 %>% 
  select(starts_with("sc_plan_ets"),
         starts_with("sc_plan_notifi"),
         -sc_plan_notification_autre) %>% 
  colnames()

vars_ets <- M2_F2 %>% 
  select(starts_with("sc_plan_ets")) %>% 
  names()

classe_index <- as.integer(str_extract(vars_ets, "\\d+$"))

recode_year1 <- function(df, borne, var_year){
  # Dans le cas où il y a une question borne
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{borne}} == 1 & is.na({{var_year}}) ~ 9999L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 7777L,       # Pas concerné
        is.na({{borne}}) ~ NA_integer_,               # Pas participé
        TRUE ~ {{var_year}}                           # Réponse renseignée
      )
    )
}

recode_month1 <- function(df, borne, var_month){
  
  df %>% 
    mutate(
      {{var_month}} := case_when(
        {{borne}} == 1 & is.na({{var_month}}) ~ 99L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 77L,        # Pas concerné
        is.na({{borne}}) ~ NA_integer_,              # Pas participé
        TRUE ~ {{var_month}}                         # Réponse renseignée
    ))
}

recode_year2 <- function(df, borne_module, var_year){
  # Dans la cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{borne_module}} == "P" & is.na({{var_year}}) ~ 9999L,       # Concerné, mais pas répondu
        {{borne_module}} != "P" & is.na({{var_year}}) ~ NA_integer_, # Pas participé
        TRUE ~ {{var_year}}                                          # Réponse renseignée
      )
    )
}

recode_month2 <- function(df, borne_module, var_month){
  # Dans la cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_month}} := case_when(
        {{borne_module}} == "P" & is.na({{var_month}}) ~ 99L,         # Concerné, mais pas répondu
        {{borne_module}} != "P" & is.na({{var_month}}) ~ NA_integer_, # Pas participé
        TRUE ~ {{var_month}}                                          # Réponse renseignée
      )
    )
}


recode_qborne <- function(df, borne, var_miss){
  # Pour les questions bornées au sein d'un module
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{borne}} == 1 & is.na({{var_miss}}) ~ 555L,  # Concerné, pas répondu
        {{borne}} == 0  & is.na({{var_miss}}) |
          {{borne}} > 1 & is.na({{var_miss}}) ~ 777L, # Pas concerné, pas répondu
        is.na({{borne}}) ~ NA_integer_,               # Pas participé
        TRUE ~ {{var_miss}}                           # Réponse renseignée
      )
    )
}

recode_qnorm <- function(df, big_borne, var_miss){
  # Pour les questions d'un module hors bornes
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{big_borne}} == "P" & is.na({{var_miss}}) ~ 555L,
        {{big_borne}} != "P" & is.na({{var_miss}}) ~ NA_integer_,
        TRUE ~ {{var_miss}}
      )
    )
}
recode_qcm <- function(df, borne, list_vars){
  
  df %>% 
    mutate(
      across(
        .cols = all_of( {{list_vars}} ),
        .fns = ~ case_when(
          {{borne}} == 1 & is.na(.) ~ 0L,
          {{borne}} == 0 | {{borne}} > 1 ~ 777L,
          is.na({{borne}}) ~ NA_integer_,
          TRUE ~ .
        )
      )
    )
  
}

M2_F3 <- M2_F2 %>%
  mutate(
    across(
      .cols = all_of(vars_int),
      .fns = ~ as.integer(.)
    )) %>%
  mutate(
    sc_plan_classe = case_when(
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_,
      sc_plan == 1 ~ pmap_int(
        .l = select(., all_of(vars_ets)),
        .f = function(...) {
          row <- c(...)  # transforme les arguments en vecteur ligne
          first_class <- classe_index[which(row == 1)]
          if (length(first_class) == 0) 0 else min(first_class)
        }
      )
    ),
    sc_plan_nb = case_when(
      sc_plan == 1 ~ as.integer(rowSums(select(., all_of(vars_ets)) == 1, na.rm = T)),
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_
    )) %>% 
  recode_year1(sc_plan, sc_plan_an) %>%
  recode_qcm(sc_plan, vars_choix_multiple) %>% 
  recode_qnorm(sc_type, sc_plan_demande)
  
verif <- M2_F3 %>% 
  relocate(sc_type, sc_plan, sc_plan_classe, sc_plan_nb, sc_plan_an, sc_plan_demande, all_of(vars_choix_multiple))


sc_P <- M2_F3 %>% 
  filter(is.na(sc_type)) %>% 
  relocate(sc_type, sc_debut, sc_debut_an, sc_redoubl, sc_interromp, sc_plan, sc_plan_an, sc_plan_demande, sc_fin_etudes, sc_formation, sc_diplome, sc_diplome_an)


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