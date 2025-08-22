# Pour la manipulation des données
library(tidyverse)
library(haven)
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
M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")

## 1.1 Renommer ####
# Vecteurs des noms de colonne qui seront manipulés de la même manière
cols_fa <- M2_F0 %>% 
  select(all_of(starts_with("fa_")),
         -fa_an_diplome) %>% 
  colnames()

cols_ps <- M2_F0 %>% 
  select(all_of(starts_with("ps_")),
         -ps_an_debut) %>% 
  colnames()

# Appliquer les changements de noms et ajout id_link
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

# Clean up
rm(list = c("cols_fa",
            "cols_ps"))

## 1.2 Corriger doublons ####
doublons <- M2_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

# Fonction pour trouver les lignes qui comprennent l'identité (pas utilisée pour l'instant)
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
  ) %>% 
  mutate(
    id_age_cat_3 = case_when(
      id_age_cat_2 == "Adolescent" ~ "Moins de 18 ans",
      TRUE ~ id_age_cat_3
    )
  ) %>% 
  arrange(id_anonymat)

# CLean up
rm(list = c("clean",
            "doublons",
            "identity",
            "ids_TPO",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

### Base identité ####
M1_identity <- M2_F2 %>% 
  select(starts_with("id_"))

# 2. REDOUBLEMENT ####
M2_rdb_F0 <- read_sas("../raw_data/gb_ddb_sc_02_rdb_02.sas7bdat")

## 2.1 Réduire et renommer #### 
# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P2_rdb_F0 <- M2_rdb_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation,
         setdiff(names(.), names(M2_F0)), -id_age_cat)
# Je me réfère à la base M2_F0 brute pour la sélection des colonnes puisque 
# ces deux bases utilisent l'ancienne nomenclature pour nommer les variables

# Renommer les variables
cols_rdb <- P2_rdb_F0 %>% 
  select(all_of(starts_with("rdb_")), -rdb_ps_redoubl) %>% 
  colnames()

cols_rd <- P2_rdb_F0 %>% 
  select(all_of(starts_with("rd_"))) %>% 
  colnames()

P2_rdb_F1 <- P2_rdb_F0 %>% 
  rename_with(~ gsub("^rdb_", "sc_rdb_", .x), all_of(cols_rdb)) %>%
  rename_with(~ gsub("^rd_", "", .x), all_of(cols_rd)) %>% 
  rename("sc_rdb_redoubl" = "rdb_ps_redoubl")

## 2.2 Corriger doublons ####
doublons <- P2_rdb_F1 %>% 
  filter(is.na(id_date_creation))

# Puisque je ne cherche que les id_anonymat, je vais chercher dans la base
# M1_identity. Si je faisais la recherche locale, il me manquerait des personnes.
identity_main <- M1_identity %>% 
  filter(id_link %in% doublons$id_link) %>% 
  select(id_anonymat, id_link, id_date_creation)

# Ce sont les données du redoublement renseignées lors de la connexion avec
# l'identifiant du doublon
doublons_clean <- left_join(
  identity_main,
  doublons %>% 
    select(-id_anonymat, -id_date_creation),
  by = "id_link",
  relationship = "many-to-many"
)

# Ici, ce sont les données du redoublement renseignées lors de la connexion
# avec l'identifiant de l'identité
identity_rdb <- P2_rdb_F1 %>% 
  filter(id_link %in% doublons$id_link &
           !is.na(id_date_creation))

# Regrouper ces deux bases de renseignements sur le redoublement pour repérer
# les doubles renseignements d'un même redoublement scolaire
together <- bind_rows(
  doublons_clean %>% 
    mutate(source = "db"),
  identity_rdb %>% 
    mutate(source = "id")
) %>%  
  group_by(id_anonymat, an) %>% 
  mutate(n = n()) %>%
  arrange(n, id_anonymat, an) %>%
  relocate(id_anonymat, n, an, classe, source, id_date_creation, sc_rdb_date_creation) %>% 
  ungroup()

# Retirer à la main, les événements doubles et modifier les modalités de certaines
# variables qui ont été générées automatiquement
together_c <- together %>% 
  slice(c(2,
          3,
          5,
          6,
          8,
          10,
          12,
          14,
          16,
          18)) %>% 
  select(-n, -source) %>% 
  mutate(
    sc_rdb_redoubl = 1,
    sc_rdb_cat = "sc_02_rdb_02"
  )
# Liste des identifiants à retirer avant de joindre la base corrigée
ids_TPO <- union(unique(together_c$id_link), unique(together_c$id_anonymat))

# Joindre les deux bases
P2_rdb_F2 <- bind_rows(
  P2_rdb_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  together_c
) %>% 
  mutate(
    sc_rdb_cor = case_when(
      sc_rdb_redoubl == 1 ~ 1,
      is.na(sc_rdb_redoubl) ~ 33
  ))
# Je conserve ceux qui n'ont pas participé, mais qui ont déclaré avoir
# eu un redoublement pour ajouter la variable sc_rdb_cor et lui attribuer des
# nouvelles modalités pour prendre en compte ces situations.

# Clean up
rm(list = c("together",
            "together_c",
            "ids_TPO",
            "doublons",
            "doublons_clean",
            "identity_main",
            "identity_rdb",
            "cols_rd",
            "cols_rdb"))

## 2.3 Préparer le pivot wider ####
P2_rdb_F3 <- P2_rdb_F2 %>% 
  group_by(id_anonymat) %>% 
  arrange(classe, an) %>% 
  mutate(
    sc_rdb_nb = case_when(
      sc_rdb_cor == 33 ~ 333,
      sc_rdb_cor == 1 ~ n()),
    rang_sc_rdb = case_when(
      sc_rdb_nb == 1 ~ "sc_rdb01",
      TRUE ~ paste0("sc_rdb", sprintf("%02d", row_number()))
    )) %>% 
  relocate(id_anonymat, sc_rdb_nb, rang_sc_rdb, an, classe, sc_rdb_cor) %>% 
  ungroup() %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) # Besoin de transformer les characters en NA pour l'utilisation des fonctions

# Variables qui seront en plusieurs colonnes
vars_wider <- P2_rdb_F3 %>% 
  select(an, cause, classe, classe_autre, classe_cat) %>% 
  colnames()

## 2.4 Recoder modalités (L) ####

rcd_yearL <- function(df, borne, var_year){
  df %>% 
    mutate(
      {{var_year}} := as.integer(case_when(
        {{borne}} == 33 & is.na({{var_year}}) ~ 3333L, # Concerné, pas répondu au module
        {{borne}} == 1 & is.na({{var_year}}) ~ 5555L,  # Concerné, mais pas répondu
        ({{borne}} == 0 | {{borne}} > 1) & is.na({{var_year}})  ~ 7777L,        # Pas concerné
        is.na({{borne}}) ~ NA_integer_,                # Pas participé
        TRUE ~ as.integer({{var_year}})                # Réponse renseignée
      )
      ))
}

rcd_varsL <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 33 & is.na(.x) ~ 333L,      # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ 555L,       # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ 777L,   # Pas concerné
          is.na({{borne}}) ~ NA_integer_,          # Pas participé
          TRUE ~ as.integer(.x)                    # Réponse renseignée
        ))
      )
    )
}

rcd_chrsL <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 33 & is.na(.x) ~ "Non renseigné (333)",     # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ "Non renseigné (555)",      # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ "Non concernée (777)",  # Pas concerné
          is.na({{borne}}) ~ NA_character_,                      # Pas participé
          TRUE ~ as.character(.x)                                # Réponse renseignée
        ))
      )
    )
}

# Appliquer les fonctions pour recoder les valeurs manquantes avant le pivot wide
# La raison pour laquelle je le fais d'abord ici, c'est que c'est encore une ligne
# par événement. Plus tard ce sera un autre code pour la base où c'est une ligne par 
# personne. Et il faut tout de suite indiquer les situations où les données sont manquantes (555)
# parce que la personne a sauté cette question, des situations où les données sont
# manquantes (444) parce que la personne n'a pas eu d'autre évents renouvelables
P2_rdb_F4 <- P2_rdb_F3 %>% 
  group_by(id_anonymat) %>% 
  rcd_yearL(sc_rdb_cor, an) %>% 
  rcd_varsL(sc_rdb_cor, c(cause, classe)) %>%
  rcd_chrsL(sc_rdb_cor, classe_cat) %>% 
  ungroup()

## 2.5 Pivot ####
# On pivote ensuite en wide en appliquant les nouveaux noms de variables
P2_rdb_W1 <- P2_rdb_F4 %>% 
  pivot_wider(
    names_from = rang_sc_rdb,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_rdb}_{.value}"
  )

## 2.6 Recoder modalités (W) ####

# Fonction pour recoder les variables qui seront wide. La raison pour laquelle
# il faut des fonctions différentes c'est parce qu'il y a une nouvelle sorte de
# NA = 444. C'est dans le cas où une personne a connu un événement renouvelable, mais 
# pas autant de fois que celui qui en a renseigné le plus. 
rcd_yearsW <- function(df, borne, vars_year){
  df %>% 
    mutate(
      across(
        {{vars_year}},
        ~ as.integer(case_when(
          {{borne}} == 33 & is.na(.x) ~ 3333L,    # Concerné, pas répondu module
          {{borne}} == 1 & is.na(.x) ~ 4444L,     # Concerné, mais pas plus de répétitions
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x)  ~ 7777L, # Pas concerné
          is.na({{borne}}) ~ NA_integer_,         # Pas participé
          TRUE ~ as.integer(.x)                   # Réponse renseignée
        ))
      )
    )
}

rcd_varsW <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 33 & is.na(.x) ~ 333L,    # Concerné, pas répondu module
          {{borne}} == 1 & is.na(.x) ~ 444L,     # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ 777L, # Pas concerné
          is.na({{borne}}) ~ NA_integer_,        # Pas participé
          TRUE ~ as.integer(.x)                  # Réponse renseignée
        ))
      )
    )
}

rcd_chrsW <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 33 & is.na(.x) ~ "Non renseigné (333)",    # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ "Non concerné (444)",      # Concerné, mais pas de répétition
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ "Non concerné (777)",  # Pas concerné
          is.na({{borne}}) ~ NA_character_,                     # Pas participé
          TRUE ~ as.character(.x)                               # Réponse renseignée
        ))
      )
    )
}

# on extrait ensuite les différentes sortes de variables qui seront recodées à
# l'aide des différentes fonctions
vars_years <- P2_rdb_W1 %>% 
  select(ends_with("_an"),
         -contains("01")) %>% 
  colnames()

vars_rcd <- P2_rdb_W1 %>% 
  select(ends_with("_classe"),
         ends_with("_cause"),
         -contains("01")) %>% 
  colnames()

vars_chr <- P2_rdb_W1 %>% 
  select(ends_with("classe_cat"),
         -contains("01")) %>% 
  colnames()

P2_rdb_W2 <- P2_rdb_W1 %>% 
  rcd_yearsW(sc_rdb_cor, all_of(vars_years)) %>% 
  rcd_varsW(sc_rdb_cor, all_of(vars_rcd)) %>% 
  rcd_chrsW(sc_rdb_cor, all_of(vars_chr))

# Clean up
rm(list = c("vars_chr",
            "vars_rcd",
            "vars_wider",
            "vars_years"))
###

## 2.7 M2_F3 Joindre à la base scolarité ####

common_vars <- intersect(names(M2_F2), names(P2_rdb_W2))

M2_F3 <- full_join(
  M2_F2,
  P2_rdb_W2,
  by = common_vars) %>% 
  arrange(id_anonymat)















