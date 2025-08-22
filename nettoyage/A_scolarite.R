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

## 1.1 rename ####
# Vecteurs des noms de colonne qui seront manipulés de la même manière
cols_fa <- M2_F0 %>% 
  select(all_of(starts_with("fa_")),
         -fa_an_diplome) %>% 
  colnames()

cols_ps <- M2_F0 %>% 
  select(all_of(starts_with("ps_")),
         -ps_an_debut) %>% 
  colnames()

# Appliquer les changements de noms
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
  ) %>% 
  mutate(
    id_age_cat_3 = case_when(
      id_age_cat_2 == "Adolescent" ~ "Moins de 18 ans",
      TRUE ~ id_age_cat_3
    )
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

## 1.3 Explorer missing values for identity cols ####
M1_identity <- M2_F2 %>% 
  select(starts_with("id_"))

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

# renommer les colonnes et transformer les "" characters en NAs
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

## 2.2 Retirer doublons ####
# Ne garder que les colonnes sur le redoublement et les quelques variables
# pour permettre la liaison avec la base générale sur la scolarité.
# Je retire aussi id_age_cat pcq elle correspond déjà à id_age_cat_2
rdb_F0 <- M2_rdb_F1 %>% 
  select(id_anonymat, id_link, id_date_creation, setdiff(names(.), names(M2_F2)), -id_age_cat) %>% 
  rename("sc_redoubl" = "sc_rdb_redoubl") # Je renomme aussi cette variable 
                                          # avec le même nom que dans M2_F2
# Je pourrais renommer cette variable plus tôt, je ne sais pas encore ce que 
# va donner ce changement. 

doublons <- rdb_F0 %>% 
  filter(is.na(id_date_creation))

identity_main <- M1_identity %>% 
  filter(id_link %in% doublons$id_link) %>% 
  select(id_anonymat, id_link, id_date_creation)

# Ce sont les données du redoublement renseignées lors de la connexion avec
# l'identifiant du doublon
doublons_clean <- left_join(
  doublons %>% 
    select(-id_anonymat, -id_date_creation),
  identity_main,
  by = "id_link",
  relationship = "many-to-many"
)

# Ici, ce sont les données du redoublement renseignées lors de la connexion
# avec l'identifiant de l'identité
identity_rdb <- rdb_F0 %>% 
  filter(id_link %in% doublons$id_link &
           !is.na(id_date_creation))

# Regrouper ces deux bases de rensignements sur le redoublement pour repérer
# les doubles renseignements d'un même redoublement
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
  mutate(sc_redoubl = 1,
         sc_rdb_cat = "sc_02_rdb_02"
         )
# Liste des identifiants à retirer avant de joindre la base corrigée
ids_TPO <- union(unique(together_c$id_link), unique(together_c$id_anonymat))

# Joindre les deux bases
rdb_F1 <- bind_rows(
  rdb_F0 %>% 
    filter(!id_anonymat %in% ids_TPO,
           !is.na(sc_rdb_date_creation)),
  together_c
)

## 2.3 Corriger valeurs manquantes ####

rcd_year <- function(df, borne, var_year){
  df %>% 
    mutate(
      {{var_year}} := as.integer(case_when(
        {{borne}} == 1 & is.na({{var_year}}) ~ 5555L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 7777L,       # Pas concerné
        is.na({{borne}}) ~ NA_integer_,               # Pas participé
        TRUE ~ as.integer({{var_year}})                           # Réponse renseignée
      )
    ))
}

rcd_vars <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
        {{borne}} == 1 & is.na(.x) ~ 555L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 777L,       # Pas concerné
        is.na({{borne}}) ~ NA_integer_,               # Pas participé
        TRUE ~ as.integer(.x)                           # Réponse renseignée
        ))
        )
      )
}

rcd_chr <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 1 & is.na(.x) ~ "Non renseigné", # Concerné, mais pas répondu
          {{borne}} == 0 | {{borne}} > 1 ~ "Non concerné e",       # Pas concerné
          is.na({{borne}}) ~ NA_character_,               # Pas participé
          TRUE ~ as.character(.x)                           # Réponse renseignée
        ))
      )
    )
}

# Appliquer les fonctions pour recoder les valeurs manquantes avant le pivot wide
# La raison pour laquelle je le fais d'abord ici, c'est que c'est encore une ligne
# par événement. Plus tard ce sera un autre code pour la base où c'est une ligne par 
# personne.
rdb_F2 <- rdb_F1 %>% 
  group_by(id_anonymat) %>% 
  rcd_year(sc_redoubl, an) %>% 
  rcd_vars(sc_redoubl, c(cause, classe)) %>%
  rcd_chr(sc_redoubl, classe_cat) %>% 
  ungroup()

## 2.4 Ajout variables pour préparer pivot ####
rdb_F3 <- rdb_F2 %>% 
  group_by(id_anonymat) %>% 
  arrange(classe, an) %>% 
  mutate(sc_rdb_nb = n(),
         rang_sc_rdb = case_when(
           sc_rdb_nb == 1 ~ "sc_rdb01",
           TRUE ~ paste0("sc_rdb", sprintf("%02d", row_number()))
         )) %>% 
  relocate(id_anonymat, sc_rdb_nb, rang_sc_rdb, an, classe) %>% 
  ungroup()

# Variables qui seront en plusieurs colonnes
vars_wider <- rdb_F3 %>% 
  select(an, cause, classe, classe_autre, classe_cat) %>% 
  colnames()

# Fonction pour recoder les variables qui seront wide. La raison pour laquelle
# il faut des fonctions différentes c'est parce qu'il y a une nouvelle sorte de
# NA. C'est dans le cas où une personne a connu un événement renouvelable, mais 
# pas autant de fois que celui qui en a renseigné le plus. 
rcd_yearsW <- function(df, borne, vars_year){
  df %>% 
    mutate(
      across(
        {{vars_year}},
        ~ as.integer(case_when(
          {{borne}} == 1 & is.na(.x) ~ 4444L, # Concerné, mais pas plus de répétitions
          {{borne}} == 0 | {{borne}} > 1 ~ 777L,       # Pas concerné
          is.na({{borne}}) ~ NA_integer_,               # Pas participé
          TRUE ~ as.integer(.x)                           # Réponse renseignée
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
          {{borne}} == 1 & is.na(.x) ~ 444L, # Concerné, mais pas répondu
          {{borne}} == 0 | {{borne}} > 1 ~ 777L,       # Pas concerné
          is.na({{borne}}) ~ NA_integer_,               # Pas participé
          TRUE ~ as.integer(.x)                           # Réponse renseignée
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
          {{borne}} == 1 & is.na(.x) ~ "444-Non concerné", # Concerné, mais pas de répétition
          {{borne}} == 0 | {{borne}} > 1 ~ "777-Non concerné",       # Pas concerné
          is.na({{borne}}) ~ NA_character_,               # Pas participé
          TRUE ~ as.character(.x)                           # Réponse renseignée
        ))
      )
    )
}

# On pivote ensuite en wide en appliquant les nouveaux noms de variables
rdb_W1 <- rdb_F3 %>% 
  pivot_wider(
    names_from = rang_sc_rdb,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_rdb}_{.value}"
  )

# on extrait ensuite les différentes sortes de variables qui seront recodées à
# l'aide des différentes fonctions
vars_years <- rdb_W1 %>% 
  select(ends_with("_an")) %>% 
  colnames()

vars_rcd <- rdb_W1 %>% 
  select(ends_with("_classe"),
         ends_with("_cause")) %>% 
  colnames()

vars_chr <- rdb_W1 %>% 
  select(ends_with("classe_cat")) %>% 
  colnames()

rdb_W2 <- rdb_W1 %>% 
  rcd_yearsW(sc_redoubl, all_of(vars_years)) %>% 
  rcd_varsW(sc_redoubl, all_of(vars_rcd)) %>% 
  rcd_chrsW(sc_redoubl, all_of(vars_chr))
  
# Maintenant, la base rdb_W2 est propre et bien recodée. Il faut la joindre 
# au reste des données sur le parcours scolaire pour correctement renseigner 
# valeurs manquantes.
