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
# Importer la base
M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")

## 1.1 Renommer variables ####
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

# M1_identity
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
      is.na(sc_rdb_redoubl) ~ 333
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

## 2.3 Recoder NAs (ML) ####
# NOUVEAU TESTE POUR TOUT RECODER EN MEME TEMPS LOL ####
all_ids <- M2_F2 %>%
  select(id_anonymat, id_link, id_date_creation, sc_date_creation, sc_type, sc_id_cat, sc_redoubl)

common_vars <- intersect(names(all_ids), names(P2_rdb_F2))

T2_rdb_F0 <- full_join(
  P2_rdb_F2,
  all_ids,
  by = common_vars,
  relationship = "many-to-many"
) %>% 
  arrange(id_anonymat)

T2_rdb_F1 <- T2_rdb_F0 %>% 
  mutate(
    sc_rdb_cor = case_when(
      sc_redoubl == 0 & is.na(sc_rdb_cor) ~ 0,
      !is.na(sc_type) & is.na(sc_redoubl) & is.na(sc_rdb_cor) ~ 555,
      TRUE ~ sc_rdb_cor
    )
  ) %>% 
  group_by(id_anonymat) %>% 
  arrange(classe, an) %>% 
  mutate(
    sc_rdb_nb = case_when(
      sc_rdb_cor == 333 ~ 333,
      sc_rdb_cor == 555 ~ 555,
      sc_rdb_cor == 0 ~ 0,
      is.na(sc_rdb_cor) ~ NA_integer_,
      sc_rdb_cor == 1 ~ n()
    ),
    rang_sc_rdb = paste0("sc_rdb", sprintf("%02d", row_number())
    )
  )%>% 
  relocate(id_anonymat, sc_rdb_nb, rang_sc_rdb, an, classe, sc_rdb_cor) %>% 
  ungroup() %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

rcd_years_ML <- function(df, borne, vars){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 3333L,                   # Concerné, pas répondu module
          {{borne}} == 1 & is.na(.x) ~ 5555L,                    # Concerné, mais pas répondu à la question
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x)  ~ 7777L, # Pas concerné
          TRUE ~ as.integer(.x)                                  # Réponse renseignée
        ))
      )
    )
}

rcd_nor_ML <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 333L,                  # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ 555L,                   # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ 777L, # Pas concerné
          TRUE ~ as.integer(.x)                                # Réponse renseignée
        ))
      )
    )
}

rcd_chrs_ML <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 333 & is.na(.x) ~ "Non renseigné (333)",                  # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ "Non renseigné (555)",                   # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ "Non concernée (777)", # Pas concerné
          TRUE ~ as.character(.x)                                               # Réponse renseignée
        ))
      )
    )
}

T2_rdb_F2 <- T2_rdb_F1 %>% 
  group_by(id_anonymat) %>% 
  rcd_years_ML(sc_rdb_cor, an) %>% 
  rcd_nor_ML(sc_rdb_cor, c(classe, cause)) %>% 
  rcd_chrs_ML(sc_rdb_cor, classe_cat) %>% 
  ungroup()

## 2.4 Pivot Wide ####
# Variables qui seront en plusieurs colonnes
vars_wider <- T2_rdb_F2 %>% 
  select(an, cause, classe, classe_autre, classe_cat) %>% 
  colnames()

# On pivote ensuite en wide en appliquant les nouveaux noms de variables
P2_rdb_W1 <- T2_rdb_F2 %>% 
  pivot_wider(
    names_from = rang_sc_rdb,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_rdb}_{.value}"
  )

## 2.5 Recoder NAs (MW) ####
rcd_years_MW <- function(df, borne, vars){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 3333L,    # Concerné, pas répondu module
          {{borne}} == 1 & is.na(.x) ~ 4444L,     # Concerné, mais pas plus de répétitions
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x)  ~ 7777L, # Pas concerné/Pas répondu à la question borne
          is.na({{borne}}) & is.na(.x) ~ NA_integer_,         # Pas participé
          TRUE ~ as.integer(.x)                   # Réponse renseignée
        ))
      )
    )
}

rcd_nor_MW <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 333L,    # Concerné, pas répondu module
          {{borne}} == 1 & is.na(.x) ~ 444L,     # Concerné, mais pas répondu
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ 777L, # Pas concerné
          TRUE ~ as.integer(.x)                  # Réponse renseignée
        ))
      )
    )
}

rcd_chr_MW <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 333 & is.na(.x) ~ "Non renseigné (333)",    # Concerné, pas répondu au module
          {{borne}} == 1 & is.na(.x) ~ "Non concerné (444)",      # Concerné, mais pas de répétition
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ "Non concerné (777)",  # Pas concerné
          is.na({{borne}}) & is.na(.x) ~ NA_character_,                     # Pas participé
          TRUE ~ as.character(.x)                               # Réponse renseignée
        ))
      )
    )
}

vars_years_MW <- P2_rdb_W1 %>% 
  select(ends_with("_an")) %>% 
  colnames()

vars_nor_MW <- P2_rdb_W1 %>% 
  select(ends_with("_cause"),
         ends_with("_classe")) %>% 
  colnames()

vars_chr_MW <- P2_rdb_W1 %>% 
  select(ends_with("_classe_cat")) %>% 
  colnames()

P2_rdb_W2 <- P2_rdb_W1 %>% 
  group_by(id_anonymat) %>% 
  rcd_years_MW(sc_rdb_cor, all_of(vars_years_MW)) %>% 
  rcd_nor_MW(sc_rdb_cor, all_of(vars_nor_MW)) %>% 
  rcd_chr_MW(sc_rdb_cor, all_of(vars_chr_MW))

## 2.6 Joindre Scolarité ####
common_vars <- intersect(names(M2_F2), names(P2_rdb_W2))

M2_F3 <- left_join(
  M2_F2,
  P2_rdb_W2,
  by = common_vars
)

## 2.7 Recoder NAs (B) ####
rcd_qcm <- function(df, borne, vars){
  df %>%
    mutate(
      all_na_vars = if_all({{vars}}, is.na)
    ) %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          ({{borne}}) == 333 & is.na(.x) ~ 333L,
          ({{borne}}) == 1 & all_na_vars ~ 555L,
          ({{borne}}) == 555 & all_na_vars ~ 777L,
          ({{borne}} == 0 | {{borne}} > 1) & is.na(.x) ~ 777L, # Pas répondu au QCM
          {{borne}} == 1 & is.na(.x) ~ 0L,                     # Pas sélection dans qcm          # Pas participé
          TRUE ~ as.integer(.x)                                # Réponse renseignée
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

vars_notif <- M2_F3 %>% 
  select(starts_with("sc_plan_not"),
         -sc_plan_notification_autre) %>% 
  colnames()

vars_ets <- M2_F3 %>% 
  select(starts_with("sc_plan_ets")) %>% 
  names()

classe_index <- as.integer(str_extract(vars_ets, "\\d+$"))

M2_F4 <- M2_F3 %>% 
  group_by(id_anonymat) %>% 
  mutate(
    sc_plan = case_when(
      !is.na(sc_type) & is.na(sc_plan) ~ 555,
      TRUE ~ sc_plan
    )
  ) %>%
  ungroup() %>% 
  mutate(
    sc_plan_prem_classe = pmap_int(
      .l = select(., all_of(vars_ets)),
      .f = function(...) {
        row <- c(...)
        first_class <- classe_index[which(row == 1)]
        if (length(first_class) == 0) 0 else min(first_class)
      }
    ),
    sc_plan_nb = case_when(
      sc_plan == 1 ~ rowSums(select(., all_of(vars_ets)) == 1, na.rm = T),
      sc_plan == 0 ~ 0,
      sc_plan == 555 ~ 777L,
      is.na(sc_plan) ~ NA_integer_
    )
  ) %>% 
  group_by(id_anonymat) %>%
  rcd_qcm(sc_plan, all_of(vars_ets)) %>% 
  rcd_qcm(sc_plan, all_of(vars_notif)) %>%
  rcd_years_ML(sc_plan, sc_plan_an) %>%
  mutate(
    sc_plan_demande = case_when(
      sc_plan == 0 & is.na(sc_plan_demande) ~ 555L,
      sc_plan >= 1 & is.na(sc_plan_demande) ~ 777L,
      TRUE ~ sc_plan_demande
    )
  )

M2_F5 <- M2_F4 %>% 
  rcd_vars(c(sc_fin_etudes, sc_diplome), sc_type) %>% 
  mutate(
    sc_formation = case_when(
      sc_fin_etudes == 0 & is.na(sc_formation) ~ 555L,
      sc_fin_etudes >= 1 & is.na(sc_formation) ~ 777L,
      TRUE ~ sc_formation
    )
  ) %>% 
  rcd_years(sc_diplome_an, sc_type) %>% 
  relocate(id_anonymat, sc_type, sc_fin_etudes, sc_formation, sc_formation_autre, sc_diplome, sc_diplome_autre, sc_diplome_an)
  
# Clean up 
rm(list = c("common_vars",
            "vars_chr_MW",
            "vars_ets",
            "vars_nor_MW",
            "vars_notif",
            "vars_wider",
            "vars_years_MW",
            "all_ids"))
# 3. INTERRUPTIONS SCOLAIRES ####
M2_int_F0 <- read_sas("../raw_data/gb_ddb_sc_02_int_02.sas7bdat")

## 3.1 Réduire et renommer #### 
# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P2_int_F0 <- M2_int_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation,
         setdiff(names(.), names(M2_F0)), -id_age_cat)

cols_int <- P2_int_F0 %>% 
  select(all_of(starts_with("int_")), -int_ps_interromp) %>% 
  colnames()

cols_it <- P2_int_F0 %>% 
  select(starts_with("it_")) %>% 
  colnames()

P2_int_F1 <- P2_int_F0 %>% 
  rename_with(~ gsub("^int_", "sc_int_", .x), all_of(cols_int)) %>%
  rename_with(~ gsub("^it_", "", .x), all_of(cols_it)) %>%
  rename("sc_int_interromp" = "int_ps_interromp") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

rm(list = c("cols_int", "cols_it"))
## 3.2 Corriger doublons ####
doublons <- P2_int_F1 %>% 
  filter(is.na(id_date_creation))

# Puisque je ne cherche que les id_anonymat, je vais chercher dans la base
# M1_identity. Si je faisais la recherche locale, il me manquerait des personnes.
identity_main <- M1_identity %>% 
  filter(id_link %in% doublons$id_link) %>% 
  select(id_anonymat, id_link, id_date_creation)

# Ce sont les données de l'interruption renseignées lors de la connexion avec
# l'identifiant du doublon
doublons_clean <- left_join(
  identity_main,
  doublons %>% 
    select(-id_anonymat, -id_date_creation),
  by = "id_link",
  relationship = "many-to-many"
)

# Ici, ce sont les données de l'interruption renseignées lors de la connexion
# avec l'identifiant de l'identité
identity_rdb <- P2_int_F1 %>% 
  filter(id_link %in% doublons$id_link &
           !is.na(id_date_creation))

# Regrouper ces deux bases de renseignements sur les interruptions pour repérer
# les doubles renseignements d'une même interruption scolaire
together <- bind_rows(
  doublons_clean %>% 
    mutate(source = "db"),
  identity_rdb %>% 
    mutate(source = "id")
) %>%  
  group_by(id_anonymat, an) %>% 
  mutate(n = n()) %>%
  arrange(n, id_anonymat, an) %>%
  relocate(id_anonymat, n, an, mois, classe, source, id_date_creation, sc_int_date_creation) %>% 
  ungroup()

# Retirer à la main, les événements doubles et modifier les modalités de certaines
# variables qui ont été générées automatiquement
together_c <- together %>% 
  slice(c(1,
          2,3,
          4,
          5,
          7,9,
          11,
          12:19)) %>% 
  select(-n, -source) %>% 
  mutate(
    sc_int_interromp = 1,
    sc_int_cat = "sc_02_int_02"
  )
# Liste des identifiants à retirer avant de joindre la base corrigée
ids_TPO <- union(unique(together_c$id_link), unique(together_c$id_anonymat))

# Joindre les deux bases
P2_int_F2 <- bind_rows(
  P2_int_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  together_c
) %>% 
  mutate(
    sc_int_cor = case_when(
      sc_int_interromp == 1 ~ 1,
      is.na(sc_int_interromp) ~ 333
    ))
# Je conserve ceux qui n'ont pas participé, mais qui ont déclaré avoir
# eu un redoublement pour ajouter la variable sc_int_cor et lui attribuer des
# nouvelles modalités pour prendre en compte ces situations.
# Clean up
rm(list = c("together",
            "together_c",
            "ids_TPO",
            "doublons",
            "doublons_clean",
            "identity_main",
            "identity_rdb"))

## 3.3 Recoder NAs (ML) ####
all_ids <- M2_F5 %>%
  select(id_anonymat, id_link, id_date_creation, sc_date_creation, sc_type, sc_id_cat, sc_interromp)

common_vars <- intersect(names(all_ids), names(P2_int_F2))

T2_int_F0 <- full_join(
  P2_int_F2,
  all_ids,
  by = common_vars,
  relationship = "many-to-many"
) %>% 
  arrange(id_anonymat)

T2_int_F1 <- T2_int_F0 %>% 
  mutate(
    sc_int_cor = case_when(
      sc_interromp == 0 & is.na(sc_int_cor) ~ 0,
      !is.na(sc_type) & is.na(sc_interromp) & is.na(sc_int_cor) ~ 555,
      TRUE ~ sc_int_cor
    )
  ) %>% 
  group_by(id_anonymat) %>% 
  arrange(id_anonymat, desc(is.na(an)), an, classe, mois) %>% 
  mutate(
    sc_int_nb = case_when(
      sc_int_cor == 333 ~ 333,
      sc_int_cor == 555 ~ 555,
      sc_int_cor == 0 ~ 0,
      is.na(sc_int_cor) ~ NA_integer_,
      sc_int_cor == 1 ~ n()
    ),
    rang_sc_int = paste0("sc_int", sprintf("%02d", row_number())
    )
  )%>% 
  relocate(id_anonymat, sc_int_nb, rang_sc_int, classe, an, mois, sc_int_interromp, 
           sc_interromp, sc_int_cor,  sc_type, sc_date_creation, sc_int_date_creation) %>% 
  ungroup() %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

rcd_years2_ML <- function(df, borne, vars){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 3333L, # Concerné, pas répondu au module
          {{borne}} == 0 & is.na(.x) ~ 2222L,  # Équivalent du 777 mais au sein du module
          {{borne}} == 1 & is.na(.x) ~ 5555L, # Concerné, pas répondu à la question
          {{borne}} == 777 & is.na(.x) ~ 7777L, # Ceux qui n'ont jamais eu d'interruptions
          TRUE ~ as.integer(.x)
        ))
      )
    )
}

rcd_nor2_ML <- function(df, borne, vars){
  df %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          {{borne}} == 333 & is.na(.x) ~ 333L, # Concerné, pas répondu au module
          {{borne}} == 0 & is.na(.x) ~ 222L,  # Équivalent du 777 mais au sein du module
          {{borne}} == 1 & is.na(.x) ~ 555L, # Concerné, pas répondu à la question
          {{borne}} == 777 & is.na(.x) ~ 777L, # Ceux qui n'ont jamais eu d'interruptions
          TRUE ~ as.integer(.x)
        ))
      )
    )
}

rcd_chrs2_ML <- function(df, borne, vars){
  df %>%
    mutate(
      across(
        {{vars}},
        ~ as.character(case_when(
          {{borne}} == 333 & is.na(.x) ~ "Non renseigné (333)",                  # Concerné, pas répondu au module
          {{borne}} == 0 & is.na(.x) ~ "Non concerné (222)",
          {{borne}} == 1 & is.na(.x) ~ "Non renseigné (555)",                   # Concerné, mais pas répondu
          {{borne}} == 777 & is.na(.x) ~ "Non concernée (777)", # Pas concerné
          TRUE ~ as.character(.x)                                               # Réponse renseignée
        ))
      )
    )
}
vars_qcm <- T2_int_F1 %>% 
  select(contains("cause_"),
         -cause_autre) %>% 
  colnames()

T2_int_F2 <- T2_int_F1 %>% 
  group_by(id_anonymat) %>% 
  rcd_years_ML(sc_int_cor, an) %>% 
  rcd_nor_ML(sc_int_cor, c(classe, mois, repr)) %>% 
  rcd_chrs_ML(sc_int_cor, classe_cat) %>% 
  rcd_qcm(sc_int_cor, all_of(vars_qcm)) %>% 
  rcd_nor2_ML(repr, c(classe_repr, mois_repr, interromp_duree, duree_j)) %>%
  rcd_years2_ML(repr, an_repr) %>% 
  rcd_chrs2_ML(repr, classerepr_cat) %>% 
  mutate(
    non_repr = case_when(
      repr == 0 & is.na(non_repr) ~ 555L,
      repr == 1 & is.na(non_repr) ~ 222L,
      repr == 333 & is.na(non_repr) ~ 333L,
      repr == 777 & is.na(non_repr) ~ 777L,
      TRUE ~ non_repr
    )
  ) %>% 
  ungroup() %>% 
  group_by(id_anonymat, sc_int_date_creation) %>% 
  mutate(n = n(),
         sc_int_date_creation = case_when(
           sc_int_nb < 10 & n < sc_int_nb ~ sc_date_creation,
           TRUE ~ sc_int_date_creation
         )) %>% 
  ungroup() %>% 
  select(-n)
  

## 3.4 Pivot Wide ####
# Variables qui seront en plusieurs colonnes
vars_wider <- T2_int_F2 %>% 
  select(classe, mois, an, contains("cause_"), repr, classe_repr, classe_repr_autre, classe_autre,
         classerepr_cat, mois_repr, an_repr, interromp_duree, duree_j, non_repr, classe_cat) %>% 
  colnames()

# On pivote ensuite en wide en appliquant les nouveaux noms de variables
P2_int_W1 <- T2_int_F2 %>% 
  pivot_wider(
    names_from = rang_sc_int,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_int}_{.value}"
  )

## 3.5 Recoder NAs (MW) ####
vars_years_MW <- P2_int_W1 %>% 
  select(ends_with("_an"),
         ends_with("an_repr")) %>% 
  colnames()

vars_nor_MW <- P2_int_W1 %>% 
  select(contains("_cause"),
         ends_with("_classe"),
         ends_with("_mois"),
         contains("_repr"),
         -contains("_an"),
         contains("duree"),
         -ends_with("autre")) %>% 
  colnames()

vars_chr_MW <- P2_int_W1 %>% 
  select(ends_with("_classe_cat"),
         ends_with("classerepr_cat")) %>% 
  colnames()

P2_int_W2 <- P2_int_W1 %>% 
  group_by(id_anonymat) %>% 
  rcd_years_MW(sc_int_cor, all_of(vars_years_MW)) %>% 
  rcd_nor_MW(sc_int_cor, all_of(vars_nor_MW)) %>% 
  rcd_chr_MW(sc_int_cor, all_of(vars_chr_MW)) %>% 
  arrange(desc(sc_int_nb))

## 3.6 Joindre scolarité ####
common_vars <- intersect(names(M2_F5), names(P2_int_W2))

M2_F6 <- left_join(
  M2_F5,
  P2_int_W2,
  by = common_vars
)





