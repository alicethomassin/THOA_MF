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

## 1.3 recoder variables ####

### 1.3.1 Mettre labels ####


test <- M2_F2 %>% 
  select(id_anonymat, id_age, sc_debut)

look_for(test)

test %>% dfSummary() %>% view()

# Modification de la fonctions dfSummary() pour remplacer la dernière ligne par
# Q1 et Q3
st_options(
  dfSummary.custom.1 = 
    expression(
      paste(
        "Q1 - Q3 :",
        round(
          quantile(column_data, probs = .25, type = 2, 
                   names = FALSE, na.rm = TRUE), digits = 1
        ), " - ",
        round(
          quantile(column_data, probs = .75, type = 2, 
                   names = FALSE, na.rm = TRUE), digits = 1
        )
      )
    )
)

dfSummary(M1_identity, 
          plain.ascii  = FALSE, 
          style        = "grid", 
          graph.magnif = 0.82, 
          varnumbers   = FALSE,
          valid.col    = FALSE,
          tmp.img.dir  = "/tmp") %>% view()


# Sélectionner variables à mettre en integer
vars_int <- M2_F2 %>% 
  select(
    id_centre1, id_centre2, id_centre3, id_dep_nais, id_lieu_nais, id_sexe,
    id_age, id_tab_db, sc_diplome_an, sc_diplome, sc_fin_etudes, sc_formation,
    sc_debut_an, sc_debut, sc_interromp, sc_redoubl, sc_debut_corr, 
    sc_debut_age, sc_diplome_age, starts_with("sc_plan"), -sc_plan_notification_autre) %>% 
  colnames()

# Sélectionner les variables à traiter comme des choix multiples
vars_choix_multiple <- M2_F2 %>% 
  select(starts_with("sc_plan_ets"),
         starts_with("sc_plan_notifi"),
         -sc_plan_notification_autre) %>% 
  colnames()

# Sélectionner les variables pour extraireune partie du titre et en faire
# de nouvelles variables pour connaître le nombre de plans d'intervention
# ainsi que la classe de ce premier plan
vars_ets <- M2_F2 %>% 
  select(starts_with("sc_plan_ets")) %>% 
  names()

classe_index <- as.integer(str_extract(vars_ets, "\\d+$"))

recode_year1 <- function(df, borne, var_year){
  # Dans le cas où il y a une question borne
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{borne}} == 1 & is.na({{var_year}}) ~ 5555L, # Concerné, mais pas répondu
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
        {{borne}} == 1 & is.na({{var_month}}) ~ 55L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 77L,        # Pas concerné
        is.na({{borne}}) ~ NA_integer_,              # Pas participé
        TRUE ~ {{var_month}}                         # Réponse renseignée
    ))
}

recode_year2 <- function(df, type, var_year){
  # Dans le cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{type}} == "P" & is.na({{var_year}}) ~ 5555L,       # Concerné, mais pas répondu
        {{type}} != "P" & is.na({{var_year}}) ~ NA_integer_, # Pas participé   
        TRUE ~ {{var_year}}                                  # Réponse renseignée
      )
    )
}

recode_month2 <- function(df, type, var_month){
  # Dans la cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_month}} := case_when(
        {{type}} == "P" & is.na({{var_month}}) ~ 55L,         # Concerné, mais pas répondu
        {{type}} != "P" & is.na({{var_month}}) ~ NA_integer_, # Pas participé
        TRUE ~ {{var_month}}                                  # Réponse renseignée
      )
    )
}

recode_qnorm <- function(df, type, var_miss){
  # Pour les questions d'un module hors bornes, donc obligatoires
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{type}} == "P" & is.na({{var_miss}}) ~ 555L,        # Pas répondu
        {{type}} != "P" & is.na({{var_miss}}) ~ NA_integer_, # Pas participé module
        TRUE ~ {{var_miss}}                                  # Réponse enregistrée
      )
    )
}

recode_qborned <- function(df, borne, var_miss){
  # Pour les questions bornées au sein d'un module
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{borne}} == 1 & is.na({{var_miss}}) ~ 444L,  # Concerné, pas répondu
        {{borne}} == 555 & is.na({{var_miss}}) ~ 555L,# Pas répondu
        {{borne}} == 0  & is.na({{var_miss}}) |
          {{borne}} > 1 & is.na({{var_miss}}) ~ 777L, # Pas concerné, pas répondu
        is.na({{borne}}) ~ NA_integer_,               # Pas participé module
        TRUE ~ {{var_miss}}                           # Réponse renseignée
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
          {{borne}} == 444 ~ 444L,
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
      sc_plan == 555 ~ 555L,
      sc_plan == 444 ~ 444L,
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
    )) 

vle_var <- M2_F3 %>% 
  relocate(sc_type, sc_debut, sc_redoubl, sc_interromp,
           sc_plan, sc_plan_an, sc_plan_classe, sc_plan_nb, sc_plan_demande, 
           sc_fin_etudes, sc_formation, sc_formation_autre, sc_diplome,
           sc_diplome_autre, sc_diplome_an)

M2_F4 <- M2_F3 %>% 
  recode_year1(sc_plan, sc_plan_an) %>%
  recode_qcm(sc_plan, vars_choix_multiple) %>% 
  recode_qnorm(sc_type, sc_plan_demande) 
  
  
verif <- M2_F3 %>% 
  relocate(sc_type, sc_debut, sc_debut_autre, sc_debut_corr, sc_debut_an, sc_debut_age, sc_debut_date,
           sc_plan, sc_plan_classe, sc_plan_nb, sc_plan_an, sc_plan_demande, all_of(vars_choix_multiple))


sc_P <- M2_F3 %>% 
  relocate(sc_type, all_of(quest_norm))

quest_norm <- M2_F1 %>% 
  select(sc_debut, sc_debut_an, sc_redoubl, sc_interromp, sc_plan, sc_plan_demande,
         sc_fin_etudes) %>% 
  colnames()


sc_empties <- M2_F2 %>%
  select(id_anonymat, sc_type, all_of(quest_norm)) %>% 
  mutate(n = n_miss_row(across(all_of(quest_norm)))) %>% 
  relocate(id_anonymat, sc_type, n)



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
          {{borne}} == 0 | {{borne}} > 1 ~ "Non concerné",       # Pas concerné
          is.na({{borne}}) ~ NA_character_,               # Pas participé
          TRUE ~ as.character(.x)                           # Réponse renseignée
        ))
      )
    )
}

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

vars_wider <- rdb_F3 %>% 
  select(an, cause, classe, classe_autre, classe_cat) %>% 
  colnames()

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
          {{borne}} == 1 & is.na(.x) ~ "444-Pas concerné", # Concerné, mais pas de répétition
          {{borne}} == 0 | {{borne}} > 1 ~ "777-Non concerné",       # Pas concerné
          is.na({{borne}}) ~ NA_character_,               # Pas participé
          TRUE ~ as.character(.x)                           # Réponse renseignée
        ))
      )
    )
}

rdb_W1 <- rdb_F3 %>% 
  pivot_wider(
    names_from = rang_sc_rdb,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_rdb}_{.value}"
  )

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
  
#JE SUIS RENDUE ICI
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