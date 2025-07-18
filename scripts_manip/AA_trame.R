# Pour la manipulation des données
library(tidyverse)

# Pour importer les données SAS
library(haven)

# Pour identifier les colonnes vides
library(janitor)

# 1.1 SCOLARITÉ ####

M2_F0 <- read_sas("../raw_data/gb_ddb_id_01_sc_02.sas7bdat")
## 1.1.1 Remove empty cols ####

M2_F1 <- M2_F0 %>%
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## 1.1.2 Rename cols ####

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
         "sc_commentaires" = "commentaires_scol")

### Clean up
rm(list = c("cols_fa",
            "cols_ps",
            "M2_F0",
            "M2_F1"))

## 1.1.3 Remove duplicates ####

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


# A - M1_V0 ####

M1_V0 <- M2_F3

### Nettoyer l'environnement 
rm(list = c("ids_TPO",
            "clean",
            "doublons",
            "identity",
            "M2_F2",
            "list_doublons",
            "vars_doublons",
            "vars_identity",
            "M2_F3"))

# 1.2 REDOUBLEMENT ####

M2_rdb_F0 <- read_sas("../raw_data/gb_ddb_sc_02_rdb_02.sas7bdat")

# Pas de colonnes vides

## 1.2.1 Rename cols ####

cols_fa <- M2_rdb_F0 %>% 
  select(all_of(starts_with("fa_"))) %>% 
  colnames()

cols_ps <- M2_rdb_F0 %>% 
  select(all_of(starts_with("ps_"))) %>% 
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
         "sc_rdb_redoubl" = "rdb_ps_redoubl") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
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

### Clean up 
rm(list = c("M2_rdb_F0",
            "cols_fa",
            "cols_ps",
            "cols_rd",
            "cols_rdb"))

## 1.2.2 Remove duplicates ####

vars_identity <- M2_rdb_F1 %>% 
  select(all_of(starts_with("id_")),
         all_of(starts_with("sc_")),
         -all_of(c("sc_rdb_date_creation",
                   "sc_rdb_redoubl",
                   "sc_rdb_type",
                   "sc_rdb_cat",
                   "id_age_cat"))) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(M2_rdb_F1), vars_identity))

doublons <- M2_rdb_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- M1_V0 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(vars_identity))

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

### Nettoyer l'environnement 
rm(list = c("M2_rdb_F1",
            "doublons",
            "list_doublons",
            "identity",
            "vars_identity",
            "vars_doublons",
            "clean",
            "ids_TPO"))

## 1.2.3 Événements distincts ####

test_event <- M2_rdb_F2 %>% 
  group_by(id_anonymat) %>% 
  filter(n_distinct(classe) != n()) %>% 
  arrange(id_anonymat, classe, an) %>% 
  relocate(id_tab_db, .before = id_date_creation) %>% 
  ungroup()

clean <- test_event %>% 
  slice(-c(3,6,8,9,12,16,24,26))

ids_TPO <- unique(clean$id_anonymat)

M2_rdb_F3 <- bind_rows(
  M2_rdb_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>% 
  arrange(id_anonymat, sc_rdb_date_creation) %>% 
  group_by(id_anonymat) %>% 
  mutate(sc_rdb_cat = "sc_02_rdb_02",
         sc_rdb_date_creation = case_when(
           n_distinct(sc_rdb_date_creation) > 1 ~ id_date_creation,
           TRUE ~ sc_rdb_date_creation)) %>% 
  ungroup()


### Nettoyer l'environnement 
rm(list = c("clean",
            "ids_TPO",
            "test_event",
            "M2_rdb_F2"))

## 1.2.4 Identité unique ####

vars_identity <- M2_rdb_F3 %>% 
  select(all_of(starts_with("id_")),
         all_of(starts_with("sc_")),
         -id_anonymat) %>% 
  colnames()

test_identity_cols <- M2_rdb_F3 %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(vars_identity), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

### Nettoyer l'environnement 
rm(list = c("test_identity_cols",
            "vars_identity"))

## 1.2.5 Lignes uniques ####

M2_rdb_F4 <- M2_rdb_F3 %>% 
  group_by(id_anonymat) %>% 
  arrange(id_anonymat, classe, desc(is.na(an)), an) %>% 
  mutate(sc_rdb_nb = n(),
         rang_sc_rdb = case_when(
           sc_rdb_nb == 1 ~ "sc_rdb01",
           TRUE ~ paste0("sc_rdb", sprintf("%02d", row_number()))
         )) %>% 
  ungroup() %>% 
  relocate(c(sc_rdb_nb, rang_sc_rdb), .after = classe)

vars_wider <- M2_rdb_F4 %>% 
  select(an, cause, classe, classe_autre, classe_cat) %>% 
  colnames()

M2_rdb_W1 <- M2_rdb_F4 %>% 
  pivot_wider(
    names_from = rang_sc_rdb,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_rdb}_{.value}"
  )

# Nettoyer l'environnement
rm(list = c("M2_rdb_F3",
            "vars_wider",
            "M2_rdb_F4"))

# B - M1_V1 ####
common_vars <- intersect(names(M1_V0), names(M2_rdb_W1))

verif_join <- anti_join(
  M2_rdb_W1,
  M1_V0,
  by = common_vars
)

M1_V1 <- left_join(
  M1_V0,
  M2_rdb_W1,
  by = common_vars
) %>% 
  group_by(id_anonymat) %>% 
  mutate(
    id_age_cat = case_when(
      (is.na(id_age_cat) & id_age < 18) ~ "Adolescent",
      (is.na(id_age_cat) & id_age > 17) ~ "Adulte",
      TRUE ~ id_age_cat
    )) %>% 
  ungroup()

# Nettoyer l'environnement
rm(list = c("common_vars",
            "M2_rdb_W1",
            "verif_join"))

# 1.3 INTERRUPTIONS SCOLAIRES ####
M2_int_F0 <- read_sas("../raw_data/gb_ddb_sc_02_int_02.sas7bdat")


## 1.3.1 Rename cols ####

cols_fa <- M2_int_F0 %>% 
  select(all_of(starts_with("fa_"))) %>% 
  colnames()

cols_ps <- M2_int_F0 %>% 
  select(all_of(starts_with("ps_"))) %>% 
  colnames()

cols_int <- M2_int_F0 %>% 
  select(all_of(starts_with("int_")), -int_ps_interromp) %>% 
  colnames()

cols_it <- M2_int_F0 %>% 
  select(all_of(starts_with("it_"))) %>% 
  colnames()

M2_int_F1 <- M2_int_F0 %>%  
  rename_with(~ gsub("^fa_", "sc_", .x), all_of(cols_fa)) %>% 
  rename_with(~ gsub("^ps_", "sc_", .x), all_of(cols_ps)) %>%
  rename_with(~ gsub("^int_", "sc_int_", .x), all_of(cols_int)) %>%
  rename_with(~ gsub("^it_", "", .x), all_of(cols_it)) %>%
  rename("id_tab_db" = "tab_db",
         "sc_int_interromp" = "int_ps_interromp") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
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

### Clean up
rm(list = c("M2_int_F0",
            "cols_fa",
            "cols_int",
            "cols_it",
            "cols_ps"))

## 1.3.2 Remove duplicates ####

vars_identity <- M2_int_F1 %>% 
  select(all_of(starts_with("id_")),
         all_of(starts_with("sc_")),
         -all_of(c("sc_int_date_creation",
                   "sc_int_interromp",
                   "sc_int_type",
                   "sc_int_cat"))) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(M2_int_F1), vars_identity))

doublons <- M2_int_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- M1_V1 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(vars_identity))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  by = "id_link"
)

ids_TPO <- unique(identity$id_link)

M2_int_F2 <- bind_rows(
  M2_int_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>%
  filter(sc_int_type == "P")

# Nettoyer l'environnement
rm(list = c("vars_identity",
            "vars_doublons",
            "doublons",
            "list_doublons",
            "identity",
            "clean",
            "ids_TPO",
            "M2_int_F1"))

## 1.3.3 Événements distincts ####
test_event <- M2_int_F2 %>% 
  group_by(id_anonymat, classe, an_repr) %>%
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) %>% 
  arrange(id_anonymat, classe, an) %>% 
  relocate(c(n_comb, classe, an, mois, repr, non_repr, an_repr, mois_repr, interromp_duree, duree_j), .after = sc_int_date_creation) %>% 
  ungroup()

clean <- M2_int_F2 %>% 
  filter(id_anonymat %in% test_event$id_anonymat) %>%
  arrange(id_anonymat, classe) %>% 
  mutate(
    cause_1 = case_when(
      id_anonymat == "YASAA" & row_number() == 7 ~ cause_1[which(id_anonymat == "YASAA" & row_number() == 6)],
      TRUE ~ cause_1
    ),
    cause_999 = case_when(
      id_anonymat == "JNYQS" & row_number() == 1 ~ cause_999[which(id_anonymat == "JNYQS" & row_number() == 2)],
      id_anonymat == "YASAA" & row_number() == 7 ~ cause_999[which(id_anonymat == "YASAA" & row_number() == 6)],
      TRUE ~ cause_999
    ),
    cause_autre = case_when(
      id_anonymat == "JNYQS" & row_number() == 1 ~ cause_autre[which(id_anonymat == "JNYQS" & row_number() == 2)],
      id_anonymat == "YASAA" & row_number() == 7 ~ cause_autre[which(id_anonymat == "YASAA" & row_number() == 6)],
      TRUE ~ cause_autre
    )) %>% 
  slice(-c(2,6))


M2_int_F3 <- bind_rows(
  M2_int_F2 %>% 
    filter(!id_anonymat %in% clean$id_anonymat),
  clean
) %>% 
  arrange(id_anonymat, sc_int_date_creation) %>% 
  group_by(id_anonymat) %>% 
  mutate(sc_int_cat = "sc_02_int_02",
         sc_int_date_creation = case_when(
           n_distinct(sc_int_date_creation) > 1 ~ id_date_creation,
           TRUE ~ sc_int_date_creation)) %>% 
  ungroup()



# Nettoyer l'environnement
rm(list = c("clean",
            "test_event",
            "M2_int_F2"))

## 1.3.4 Identité unique ####
vars_identity <- M2_int_F3 %>% 
  select(all_of(starts_with("id_")),
         all_of(starts_with("sc_")),
         -id_anonymat) %>% 
  colnames()

test_identity_cols <- M2_int_F3 %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(vars_identity), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

# Nettoyer l'environnement
rm(list = c("test_identity_cols",
            "vars_identity"))

## 1.3.5 Lignes uniques ####
M2_int_F4 <- M2_int_F3 %>% 
  group_by(id_anonymat) %>% 
  arrange(id_anonymat, desc(is.na(an)), an, mois, classe) %>% 
  mutate(sc_int_nb = n(),
         rang_sc_int = case_when(
           sc_int_nb == 1 ~ "sc_int01",
           TRUE ~ paste0("sc_int", sprintf("%02d", row_number()))
         )) %>% 
  ungroup() %>% 
  relocate(c(sc_int_nb, rang_sc_int, classe), .before = an)

vars_wider <- M2_int_F4 %>% 
  select(-all_of(starts_with("id_")),
         -all_of(starts_with("sc_")),
         -rang_sc_int) %>% 
  colnames()

M2_int_W1 <- M2_int_F4 %>% 
  pivot_wider(
    names_from = rang_sc_int,
    values_from = all_of(vars_wider),
    names_glue = "{rang_sc_int}_{.value}"
  )

# Nettoyer l'environnement
rm(list = c("M2_int_F3",
            "vars_wider",
            "M2_int_F4"))

# C - M1_V2 ####
common_vars <- intersect(names(M1_V1), names(M2_int_W1))

verif_join <- anti_join(
  M2_int_W1,
  M1_V1,
  by = common_vars
)

M1_V2 <- left_join(
  M1_V1,
  M2_int_W1,
  by = common_vars
)

# Nettoyer l'environnement
rm(list = c("common_vars",
            "M2_int_W1",
            "verif_join"))
# 2.1 PROFESSION ####
M3_F0 <- read_sas("../raw_data/gb_ddb_id_01_pr_03.sas7bdat")

## 2.1.1 Remove empty cols ####
M3_F1 <- M3_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## 2.1.2 Rename cols ####

cols_sap <- M3_F1 %>% 
  select(all_of(starts_with("sap_")),
         statut_emploi,
         parc_prof) %>% 
  colnames()

cols_pp <- M3_F1 %>% 
  select(all_of(starts_with("pp_")),
         -pp_an_prems,
         -pp_ado_pdt_6mois
  ) %>% 
  colnames()

M3_F2 <- M3_F1 %>% 
  rename_with(~ paste0("pr_", .x), all_of(cols_sap)) %>% 
  rename_with(~ gsub("^pp_", "pr_", .x), all_of(cols_pp)) %>% 
  rename("id_tab_db" = "tab_db",
         "pr_sap_csp_prof" = "csp_sap_prof",
         "pr_commentaires" = "commentaires_prof",
         "pr_prems_an" = "pp_an_prems",
         "pr_pdt_6mois_ado" = "pp_ado_pdt_6mois") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

### Clean up
rm(list = c("M3_F0",
            "M3_F1",
            "cols_pp",
            "cols_sap"))

## 2.1.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M3_F2), names(M1_V2)))

vars_doublons <- union("id_link", setdiff(names(M3_F2), common_vars))

doublons <- M3_F2 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M1_V2 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- full_join(
  identity,
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
) %>% 
  select(-id_link)

ids_TPO <- union(list_doublons, clean$id_anonymat)

M3_F3 <- bind_rows(
  M3_F2 %>% 
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

### Clean up
rm(list = c("common_vars",
            "ids_TPO",
            "list_doublons",
            "vars_doublons",
            "clean",
            "doublons",
            "identity",
            "M3_F2"))

# D - M1_V3 ####

common_vars <- intersect(names(M1_V2), names(M3_F3))


verif_join <- anti_join(
  M3_F3,
  M1_V2,
  by = common_vars
)

M1_V3 <- left_join(
  M1_V2,
  M3_F3,
  by = common_vars
)

### Clean up
rm(list = c("common_vars",
            "verif_join"))

# 2.2 INTERRUPTIONS PRO ####

## 2.2.1 Remove empty cols ####
M3_int_F0 <- read_sas("../raw_data/gb_ddb_pr_03_int_03.sas7bdat")

M3_int_F1 <- M3_int_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

rm(M3_int_F0)

## 2.2.2 Rename cols ####

# Colonnes où on ajoute le préfix pr
cols_add_pr <- M3_int_F1 %>% 
  select(all_of(starts_with("sap_")),
         statut_emploi,
         parc_prof) %>% 
  colnames()

# Colonnes où on enlève le préfix pcq concerne events multiples
cols_rmv_pp_interromp <- M3_int_F1 %>% 
  select(all_of(starts_with("pp_interromp_"))) %>% 
  colnames()

# Colonnes où on enlève aussi le préfixe pcq concerne events multiples
# mais la structure des variables est différente
cols_rmv_pp <- M3_int_F1 %>% 
  select(all_of(starts_with("pp_reprise_")),
         pp_duree_j,
         pp_non_repr) %>% 
  colnames()

# Variables où ne fait que changer le préfix
cols_sub_pr <- M3_int_F1 %>% 
  select(all_of(starts_with("pp_")), -all_of(c(cols_rmv_pp_interromp, 
                                               cols_rmv_pp)),
         -pp_reprise,
         -pp_an_prems) %>% 
  colnames()


M3_int_F2 <- M3_int_F1 %>% 
  rename_with(~ paste0("pr_", .x), all_of(cols_add_pr)) %>%
  rename_with(~ gsub("^pp_interromp_", "", .x), all_of(cols_rmv_pp_interromp)) %>%
  rename_with(~ gsub("^pp_", "", .x), all_of(cols_rmv_pp)) %>% 
  rename_with(~ gsub("^pp_", "pr_", .x), all_of(cols_sub_pr)) %>%
  rename("id_tab_db" = "tab_db",
         "pr_commentaires" = "commentaires_prof",
         "pr_int_date_creation" = "int_date_creation",
         "pr_int_interromp"  = "int_pp_interromp",
         "pr_int_type" = "int_type",
         "reprise" = "pp_reprise",
         "pr_prems_an" = "pp_an_prems") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))%>% 
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

### Clean up
rm(list = c("cols_add_pr",
            "cols_rmv_pp_interromp",
            "cols_rmv_pp",
            "cols_sub_pr"))

## 2.2.3 Remove duplicates ####

vars_doublons <- union("id_link", setdiff(names(M3_int_F2), names(M1_V3)))

common_vars <- intersect(names(M3_int_F2), names(M1_V3))

doublons <- M3_int_F2 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_anonymat)

identity <- M1_V3 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  by = "id_link",
  relationship = "many-to-many"
)

comparaison <- bind_rows(
  clean,
  M3_int_F2 %>% 
    filter(id_anonymat %in% clean$id_anonymat)
) %>% 
  relocate(id_anonymat, id_date_creation, pr_int_date_creation, an, mois) %>% 
  arrange(id_anonymat, an, mois)

corr <- comparaison %>% 
  slice(-c(2, 7, 8, 10, 11, 14))

ids_TPO <- union(unique(comparaison$id_anonymat), unique(comparaison$id_link))

M3_int_F3 <- bind_rows(
  M3_int_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  corr
) %>% 
  arrange(id_anonymat, an, mois) %>% 
  filter(pr_int_type == "P") %>% 
  group_by(id_anonymat) %>% 
  mutate(pr_int_date_creation = case_when(
    n_distinct(pr_int_date_creation) > 1 ~ id_date_creation,
    TRUE ~ pr_int_date_creation)) %>% 
  ungroup()

### Clean up
rm(list = c("vars_doublons",
            "common_vars",
            "doublons",
            "list_doublons",
            "comparaison",
            "identity",
            "clean",
            "corr",
            "ids_TPO"))

## 2.2.4 Événements distincts ####

test_event <- M3_int_F3 %>% 
  group_by(id_anonymat, an, mois) %>%
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) %>% 
  arrange(id_anonymat, an, mois) %>%
  ungroup()

# Une personne n'a pas renseigné de date, mais les raisons sont différentes
# je vais donc conserver les deux observations

### Clean up
rm(test_event)

## 2.2.5 Identité unique ####
vars_wider <- M3_int_F3 %>% 
  select(-all_of(starts_with("id_")),
         -all_of(starts_with("pr_"))) %>% 
  colnames()

vars_fixe <- setdiff(setdiff(names(M3_int_F3), vars_wider), "id_anonymat")

test_identity_cols <- M3_int_F3 %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(vars_fixe), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

# Toutes les identités sont les mêmes

### Clean up
rm(list = c("test_identity_cols",
            "vars_wider",
            "vars_fixe"))


## 2.2.6 Lignes uniques ####
M3_int_F4 <- M3_int_F3 %>% 
  group_by(id_anonymat) %>% 
  arrange(id_anonymat, desc(is.na(an)), an, mois, reprise_an, reprise_mois) %>% 
  mutate(pr_int_nb = n(),
         rang_pr_int = case_when(
           pr_int_nb == 1 ~ "pr_int01",
           TRUE ~ paste0("pr_int", sprintf("%02d", row_number()))
         )) %>% 
  ungroup() %>% 
  relocate(pr_int_nb, rang_pr_int, an, mois, reprise_an, reprise_mois)

vars_wider <- M3_int_F4 %>% 
  select(-all_of(starts_with("id_")),
         -all_of(starts_with("pr_")),
         -rang_pr_int) %>% 
  colnames()

M3_int_W1 <- M3_int_F4 %>% 
  pivot_wider(
    names_from = rang_pr_int,
    values_from = all_of(vars_wider),
    names_glue = "{rang_pr_int}_{.value}"
  )

# E - M1_V4 ####

common_vars <- intersect(names(M3_int_W1), names(M1_V3))

verif_join <- anti_join(
  M3_int_W1,
  M1_V3,
  by = common_vars
)

# Réunir tous les individus et les variables concernés
comparaison <- bind_rows(
  M3_int_W1 %>% 
    filter(id_anonymat %in% verif_join$id_anonymat) %>%
    select(all_of(common_vars)),
  M1_V3 %>% 
    filter(id_anonymat %in% verif_join$id_anonymat) %>%
    select(all_of(common_vars))
) %>% 
  arrange(id_anonymat)

# Obtenir le nom des variables problématiques
test_identity_cols <- comparaison %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(setdiff(common_vars, "id_anonymat")), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

vars_verif <- colnames(test_identity_cols)

# Remplacer les variables problématiques par celles de M1_V3
corrections <- M1_V3 %>% 
  select(all_of(vars_verif)) %>% 
  filter(id_anonymat %in% M3_int_W1$id_anonymat)

M3_int_W2 <- left_join(
  M3_int_W1 %>% 
    select(-setdiff(vars_verif, "id_anonymat")),
  corrections,
  by = "id_anonymat"
) 

# Retester l'anti_join
verif_join <- anti_join(
  M3_int_W2,
  M1_V3,
  by = common_vars
) # Le df est vide

M1_V4 <- left_join(
  M1_V3,
  M3_int_W2,
  by = common_vars
)
