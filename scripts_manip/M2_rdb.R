# REDOUBLEMENT ####

M2_rdb_F0 <- read_sas("../raw_data/gb_ddb_sc_02_rdb_02.sas7bdat")

# Pas de colonnes vides

## Rename cols ####

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
rm(list = c("M2_int_F0",
            "cols_fa",
            "cols_ps",
            "cols_rd",
            "cols_rdb"))

## Remove duplicates ####

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

identity <- M2_F3 %>% 
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
rm(list = c("module_redoublement_scolaire",
            "M2_rdb_F0",
            "cols_fa",
            "cols_ps",
            "cols_rd",
            "cols_rdb",
            "M2_rdb_F1",
            "doublons",
            "list_doublons",
            "identity",
            "vars_identity",
            "vars_doublons",
            "clean",
            "ids_TPO"))

## Événements distincts ####

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

## Identité unique ####

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

## Lignes uniques ####

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
