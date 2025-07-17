# INTERRUPTIONS SCOLAIRES ####
M2_int_F0 <- read_sas("../raw_data/gb_ddb_sc_02_int_02.sas7bdat")


## Rename cols ####

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

## Remove duplicates ####

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
rm(list = c("M2_int_F0",
            "cols_fa",
            "cols_int",
            "cols_it",
            "cols_ps",
            "module_interruption_scolaires",
            "vars_identity",
            "vars_doublons",
            "doublons",
            "list_doublons",
            "identity",
            "clean",
            "ids_TPO",
            "M2_int_F1"))

## Événements distincts ####
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

## Identité unique ####
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

## Lignes uniques ####
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
