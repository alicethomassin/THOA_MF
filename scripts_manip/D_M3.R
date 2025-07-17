# PROFESSION ####
M3_F0 <- read_sas("../raw_data/gb_ddb_id_01_pr_03.sas7bdat")

## Remove empty cols ####
M3_F1 <- M3_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## Rename cols ####

cols_sap <- M3_F1 %>% 
  select(all_of(starts_with("sap_")),
         statut_emploi,
         parc_prof) %>% 
  colnames()

cols_pp <- M3_F1 %>% 
  select(all_of(starts_with("pp_"))) %>% 
  colnames()

M3_F2 <- M3_F1 %>% 
  rename_with(~ paste0("pr_", .x), all_of(cols_sap)) %>% 
  rename_with(~ gsub("^pp_", "pr_", .x), all_of(cols_pp)) %>% 
  rename("id_tab_db" = "tab_db",
         "pr_sap_csp_prof" = "csp_sap_prof",
         "pr_commentaires" = "commentaires_prof") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

### Clean up
rm(list = c("M3_F0",
            "M3_F1",
            "cols_pp",
            "cols_sap"))

## Remove duplicates ####
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

# M1_V3 ####

## Verif join ####

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

# INTERRUPTIONS PRO ####

## Remove empty cols ####
M3_int_F0 <- read_sas("../raw_data/gb_ddb_pr_03_int_03.sas7bdat")

M3_int_F1 <- M3_int_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## Rename cols ####

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
                                               cols_rmv_pp))) %>% 
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
         "pr_int_type" = "int_type") %>% 
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

## Remove duplicates ####

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
  arrange(id_anonymat, an, mois)

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






