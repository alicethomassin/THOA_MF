# 2. PROFESSION ####
M3_F0 <- read_sas("../raw_data/gb_ddb_id_01_pr_03.sas7bdat")

## 2.1 Réduire et renommer #### 
# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P3_F0 <- M3_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation,
         setdiff(names(.), names(M2_F0)))

cols_sap <- P3_F0 %>% 
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



