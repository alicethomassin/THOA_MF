# 3.4 FRATRIE ####
M4_frt_F0 <- read_sas("../raw_data/gb_ddb_fa_04_fratrie_04.sas7bdat")

## 3.4.1 Remove empty cols ####
M4_frt_F1 <- M4_frt_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols") %>% 
  select(-fratrie_pf_fratrie)

## Rename cols ####
vars_add_fa <- M4_frt_F1 %>% 
  select(all_of(starts_with("fo_")),
         fratrie_date_creation,
         fratrie_type) %>% 
  colnames()

vars_sub_pf <- M4_frt_F1 %>% 
  select(all_of(starts_with("pf_mere_")),
         all_of(starts_with("pf_pere_")),
         pf_fratrie) %>% 
  colnames()

vars_fratrie <- M4_frt_F1 %>% 
  select(all_of(starts_with("pf_fratrie_")),
         all_of(starts_with("pf_frsr_"))) %>% 
  colnames()

vars_sub_sf <- M4_frt_F1 %>% 
  select(all_of(starts_with("sf_"))) %>% 
  colnames()

M4_frt_F2 <- M4_frt_F1 %>% 
  rename_with(~ paste0("fa_", .x), all_of(vars_add_fa)) %>%
  rename_with(~ gsub("^pf_", "fa_", .x), all_of(vars_sub_pf)) %>%
  rename("id_tab_db" = "tab_db",
         "fa_commentaires" = "commentaires_fam",
         "fa_pm_union_an" = "pf_union_an",
         "fa_pm_union" = "pf_union_parents",
         "fa_pm_union_mois" = "pf_union_mois",
         "fa_rang_nais" = "pf_rang_nais",
         "fa_pm_union_int_muco" = "pf_raison_muco",
         "fa_pm_union_int" = "pf_parents_interromp",
         "fa_pm_union_int_an" = "pf_interromp_an",
         "fa_pm_union_int_mois" = "pf_interromp_mois",
         "fa_pm_union_int_par" = "pf_interromp_par",
         "adopte_an" = "pf_frsr_an_adopte",
         "adopte_mois" = "pf_frsr_mois_adopte",
         "nais_an" = "pf_fratrie_an_nais",
         "nais_mois" = "pf_fratrie_mois_nais",
         "deces_an" = "pf_fratrie_an_deces",
         "deces_mois" = "pf_fratrie_mois_deces") %>% 
  rename_with(~ gsub("^pf_fratrie_", "", .x), any_of(vars_fratrie)) %>%
  rename_with(~ gsub("^pf_frsr_", "", .x), any_of(vars_fratrie)) %>%
  rename_with(~ gsub("^sf_", "fa_", .x), all_of(vars_sub_sf))  %>% 
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
  )) %>% 
  mutate(id_date_nais = case_when(
    id_anonymat %in% id_DDN ~ DDN,
    TRUE ~ id_date_nais
  ))

### Clean up
rm(list = c("M4_frt_F0",
            "M4_frt_F1",
            "vars_add_fa",
            "vars_sub_pf",
            "vars_fratrie",
            "vars_sub_sf"))
