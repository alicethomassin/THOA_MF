# 3.1 FAMILLE ####
M4_F0 <- read_sas("../raw_data/gb_ddb_id_01_fa_04.sas7bdat")

## 3.1.1 Remove empty cols ####
M4_F1 <- M4_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## 3.1.2 Rename cols ####

vars_sub_pf <- M4_F1 %>% 
  select(all_of(starts_with("pf_mere_")),
         all_of(starts_with("pf_pere_"))) %>% 
  colnames()

vars_add_fa <- M4_F1 %>% 
  select(all_of(starts_with("fo_"))) %>% 
  colnames()

vars_sub_sf <- M4_F1 %>% 
  select(all_of(starts_with("sf_"))) %>% 
  colnames()

M4_F2 <- M4_F1 %>% 
  rename_with(~ gsub("^pf_", "fa_", .x), all_of(vars_sub_pf)) %>%
  rename_with(~ gsub("^sf_", "fa_", .x), all_of(vars_sub_sf)) %>%
  rename_with(~ paste0("fa_", .x), all_of(vars_add_fa)) %>% 
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
         "fa_fratrie" = "pf_fratrie") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))
### Clean up
rm(list = c("M4_F0",
            "M4_F1",
            "vars_sub_pf",
            "vars_add_fa",
            "vars_sub_sf"))










