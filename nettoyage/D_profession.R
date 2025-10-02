library(tidyverse)
library(questionr)
# 2.1 PROFESSION ####
M3_F0 <- haven::read_sas("../raw_data/gb_ddb_id_01_pr_03.sas7bdat")
## 2.1 Réduire et renommer #### 
# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P3_F0 <- M3_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation,
         setdiff(names(.), names(M2_F0)))


pr_P <- M3_F0 %>% 
  filter(pr_type != "P") %>% 
  relocate(pr_type, pp_pdt_6mois)

pp_pdt <- M3_F0 %>% 
  filter(is.na(pp_pdt_6mois)) %>% 
  relocate(pr_type, pp_pdt_6mois, pp_prems_statut, sap_emploi, all_of(starts_with("pp_mesures_bis")))

## 2.1.2 Rename cols ####

cols_sap <- M3_F0 %>% 
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
