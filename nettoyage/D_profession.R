# 2.1 PROFESSION ####
M3_F0 <- read_sas("../raw_data/gb_ddb_id_01_pr_03.sas7bdat")

pr_P <- M3_F0 %>% 
  filter(pr_type != "P") %>% 
  relocate(pr_type, pp_pdt_6mois)

pp_pdt <- M3_F0 %>% 
  filter(is.na(pp_pdt_6mois)) %>% 
  relocate(pr_type, pp_pdt_6mois, pp_prems_statut, sap_emploi, all_of(starts_with("pp_mesures_bis")))

           