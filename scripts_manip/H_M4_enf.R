# 3.3 ENFANTS ####
M4_enf_F0 <- read_sas("../raw_data/gb_ddb_fa_04_enfants_04.sas7bdat")

## 3.3.1 Remove empty cols ####
M4_enf_F1 <- M4_enf_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols") %>% 
  select(-enfants_sf_enfants)

## 3.3.2 Rename cols ####
vars_add_fa <- M4_enf_F1 %>% 
  select(all_of(starts_with("fo_")),
         enfants_date_creation,
         enfants_type) %>% 
  colnames()

vars_sub_pf <- M4_enf_F1 %>% 
  select(all_of(starts_with("pf_mere_")),
         all_of(starts_with("pf_pere_")),
         pf_fratrie) %>% 
  colnames()

vars_sub_sf <- M4_enf_F1 %>% 
  select(all_of(starts_with("sf_")),
         -all_of(contains("_enf_")),
         all_of(contains("_nb"))) %>% 
  colnames()

vars_enfants <- M4_enf_F1 %>% 
  select(all_of(starts_with("sf_enf")),
         -sf_enf_adopte_nb,
         -sf_enf_bio_nb) %>% 
  colnames()


M4_enf_F2 <- M4_enf_F1 %>% 
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
         "adopte_an" = "sf_enf_an_adopte",
         "adopte_mois" = "sf_enf_mois_adopte",
         "nais_an" = "sf_enf_an_nais",
         "nais_mois" = "sf_enf_mois_nais",
         "deces_an" = "sf_enf_an_deces",
         "deces_mois" = "sf_enf_mois_deces") %>% 
  rename_with(~ gsub("^sf_enf_", "", .x), any_of(vars_enfants)) %>%
  rename_with(~ gsub("^sf_", "fa_", .x), all_of(vars_sub_sf)) %>% 
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
rm(list = c("vars_add_fa",
            "vars_sub_pf",
            "vars_sub_sf",
            "vars_enfants",
            "M4_enf_F0",
            "M4_enf_F1"))
  
## 3.3.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M4_enf_F2), names(M1_V6)))
vars_doublons <- union("id_link", setdiff(names(M4_enf_F2), common_vars))  

doublons <- M4_enf_F2 %>% 
  filter(is.na(id_date_creation))
  
list_doublons <- unique(doublons$id_link)

identity <- M1_V6 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  relationship = "many-to-many",
  by = "id_link"
) %>% 
  slice(-1)

ids_TPO <- unique(doublons$id_anonymat)

M4_enf_F3 <- bind_rows(
  M4_enf_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>% 
  filter(fa_enfants_type == "P") %>% 
  arrange(id_anonymat, nais_an, nais_mois, adopte_an, adopte_mois) %>% 
  relocate(id_anonymat, nais_an, nais_mois, prenom, adopte_an, adopte_mois) %>% 
  group_by(id_anonymat) %>% 
  mutate(
   # fa_enfants_date_creation = case_when(
   #   n_distinct(fa_enfants_date_creation) > 1 ~ id_date_creation,
   #   TRUE ~ fa_enfants_date_creation),
    fa_enfants_cat = case_when(
      n_distinct(fa_enfants_cat) > 1 ~ "fa_04_enfants_04",
      TRUE ~ fa_enfants_cat
    )) %>% 
  mutate(
    arrivee_an = case_when(
      is.na(adopte_an) ~ nais_an,
      TRUE ~ adopte_an),
    arrivee_mois = case_when(
      is.na(adopte_mois) ~ nais_mois,
      TRUE ~ adopte_mois
    )) %>% 
  ungroup() %>% 
  filter(!is.na(arrivee_an)) # Il y a 120 naissances

### Clean up
rm(list = c("clean",
            "doublons",
            "identity",
            "M4_enf_F2",
            "common_vars",
            "ids_TPO",
            "list_doublons",
            "vars_doublons"))

## 3.3.4 Événements distincts ####
test_event <- M4_enf_F3 %>% 
  group_by(id_anonymat, arrivee_an, prenom) %>%
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) %>% 
  ungroup()

ids_verif <- unique(test_event$id_anonymat)

verif <- M4_enf_F3 %>% 
  filter(id_anonymat %in% ids_verif) %>% 
  arrange(id_anonymat, arrivee_an) %>%
  relocate(id_anonymat, fa_enf_bio_nb, fa_enf_adopte_nb, issu, etat_mat, arrivee_an, prenom, conjt_prenom) %>% 
  ungroup()

clean <- verif %>%
  slice(c(2, 4, 8)) 

M4_enf_F4 <- bind_rows(
  M4_enf_F3 %>% 
    filter(!id_anonymat %in% ids_verif),
  clean
) %>% 
  arrange(id_anonymat, arrivee_an, arrivee_mois)

### Clean up
rm(list = c("clean",
            "test_event",
            "verif",
            "ids_verif"))

#### Démarche identifier conjoints ####
conjt <- M1_V6 %>% 
  filter(id_anonymat %in% ids_verif) %>% 
  select(id_anonymat, fa_cpl_nb, fa_cpl01_conjt_enf, fa_cpl01_conjt_prenom, fa_cpl01_conjt_union_an, fa_cpl01_int_an)

vars_couple <- M1_V6 %>% 
  select(all_of(starts_with("fa_cpl0"))) %>% 
  colnames()

JNDTO <- M1_V6 %>%
  filter(id_anonymat %in% verif$id_anonymat) %>% 
  mutate(across(starts_with("fa_cpl0"), as.character)) %>% 
  pivot_longer(
    cols = starts_with("fa_cpl0"),
    names_to = c("rang_couple", "variable"),
    names_pattern = "fa_cpl(\\d{2})_(.+)",
    values_to = "valeur"
) %>% 
  pivot_wider(
    names_from = variable,
    values_from = valeur) %>% 
  arrange(id_anonymat, rang_couple) %>% 
  relocate(id_anonymat, rang_couple, all_of(starts_with("conjt")),
           all_of(starts_with("int_")),
           union_nature) %>% 
  filter(!is.na(conjt_nais_an))


## 3.3.5 Identifier les naissances multiples ####

vars_enfants <- M4_enf_F4 %>% 
  select(-all_of(starts_with("fa_")),
         -all_of(starts_with("id_"))) %>% 
  colnames()

M4_enf_F5 <- M4_enf_F4 %>% 
  group_by(id_anonymat, arrivee_an, arrivee_mois) %>% 
  mutate(fa_enf_par_nais = n()) %>% 
  ungroup() %>% 
  group_by(id_anonymat) %>% 
  arrange(arrivee_an, arrivee_mois) %>% 
  mutate(fa_enf_nb = n(),
         rang_fa_enf = case_when(
           fa_enf_nb == 1 ~ "fa_enf01",
           TRUE ~ paste0("fa_enf", sprintf("%02d", row_number()))
         )) %>% 
  ungroup() %>% 
  relocate(id_anonymat, fa_enf_adopte_nb, fa_enf_bio_nb, fa_enf_nb, fa_enf_par_nais, rang_fa_enf, all_of(vars_enfants))

verif <- M4_enf_F5 %>% 
  filter(fa_enf_bio_nb != fa_enf_nb)

ids_verif <- unique(verif$id_anonymat)

test_verif <- M4_enf_F5 %>% 
  filter(id_anonymat %in% ids_verif) %>% 
  arrange(id_anonymat, arrivee_an, arrivee_mois) %>% 
  relocate(id_anonymat, fa_enf_adopte_nb, fa_enf_bio_nb, fa_enf_nb, fa_enf_par_nais, rang_fa_enf, all_of(vars_enfants))


clean <- test_verif %>% 
  slice(c(1,
          3,4,
          5,7,8,
          9,
          10,11,
          12,13,
          14,
          15,
          16,
          17,
          18,19,
          20,21,
          22)) %>% 
  group_by(id_anonymat, arrivee_an, arrivee_mois) %>% 
  mutate(fa_enf_par_nais = n()) %>% 
  ungroup() %>% 
  group_by(id_anonymat) %>% 
  arrange(arrivee_an, arrivee_mois) %>% 
  mutate(fa_enf_nb = n(),
         rang_fa_enf = case_when(
           fa_enf_nb == 1 ~ "fa_enf01",
           TRUE ~ paste0("fa_enf", sprintf("%02d", row_number()))
         )) %>% 
  ungroup() %>% 
  relocate(id_anonymat, fa_enf_adopte_nb, fa_enf_bio_nb, fa_enf_nb, fa_enf_par_nais, rang_fa_enf, all_of(vars_enfants))

M4_enf_F6 <- bind_rows(
  M4_enf_F5 %>% 
    filter(!id_anonymat %in% ids_verif),
  clean
) %>% 
  arrange(id_anonymat)
