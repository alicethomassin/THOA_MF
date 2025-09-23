# 3.4 FRATRIE ####
M4_frt_F0 <- read_sas("../raw_data/gb_ddb_fa_04_fratrie_04.sas7bdat")

## 3.4.1 Remove empty cols ####
M4_frt_F1 <- M4_frt_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
 # remove_empty("cols") %>% 
  select(-fratrie_pf_fratrie)

## 3.4.2 Rename cols ####
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
  mutate(
    id_date_nais = case_when(
      id_anonymat %in% id_DDN ~ DDN,
      TRUE ~ id_date_nais),
    arrivee_an = case_when(
      is.na(adopte_an) ~ nais_an,
      TRUE ~ adopte_an),
    arrivee_mois = case_when(
      is.na(adopte_mois) ~ nais_mois,
      TRUE ~ adopte_mois
    )) %>% 
  ungroup()

### Clean up
rm(list = c("M4_frt_F0",
            "M4_frt_F1",
            "vars_add_fa",
            "vars_sub_pf",
            "vars_fratrie",
            "vars_sub_sf"))

## 3.3.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M4_frt_F2), names(M1_V7)))
vars_doublons <- union("id_link", setdiff(names(M4_frt_F2), common_vars))  

doublons <- M4_frt_F2 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- M1_V7 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  relationship = "many-to-many",
  by = "id_link"
)

## Verif doublons dans l'union
vars_fratrie <- M4_frt_F2 %>% 
  select(-all_of(starts_with("id_")),
         -all_of(starts_with("fa_"))) %>% 
  colnames()

verif_clean <- bind_rows(
  M4_frt_F2 %>% 
    filter(id_anonymat %in% clean$id_anonymat),
  clean,
  .id = "source"
) %>% 
  arrange(id_anonymat, arrivee_an, arrivee_mois) %>% 
  relocate(id_anonymat, source, id_date_nais, fa_rang_nais, 
           arrivee_an, arrivee_mois, prenom, diag_avt_nais, muco, issu, 
           fa_fratrie_bio_nb, fa_fratrie_demi_nb, all_of(vars_fratrie))

corr <- verif_clean %>% 
  slice(c(1,3,
          5,7,9,
          11,13,
          15,
          17,
          19,
          21:30,32,
          33,35,37,39,
          41,43,
          45,
          48,49,50,
          53,54,
          55,
          56,
          59,
          60,62,64,66,68,70))

ids_TPO <- unique(union(corr$id_anonymat, doublons$id_anonymat))

M4_frt_F3 <- bind_rows(
  M4_frt_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  corr %>% 
    select(-source)
) %>% 
  arrange(id_anonymat, arrivee_an, arrivee_mois) %>% 
  relocate(id_anonymat, id_date_nais, fa_rang_nais, 
           arrivee_an, arrivee_mois, prenom, diag_avt_nais, muco, issu, 
           fa_fratrie_bio_nb, fa_fratrie_demi_nb, fa_fratrie_adopte_nb,
           all_of(vars_fratrie)) %>% 
  filter(!is.na(fa_fratrie_date_creation))

### Clean up
rm(list = c("common_vars",
            "vars_doublons",
            "doublons",
            "list_doublons",
            "identity",
            "clean",
            "verif_clean",
            "corr",
            "ids_TPO"))

## 3.3.4 Événements distincts ####
test_event <- M4_frt_F3 %>% 
  group_by(id_anonymat, arrivee_an, prenom) %>%
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) %>% 
  ungroup()

rm(test_event)

## 3.3.5 Identité unique ####

vars_fratrie <- M4_frt_F3 %>% 
  select(-all_of(starts_with("id_")),
         -all_of(starts_with("fa_"))) %>% 
  colnames()

vars_fixe <- setdiff(setdiff(names(M4_frt_F3), vars_fratrie), "id_anonymat")

test_identity_cols <- M4_frt_F3 %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(vars_fixe), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

rm(list = c("vars_fratrie",
            "vars_fixe",
            "test_identity_cols"))

## 3.3.6 Identifier le rang de la fratrie ####

M4_frt_F3 %>%
  group_by(id_anonymat) %>% 
  summarise(nb_frt = n()) %>% 
  summary(nb_frt) %>% 
  view()













