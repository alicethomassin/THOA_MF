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
    nais_an = case_when(
      is.na(nais_an) & deces_an == 2003 ~ 1943,
      TRUE ~ nais_an),
    arrivee_an = case_when(
      !is.na(adopte_an) ~ adopte_an,
      !is.na(nais_an) ~ nais_an,
      !is.na(deces_an) ~ deces_an,
      TRUE ~ NA_integer_),
    arrivee_mois = case_when(
      !is.na(adopte_mois) ~ adopte_mois,
      !is.na(nais_mois) ~ nais_mois,
      !is.na(deces_mois) ~ deces_mois,
      TRUE ~ NA_integer_
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
  relocate(id_anonymat, source, id_date_creation, fa_fratrie_date_creation, id_date_nais, fa_rang_nais, 
           arrivee_an, arrivee_mois, prenom, diag_avt_nais, muco, issu, 
           fa_fratrie_bio_nb, fa_fratrie_demi_nb, all_of(vars_fratrie))

corr <- verif_clean %>% 
  slice(c(1,3,
          5,7,9,
          11,13,
          15,
          17,
          19,
          21:31,
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

M4_frt_F4 <- M4_frt_F3 %>% 
  mutate(id_nais_an = year(id_date_nais),
         id_nais_mois = month(id_date_nais)) %>%
  group_by(id_anonymat) %>% 
  arrange(arrivee_an, arrivee_mois) %>% 
  mutate(
    fa_frt_nb = n(),
    fa_frt_older = case_when(
      arrivee_an < id_nais_an | deces_an < id_nais_an ~ 1,
      arrivee_an > id_nais_an | deces_an > id_nais_an ~ 0)) %>% 
  group_by(id_anonymat, fa_frt_older) %>% 
  arrange(arrivee_an, arrivee_mois) %>% 
  mutate(
    rang_fa_frt = case_when(
      fa_frt_nb == 1 & fa_frt_older == 1 ~ "fa_frtO01",
      fa_frt_nb == 1 & fa_frt_older == 0 ~ "fa_frtY01",
      fa_frt_older == 1 ~ paste0("fa_frtO", sprintf("%02d", row_number())),
      fa_frt_older == 0 ~ paste0("fa_frtY", sprintf("%02d", row_number()))
    )
  )

M4_frt_F4 %>% 
  relocate(id_anonymat, fa_commentaires, fa_frt_nb, rang_fa_frt, fa_frt_older, id_nais_an, arrivee_an) %>% 
  arrange(id_anonymat, fa_frt_nb, rang_fa_frt) %>% 
  view()

decede <- M4_frt_F4 %>% 
  filter(decede == 1) %>% 
  relocate(id_anonymat, fa_frt_nb, rang_fa_frt, fa_frt_older, id_nais_an, arrivee_an, fa_commentaires) %>%
  arrange(id_anonymat, fa_frt_nb, rang_fa_frt) %>% 
  filter(is.na(rang_fa_frt))

ids_verif <- unique(decede$id_anonymat)

decedes <- M4_frt_F4 %>% 
  filter(id_anonymat %in% ids_verif) %>% 
  relocate(id_anonymat, fa_frt_nb, rang_fa_frt, fa_fratrie_demi_nb, 
          fa_fratrie_adopte_nb,  fa_frt_older, id_nais_an, arrivee_an, fa_commentaires) %>%
  arrange(id_anonymat, fa_frt_nb, rang_fa_frt)

decede_clean <- decedes %>% 
  mutate(
    fa_frt_older = case_when(
      id_anonymat == "GJLAU" ~ 1,
      id_anonymat == "NHKVC" & is.na(fa_frt_older) ~ 1
    ),
    rang_fa_frt = case_when(
      id_anonymat == "GJLAU" ~ "fa_frtO01"
      id_anonymat == "NHKVC" & is.na(rang_fa_frt) ~ "fa_frtO03"
    )
  )

# je ne sais pas quoi faire ici. Je vais arrêter et demander à Gil.
# option A) choisir l'ordre de la fratrie par induction
# option B) donner un ordre aléatoire
# option C) les retirer pcq on peut pas traiter la fratrie avec les infos manquantes





