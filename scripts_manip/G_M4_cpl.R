# 3.2 COUPLES ####
M4_cpl_F0 <- read_sas("../raw_data/gb_ddb_fa_04_couple_04.sas7bdat")

## 3.2.1 Remove empty cols ####
M4_cpl_F1 <- M4_cpl_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols") %>%
  rowwise() %>% 
  mutate(
    sf_conjt_an_nais = case_when(
      is.na(sf_conjt_an_nais) & !is.na(sf_an_conjt_nais) ~ sf_an_conjt_nais,
      TRUE ~ sf_conjt_an_nais
  )) %>% 
  ungroup() %>% 
  select(-sf_an_conjt_nais,
         -couple_sf_couple_1an) # Retirer vars inutiles

## 3.2.2 Rename cols ####
vars_add_fa <- M4_cpl_F1 %>% 
  select(all_of(starts_with("fo_")),
         couple_date_creation,
         couple_type) %>% 
  colnames()

vars_sub_pf <- M4_cpl_F1 %>% 
  select(all_of(starts_with("pf_mere_")),
         all_of(starts_with("pf_pere_"))) %>% 
  colnames()

vars_sub_sf <- M4_cpl_F1 %>% 
  select(all_of(starts_with("sf_")),
         -all_of(contains("_conjt")),
         -all_of(contains("interromp")),
         -sf_nature_union,
         -sf_raison_muco) %>% 
  colnames()

vars_conjt <- M4_cpl_F1 %>% 
  select(sf_conjt_enf,
         sf_conjt_muco,
         sf_conjt_prenom,
         sf_conjt_sexe) %>% 
  colnames()

M4_cpl_F2 <- M4_cpl_F1 %>% 
  rename_with(~ paste0("fa_", .x), all_of(vars_add_fa)) %>%
  rename_with(~ gsub("^pf_", "fa_", .x), all_of(vars_sub_pf)) %>%
  rename_with(~ gsub("^sf_", "fa_", .x), all_of(vars_sub_sf)) %>%
  rename_with(~ gsub("^sf_", "", .x), all_of(vars_conjt)) %>%
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
         "fa_fratrie" = "pf_fratrie",
         "int_an" = "sf_an_interromp",
         "int" = "sf_interromp",
         "int_par" = "sf_interromp_par",
         "int_mois" = "sf_mois_interromp",
         "union_nature" = "sf_nature_union",
         "int_muco" = "sf_raison_muco",
         "conjt_union_an" = "sf_conjt_an_union",
         "conjt_union_mois" = "sf_conjt_mois_union",
         "conjt_nais_an" = "sf_conjt_an_nais",
         "conjt_nais_mois" = "sf_conjt_mois_nais") %>% 
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
rm(list = c("M4_cpl_F0",
            "M4_cpl_F1",
            "vars_add_fa",
            "vars_sub_pf",
            "vars_sub_sf",
            "vars_conjt"))

## 3.2.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M4_cpl_F2), names(M1_V5)))
vars_doublons <- union("id_link", setdiff(names(M4_cpl_F2), common_vars))

doublons <- M4_cpl_F2 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- M1_V5 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  doublons %>% 
    select(all_of(vars_doublons)),
  identity,
  relationship = "many-to-many",
  by = "id_link"
)

#### Verif double couple ####

vars_couple <- M4_cpl_F2 %>% 
  select(-all_of(starts_with("fa_")),
         -all_of(starts_with("id_"))) %>% 
  colnames()

comparaison <- bind_rows(
  clean,
  M4_cpl_F2 %>% 
    filter(id_anonymat %in% clean$id_anonymat),
  .id = "source"
) %>%
  arrange(id_anonymat, conjt_union_an, conjt_union_mois) 

verif_couples <- comparaison %>%
  group_by(id_anonymat, conjt_nais_an, conjt_nais_mois, conjt_union_an, conjt_union_mois) %>% 
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) 

couples_corr <- comparaison %>% 
  filter(id_anonymat %in% verif_couples$id_anonymat) %>% 
  relocate(id_anonymat, all_of(vars_couple), source) %>% 
  slice(-c(1, 4, 6, 9, 11, 14, 15, 18)) %>% 
  select(-source)

couples_clean <- bind_rows(
  clean %>% 
    filter(!id_anonymat %in% couples_corr$id_anonymat),
  couples_corr
)

ids_TPO <- unique(union(couples_clean$id_anonymat, couples_clean$id_link))

M4_cpl_F3 <- bind_rows(
  M4_cpl_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  couples_clean
) %>% 
  filter(fa_couple_type == "P") %>% 
  arrange(id_anonymat, conjt_union_an, conjt_union_mois) %>% 
  group_by(id_anonymat) %>% 
  mutate(
    fa_couple_date_creation = case_when(
      n_distinct(fa_couple_date_creation) > 1 ~ id_date_creation,
      TRUE ~ fa_couple_date_creation),
    fa_couple_cat = case_when(
      n_distinct(fa_couple_cat) > 1 ~ "fa_04_couple_04",
      TRUE ~ fa_couple_cat
    )) %>% 
  ungroup()

### Clean up
rm(list = c("common_vars",
            "vars_doublons",
            "doublons",
            "list_doublons",
            "identity",
            "clean",
            "vars_couple",
            "comparaison",
            "verif_couples",
            "couples_corr",
            "couples_clean",
            "ids_TPO",
            "M4_cpl_F2"))

## 3.2.4 Événements distincts ####
test_event <- M4_cpl_F3 %>% 
  group_by(id_anonymat, conjt_union_an, conjt_union_mois, conjt_nais_an) %>%
  mutate(n_comb = n()) %>% 
  filter(n_comb > 1) %>% 
  arrange(id_anonymat, conjt_union_an, conjt_union_mois, conjt_nais_an) %>%
  ungroup()

rm(test_event)

## 3.2.5 Identité unique ####
vars_wider <- M4_cpl_F3 %>% 
  select(-all_of(starts_with("fa_")),
         -all_of(starts_with("id_"))) %>% 
  colnames()

vars_fixe <- setdiff(setdiff(names(M4_cpl_F3), vars_wider), "id_anonymat")

test_identity_cols <- M4_cpl_F3 %>% 
  group_by(id_anonymat) %>% 
  summarise(across(all_of(vars_fixe), ~ n_distinct(.) > 1)) %>% 
  rowwise() %>% 
  filter(sum(c_across(-id_anonymat)) > 0) %>% 
  select(id_anonymat, where(~ !all(. == FALSE)))

rm(test_identity_cols)

