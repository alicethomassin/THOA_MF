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

## 3.1.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M4_F2), names(M1_V4)))
vars_doublons <- union("id_link", setdiff(names(M4_F2), common_vars))

doublons <- M4_F2 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M1_V4 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  identity,
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(clean$id_anonymat, clean$id_link)

M4_F3 <- bind_rows(
  M4_F2 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean %>% 
    select(-id_link)
) %>% 
  arrange(id_anonymat) %>% 
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
            "M4_F2"))

# F - M1_V4 ####
common_vars <- intersect(names(M4_F3), names(M1_V4))

## f - corriger cas unique DDN ####
verif_join <- anti_join(
  M4_F3,
  M1_V4,
  by = common_vars
) # J'ai vu que le rpoblème était la DDn d'une personne. J'ai donc transposé
# la colonne des DDN de M1 à M4

YUVJT <- M1_V4 %>% 
  select(id_anonymat, id_date_nais) %>% 
  filter(id_anonymat %in% verif_join)

id_DDN <- YUVJT$id_anonymat
DDN <- YUVJT$id_date_nais

M4_F4 <- left_join(
  M4_F3 %>% 
    select(-id_date_nais),
  M1_V4 %>% 
    select(id_anonymat, id_date_nais),
  by = "id_anonymat"
)

verif_join <- anti_join(
  M4_F4,
  M1_V4,
  by = common_vars
) # Le df est vide on peut donc joindre les deux

M1_V5 <- left_join(
  M1_V4,
  M4_F4,
  by = common_vars
)

### Clean up
rm(list = c("common_vars",
            "verif_join",
            "M4_F3"))
