# 5.1 QUALITÉ DE VIE ####
M6_F0 <- read_sas("../raw_data/gb_ddb_id_01_qv_06_test.sas7bdat")

## 5.1.1 Remove empty cols ####
M6_F1 <- M6_F0 %>%
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## 5.1.2 Rename cols ####
add_qv <- M6_F1 %>% 
  select(all_of(starts_with("sa_")),
         all_of(starts_with("sc_")),
         all_of(starts_with("am_")),
         ad_besoin,
         ad_lien,
         ad_type) %>% 
  colnames()

M6_F2 <- M6_F1 %>% 
  rename_with(~ paste0("qv_", .x), all_of(add_qv)) %>%
  rename("id_tab_db" = "tab_db",
         "qv_commentaires" = "commentaires_qual",
         "qv_ad_act01" = "VAR11",
         "qv_ad_act02" = "VAR2",
         "qv_ad_act03" = "VAR3",
         "qv_ad_act04" = "VAR4",
         "qv_ad_act05" = "VAR5",
         "qv_ad_act06" = "VAR6",
         "qv_ad_act07" = "VAR7",
         "qv_ad_act08" = "VAR8",
         "qv_ad_act09" = "VAR9",
         "qv_ad_act10" = "VAR10",
         "qv_ad_act999" = "ad_activites_999",
         "qv_ad_act_autre" = "ad_activites_autre") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

### Clean up
rm(list = c("M6_F0",
            "M6_F1",
            "add_qv"))

## 5.1.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M6_F2), names(M1_V9)))
vars_doublons <- union("id_link", setdiff(names(M6_F2), common_vars))

doublons <- M6_F2 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M1_V9 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  identity,
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(clean$id_anonymat, clean$id_link)

M6_F3 <- bind_rows(
  M6_F2 %>% 
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
            "identity"))

# K - M1_V10 ####
common_vars <- intersect(names(M6_F3), names(M1_V9))

verif_join <- anti_join(
  M6_F3,
  M1_V9,
  by = common_vars
) # le df est vide, on peut donc joindre les deux

M1_V10 <- left_join(
  M1_V9,
  M6_F3,
  by = common_vars
)

### Clean up
rm(list = c("common_vars",
            "verif_join"))

write.csv(M1_V10, "../clean_data/thoa_MV10.csv", row.names = F)

data <- read.csv("../clean_data/thoa_MV10.csv")
