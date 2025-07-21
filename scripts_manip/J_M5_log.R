# 4.1 LOGEMENT ####
M5_F0 <- read_sas("../raw_data/gb_ddb_id_01_lo_05.sas7bdat")

## 4.1.1 Remove empty cols ####
M5_F1 <- M5_F0 %>%
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")

## 4.1.2 Rename cols ####
vars_sub_la <- M5_F1 %>% 
  select(all_of(starts_with("la_"))) %>% 
  colnames()

M5_F2 <- M5_F1 %>% 
  rename_with(~ gsub("^la_", "lo_", .x), all_of(vars_sub_la)) %>% 
  rename("id_tab_db" = "tab_db",
         "lo_commentaires" = "commentaires_loge",
         "lo_dep_situ_centre1" = "dep_la_situ_centre1",
         "lo_dep_centre1" = "dep_centre1") %>% 
    mutate(across(
      .cols = where(is.character),
      .fns = ~ na_if(., "")
    ))

### Clean up 
rm(list = c("M5_F0",
            "M5_F1",
            "vars_sub_la"))

## 4.1.3 Remove duplicates ####
common_vars <- union("id_link", intersect(names(M5_F2), names(M1_V7)))
vars_doublons <- union("id_link", setdiff(names(M5_F2), common_vars))

doublons <- M5_F2 %>% 
  filter(is.na(id_date_creation)) %>% 
  mutate(id_link = id_anonymat)

list_doublons <- unique(doublons$id_link)

identity <- M1_V7 %>% 
  filter(id_link %in% list_doublons) %>% 
  select(all_of(common_vars))

clean <- left_join(
  identity,
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(clean$id_anonymat, clean$id_link)

M5_F3 <- bind_rows(
  M5_F2 %>% 
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

