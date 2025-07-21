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
