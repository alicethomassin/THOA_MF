# 3.1 FAMILLE ####
M4_F0 <- read_sas("../raw_data/gb_ddb_id_01_fa_04.sas7bdat")

## 3.1.2 Remove empty cols ####
M4_F1 <- M4_F0 %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  remove_empty("cols")
