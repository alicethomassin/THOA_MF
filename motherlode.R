# M2 REDOUBLEMENT ####
common_vars <- intersect(names(M2_F3), names(M2_rdb_W1))

verif_join <- anti_join(
  M2_rdb_W1,
  M2_F3,
  by = common_vars
)

M1_V1 <- left_join(
  M2_F3,
  M2_rdb_W1,
  by = common_vars
) %>% 
  group_by(id_anonymat) %>% 
  mutate(
    id_age_cat = case_when(
      (is.na(id_age_cat) & id_age < 18) ~ "Adolescent",
      (is.na(id_age_cat) & id_age > 17) ~ "Adulte",
      TRUE ~ id_age_cat
    )) %>% 
  ungroup()

# Nettoyer l'environnement
rm(list = c("common_vars",
            "M2_F3",
            "M2_rdb_W1",
            "verif_join"))

# M2 INTERRUPTIONS ####
common_vars <- intersect(names(M1_V1), names(M2_int_W1))

verif_join <- anti_join(
  M2_int_W1,
  M1_V1,
  by = common_vars
)

M1_V2 <- left_join(
  M1_V1,
  M2_int_W1,
  by = common_vars
)

# Nettoyer l'environnement
rm(list = c("common_vars",
            "M1_V1",
            "M2_int_W1",
            "verif_join"))