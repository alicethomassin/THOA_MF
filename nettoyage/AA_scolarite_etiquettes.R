library(tidyverse)
library(labelled)

M2_V1 <- read.csv("../clean_data/thoa_M2V1.csv")

# 4. Étiquettes des variables ####


M2_V2 <- M2_V1 %>%
  {
    vars_ets <- select(., starts_with("sc_plan_ets")) %>% names()
    labels_ets <- set_names(rep("SC_20", length(vars_ets)), vars_ets)
    set_variable_labels(., !!!labels_ets)
  } %>% 
  {
    vars_notif <- select(., starts_with("sc_plan_notif")) %>% names()
    labels_notif <- set_names(rep("SC_21", length(vars_notif)), vars_notif)
    set_variable_labels(., !!!labels_notif)
  } %>% 
  set_variable_labels(
    id_date_creation = "id_A",
    id_nom = "ID_01",
    id_nom_jeune = "ID_02",
    id_prenom = "ID_03",
    id_anonymat = "id_B",
    id_link = "id_C",
    id_sep = "id_D",
    id_tab_db = "id_E",
    id_sexe = "ID_04",
    id_date_nais = "ID_05",
    id_age = "id_F",
    id_age_cat_2 = "id_G",
    id_age_cat_3 = "id_H",
    id_lieu_nais = "ID_06",
    id_dep_nais = "ID_06",
    id_centre1 = "ID_08",
    id_centre2 = "ID_09",
    id_centre3 = "ID_10",
    id_type = "id_fin",
    sc_debut = "SC_01",
    sc_debut_autre = "sc_01",
    sc_debut_corr = "sc_A",
    sc_debut_an = "SC_02",
    sc_debut_age = "sc_B",
    sc_debut_date = "sc_C",
    sc_redoubl = "SC_03",
    sc_rdb_date_creation = "sc_D",
    sc_rdb_redoubl = "sc_E",
    sc_rdb_cor = "sc_F",
    sc_rdb_cat = "sc_G",
    sc_rdb_nb = "sc_H",
    sc_rdb_type = "sc_rdb_fin",
    sc_interromp = "SC_07",
    sc_int_date_creation = "sc_J",
    sc_int_interromp = "sc_K",
    sc_int_cor = "sc_L",
    sc_int_cat = "sc_M",
    sc_int_nb = "sc_N",
    sc_int_type = "sc_int_fin",
    sc_plan = "SC_18",
    sc_plan_an = "SC_19",
    sc_etspps = "sc_R",
    sc_plan_nb = "sc_S",
    sc_plan_prem_classe = "sc_T",
    sc_plan_demande = "SC_22",
    sc_fin_etudes = "SC_23",
    sc_formation = "SC_24",
    sc_formation_autre = "sc_24",
    sc_diplome = "SC_25",
    sc_diplome_autre = "sc_25",
    sc_diplome_an = "SC_26",
    sc_diplome_date = "sc_U",
    sc_diplome_age = "sc_V",
    sc_diplome_cat = "sc_W",
    sc_commentaires = "SC_comm",
    sc_date_creation = "sc_X",
    sc_id_cat = "sc_Y",
    sc_type = "sc_fin"
  )


etiquettes_renouv <- function(df, premiere, derniere, etiquette){
  liste <- select(df, {{premiere}}:{{derniere}}) %>% names()
  labels <- set_names(rep(etiquette, length(liste)), liste)
  set_variable_labels(df, !!!labels)
}

M2_V3 <- M2_V2 %>% 
  etiquettes_renouv(sc_rdb01_classe, sc_rdb04_classe, "SC_04") %>% 
  etiquettes_renouv(sc_rdb01_classe_cat, sc_rdb04_classe_cat, "sc_I") %>% 
  etiquettes_renouv(sc_rdb01_classe_autre, sc_rdb04_classe_autre, "sc_04") %>% 
  etiquettes_renouv(sc_rdb01_an, sc_rdb04_an, "SC_05") %>% 
  etiquettes_renouv(sc_rdb01_cause, sc_rdb04_cause, "SC_06") %>% 
  etiquettes_renouv(sc_int01_classe, sc_int08_classe, "SC_08") %>% 
  etiquettes_renouv(sc_int01_classe_cat, sc_int08_classe_cat, "sc_O") %>% 
  etiquettes_renouv(sc_int01_classe_autre, sc_int08_classe_autre, "sc_08") %>% 
  etiquettes_renouv(sc_int01_mois, sc_int08_mois, "SC_09") %>% 
  etiquettes_renouv(sc_int01_an, sc_int08_an, "SC_09") %>% 
  etiquettes_renouv(sc_int01_cause_1, sc_int08_cause_1, "SC_10") %>% 
  etiquettes_renouv(sc_int01_cause_2, sc_int08_cause_2, "SC_10") %>% 
  etiquettes_renouv(sc_int01_cause_3, sc_int08_cause_3, "SC_10") %>% 
  etiquettes_renouv(sc_int01_cause_4, sc_int08_cause_4, "SC_10") %>% 
  etiquettes_renouv(sc_int01_cause_999, sc_int08_cause_999, "SC_10") %>% 
  etiquettes_renouv(sc_int01_cause_autre, sc_int08_cause_autre, "sc_10") %>% 
  etiquettes_renouv(sc_int01_repr, sc_int08_repr, "SC_13") %>% 
  etiquettes_renouv(sc_int01_classe_repr, sc_int08_classe_repr, "SC_14") %>% 
  etiquettes_renouv(sc_int01_classerepr_cat, sc_int08_classerepr_cat, "sc_P") %>% 
  etiquettes_renouv(sc_int01_classe_repr_autre, sc_int08_classe_repr_autre, "sc_14") %>% 
  etiquettes_renouv(sc_int01_mois_repr, sc_int08_mois_repr, "SC_15") %>% 
  etiquettes_renouv(sc_int01_an_repr, sc_int08_an_repr, "SC_15") %>% 
  etiquettes_renouv(sc_int01_interromp_duree, sc_int08_interromp_duree, "SC_16") %>% 
  etiquettes_renouv(sc_int01_duree_j, sc_int08_duree_j, "sc_Q") %>% 
  etiquettes_renouv(sc_int01_non_repr, sc_int08_non_repr, "SC_17")

M2_V4 <- M2_V3 %>% 
  relocate(
    id_date_creation,
    id_nom,
    id_nom_jeune,
    id_prenom,
    id_anonymat,
    id_link,
    id_sep,
    id_tab_db,
    id_sexe,
    id_date_nais,
    id_age,
    id_age_cat_2,
    id_age_cat_3,
    id_lieu_nais,
    id_dep_nais,
    id_centre1,
    id_centre2,
    id_centre3,
    id_type,
    sc_debut,
    sc_debut_autre,
    sc_debut_corr,
    sc_debut_an,
    sc_debut_age,
    sc_debut_date,
    sc_redoubl,
    sc_rdb_date_creation,
    sc_rdb_redoubl,
    sc_rdb_cor,
    sc_rdb_cat,
    sc_rdb_nb,
    sc_rdb01_classe:sc_rdb04_classe,
    sc_rdb01_classe_cat:sc_rdb04_classe_cat,
    sc_rdb01_classe_autre:sc_rdb04_classe_autre,
    sc_rdb01_an:sc_rdb04_an,
    sc_rdb01_cause:sc_rdb04_cause,
    sc_rdb_type,
    sc_interromp,
    sc_int_date_creation,
    sc_int_interromp,
    sc_int_cor,
    sc_int_cat,
    sc_int_nb,
    sc_int01_classe:sc_int08_classe,
    sc_int01_classe_cat:sc_int08_classe_cat,
    sc_int01_classe_autre:sc_int08_classe_autre,
    sc_int01_mois:sc_int08_mois,
    sc_int01_an:sc_int08_an,
    sc_int01_cause_1:sc_int08_cause_1,
    sc_int01_cause_2:sc_int08_cause_2,
    sc_int01_cause_3:sc_int08_cause_3,
    sc_int01_cause_4:sc_int08_cause_4,
    sc_int01_cause_999:sc_int08_cause_999,
    sc_int01_cause_autre:sc_int08_cause_autre,
    sc_int01_repr:sc_int08_repr,
    sc_int01_classe_repr:sc_int08_classe_repr,
    sc_int01_classerepr_cat:sc_int08_classerepr_cat,
    sc_int01_classe_repr_autre:sc_int08_classe_repr_autre,
    sc_int01_mois_repr:sc_int08_mois_repr,
    sc_int01_an_repr:sc_int08_an_repr,
    sc_int01_interromp_duree:sc_int08_interromp_duree,
    sc_int01_duree_j:sc_int08_duree_j,
    sc_int01_non_repr:sc_int08_non_repr,
    sc_int_type,
    sc_plan,
    sc_plan_an,
    sc_plan_ets1_1:sc_plan_ets1_14,
    sc_plan_ets2_1:sc_plan_ets2_14,
    sc_plan_ets3_1:sc_plan_ets3_14,
    sc_plan_ets4_1:sc_plan_ets4_14,
    sc_plan_ets5_1:sc_plan_ets5_14,
    sc_etspps,
    sc_plan_nb,
    sc_plan_prem_classe,
    sc_plan_notification_1:sc_plan_notification_8,
    sc_plan_notification_999,
    sc_plan_notification_autre,
    sc_plan_demande,
    sc_fin_etudes,
    sc_formation,
    sc_formation_autre,
    sc_diplome,
    sc_diplome_autre,
    sc_diplome_an,
    sc_diplome_date,
    sc_diplome_age,
    sc_diplome_cat,
    sc_commentaires,
    sc_date_creation,
    sc_id_cat,
    sc_type
  )

data <- M2_V4

save(data, file = "../clean_data/thoa_scolarite_etiquettes.RData", compress = F)
load(file = "../clean_data/thoa_scolarite_etiquettes.RData")
