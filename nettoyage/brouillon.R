## 1.3 recoder variables ####
### 1.3.1 Mettre labels ####


test <- M2_F2 %>% 
  select(id_anonymat, id_age, sc_debut)

look_for(test)

test %>% dfSummary() %>% view()

# Modification de la fonctions dfSummary() pour remplacer la dernière ligne par
# Q1 et Q3
st_options(
  dfSummary.custom.1 = 
    expression(
      paste(
        "Q1 - Q3 :",
        round(
          quantile(column_data, probs = .25, type = 2, 
                   names = FALSE, na.rm = TRUE), digits = 1
        ), " - ",
        round(
          quantile(column_data, probs = .75, type = 2, 
                   names = FALSE, na.rm = TRUE), digits = 1
        )
      )
    )
)

dfSummary(M1_identity, 
          plain.ascii  = FALSE, 
          style        = "grid", 
          graph.magnif = 0.82, 
          varnumbers   = FALSE,
          valid.col    = FALSE,
          tmp.img.dir  = "/tmp") %>% view()

dfSummary(M2_F5, 
          plain.ascii  = FALSE, 
          style        = "grid", 
          graph.magnif = 0.82, 
          varnumbers   = FALSE,
          valid.col    = FALSE,
          tmp.img.dir  = "/tmp") %>% view()


# Sélectionner variables à mettre en integer
vars_int <- M2_F2 %>% 
  select(
    id_centre1, id_centre2, id_centre3, id_dep_nais, id_lieu_nais, id_sexe,
    id_age, id_tab_db, sc_diplome_an, sc_diplome, sc_fin_etudes, sc_formation,
    sc_debut_an, sc_debut, sc_interromp, sc_redoubl, sc_debut_corr, 
    sc_debut_age, sc_diplome_age, starts_with("sc_plan"), -sc_plan_notification_autre) %>% 
  colnames()

# Sélectionner les variables à traiter comme des choix multiples
vars_choix_multiple <- M2_F2 %>% 
  select(starts_with("sc_plan_ets"),
         starts_with("sc_plan_notifi"),
         -sc_plan_notification_autre) %>% 
  colnames()

# Sélectionner les variables pour extraireune partie du titre et en faire
# de nouvelles variables pour connaître le nombre de plans d'intervention
# ainsi que la classe de ce premier plan
vars_ets <- M2_F2 %>% 
  select(starts_with("sc_plan_ets")) %>% 
  names()

classe_index <- as.integer(str_extract(vars_ets, "\\d+$"))

recode_year1 <- function(df, borne, var_year){
  # Dans le cas où il y a une question borne
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{borne}} == 1 & is.na({{var_year}}) ~ 5555L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 7777L,       # Pas concerné
        is.na({{borne}}) ~ NA_integer_,               # Pas participé
        TRUE ~ {{var_year}}                           # Réponse renseignée
      )
    )
}

recode_month1 <- function(df, borne, var_month){
  
  df %>% 
    mutate(
      {{var_month}} := case_when(
        {{borne}} == 1 & is.na({{var_month}}) ~ 55L, # Concerné, mais pas répondu
        {{borne}} == 0 | {{borne}} > 1 ~ 77L,        # Pas concerné
        is.na({{borne}}) ~ NA_integer_,              # Pas participé
        TRUE ~ {{var_month}}                         # Réponse renseignée
      ))
}

recode_year2 <- function(df, type, var_year){
  # Dans le cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_year}} := case_when(
        {{type}} == "P" & is.na({{var_year}}) ~ 5555L,       # Concerné, mais pas répondu
        {{type}} != "P" & is.na({{var_year}}) ~ NA_integer_, # Pas participé   
        TRUE ~ {{var_year}}                                  # Réponse renseignée
      )
    )
}

recode_month2 <- function(df, type, var_month){
  # Dans la cas où question NA non bornée (ex DDN des parents vide, mais quand
  # même participé au module)
  df %>% 
    mutate(
      {{var_month}} := case_when(
        {{type}} == "P" & is.na({{var_month}}) ~ 55L,         # Concerné, mais pas répondu
        {{type}} != "P" & is.na({{var_month}}) ~ NA_integer_, # Pas participé
        TRUE ~ {{var_month}}                                  # Réponse renseignée
      )
    )
}

recode_qnorm <- function(df, type, var_miss){
  # Pour les questions d'un module hors bornes, donc obligatoires
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{type}} == "P" & is.na({{var_miss}}) ~ 555L,        # Pas répondu
        {{type}} != "P" & is.na({{var_miss}}) ~ NA_integer_, # Pas participé module
        TRUE ~ {{var_miss}}                                  # Réponse enregistrée
      )
    )
}

recode_qborned <- function(df, borne, var_miss){
  # Pour les questions bornées au sein d'un module
  df %>% 
    mutate(
      {{var_miss}} := case_when(
        {{borne}} == 1 & is.na({{var_miss}}) ~ 444L,  # Concerné, pas répondu
        {{borne}} == 555 & is.na({{var_miss}}) ~ 555L,# Pas répondu
        {{borne}} == 0  & is.na({{var_miss}}) |
          {{borne}} > 1 & is.na({{var_miss}}) ~ 777L, # Pas concerné, pas répondu
        is.na({{borne}}) ~ NA_integer_,               # Pas participé module
        TRUE ~ {{var_miss}}                           # Réponse renseignée
      )
    )
}

recode_qcm <- function(df, borne, list_vars){
  
  df %>% 
    mutate(
      across(
        .cols = all_of( {{list_vars}} ),
        .fns = ~ case_when(
          {{borne}} == 1 & is.na(.) ~ 0L,
          {{borne}} == 444 ~ 444L,
          {{borne}} == 0 | {{borne}} > 1 ~ 777L,
          is.na({{borne}}) ~ NA_integer_,
          TRUE ~ .
        )
      )
    )
  
}

M2_F3 <- M2_F2 %>%
  mutate(
    across(
      .cols = all_of(vars_int),
      .fns = ~ as.integer(.)
    )) %>%
  mutate(
    sc_plan_classe = case_when(
      sc_plan == 0 ~ 777L,
      sc_plan == 555 ~ 555L,
      sc_plan == 444 ~ 444L,
      is.na(sc_plan) ~ NA_integer_,
      sc_plan == 1 ~ pmap_int(
        .l = select(., all_of(vars_ets)),
        .f = function(...) {
          row <- c(...)  # transforme les arguments en vecteur ligne
          first_class <- classe_index[which(row == 1)]
          if (length(first_class) == 0) 0 else min(first_class)
        }
      )
    ),
    sc_plan_nb = case_when(
      sc_plan == 1 ~ as.integer(rowSums(select(., all_of(vars_ets)) == 1, na.rm = T)),
      sc_plan == 0 ~ 777L,
      is.na(sc_plan) ~ NA_integer_
    )) 

vle_var <- M2_F3 %>% 
  relocate(sc_type, sc_debut, sc_redoubl, sc_interromp,
           sc_plan, sc_plan_an, sc_plan_classe, sc_plan_nb, sc_plan_demande, 
           sc_fin_etudes, sc_formation, sc_formation_autre, sc_diplome,
           sc_diplome_autre, sc_diplome_an)

M2_F4 <- M2_F3 %>% 
  recode_year1(sc_plan, sc_plan_an) %>%
  recode_qcm(sc_plan, vars_choix_multiple) %>% 
  recode_qnorm(sc_type, sc_plan_demande) 


verif <- M2_F3 %>% 
  relocate(sc_type, sc_debut, sc_debut_autre, sc_debut_corr, sc_debut_an, sc_debut_age, sc_debut_date,
           sc_plan, sc_plan_classe, sc_plan_nb, sc_plan_an, sc_plan_demande, all_of(vars_choix_multiple))


sc_P <- M2_F3 %>% 
  relocate(sc_type, all_of(quest_norm))

quest_norm <- M2_F1 %>% 
  select(sc_debut, sc_debut_an, sc_redoubl, sc_interromp, sc_plan, sc_plan_demande,
         sc_fin_etudes) %>% 
  colnames()


sc_empties <- M2_F2 %>%
  select(id_anonymat, sc_type, all_of(quest_norm)) %>% 
  mutate(n = n_miss_row(across(all_of(quest_norm)))) %>% 
  relocate(id_anonymat, sc_type, n)





# 5. Qualité de vie ####
M6_F0 <- read_sas("../raw_data/gb_ddb_id_01_qv_06_test.sas7bdat")

## 3.1 Réduire et renommer #### 
# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P6_F0 <- M6_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation,
         setdiff(names(.), names(M2_F0)))

## 5.1.2 Rename cols ####
add_qv <- P6_F0 %>% 
  select(all_of(starts_with("sa_")),
         all_of(starts_with("sc_")),
         all_of(starts_with("am_")),
         ad_besoin,
         ad_lien,
         ad_type) %>% 
  colnames()

P6_F1 <- P6_F0 %>% 
  rename_with(~ paste0("qv_", .x), all_of(add_qv)) %>%
  rename("qv_commentaires" = "commentaires_qual",
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
         "qv_ad_act_autre" = "ad_activites_autre")