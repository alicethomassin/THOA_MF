# 4. LOGEMENT ####
M5_F0 <- haven::read_sas("../raw_data/gb_ddb_id_01_lo_05.sas7bdat")

# Réduire aux colonnes qui ne concernent que ce module et créer id_link
P5_F0 <- M5_F0 %>% 
  make_id_link(.) %>% 
  select(id_anonymat, id_link, id_date_creation, id_lieu_nais, id_dep_nais,
         setdiff(names(.), names(M2_F0)), -fa_amenag)

## 4.1 Rename cols ####
vars_sub_la <- P5_F0 %>% 
  select(all_of(starts_with("la_"))) %>% 
  colnames()

P5_F1 <- P5_F0 %>% 
  rename_with(~ gsub("^la_", "lo_", .x), all_of(vars_sub_la)) %>% 
  rename("lo_commentaires" = "commentaires_loge",
         "lo_dep_situ_centre1" = "dep_la_situ_centre1",
         "lo_dep_centre1" = "dep_centre1") %>% 
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  ))

## 4.2 Corriger doublons ####
doublons <- P5_F1 %>% 
  filter(is.na(id_date_creation))

list_doublons <- unique(doublons$id_link)

identity <- P5_F1 %>% 
  filter(id_link %in% list_doublons,
         !is.na(id_date_creation)) %>% 
  relocate(id_link) %>% 
  arrange(id_link)

vars_identity <- identity %>% 
  select(all_of(starts_with("id_")), lo_dep_centre1, lo_dep_situ_centre1) %>% 
  colnames()

vars_doublons <- union("id_link", setdiff(names(P5_F1), vars_identity))

clean <- full_join(
  identity %>% 
    select(all_of(vars_identity)),
  doublons %>% 
    select(all_of(vars_doublons)),
  by = "id_link"
)

ids_TPO <- union(list_doublons, clean$id_anonymat)

P5_F2 <- bind_rows(
  P5_F1 %>% 
    filter(!id_anonymat %in% ids_TPO),
  clean
) %>% 
  arrange(id_anonymat) %>% 
  mutate(
    id_lo_cat = case_when(
      id_lo_cat == "id_01" & !is.na(lo_date_creation) ~ "id_01_lo_05",
      TRUE ~ id_lo_cat
    )
  )

# CLean up
rm(list = c("clean",
            "doublons",
            "identity",
            "ids_TPO",
            "list_doublons",
            "vars_doublons",
            "vars_identity"))

## 4.3 Corriger valeurs manquantes ####

P5_F3 <- P5_F2 %>% 
  relocate(
    id_date_creation,
    id_anonymat, 
    id_link,
    lo_type,
    lo_an_logement,
    lo_situ_logement,
    lo_situ_dep,
    id_dep_nais,
    lo_situ_pays,
    id_lieu_nais,
    lo_dep_centre1,
    lo_dep_situ_centre1,
    lo_type_logement,
    lo_type_logement_autre,
    lo_occup_logement,
    lo_occup_logement_autre,
    lo_occup_cause_1:lo_occup_cause_999,
    lo_occup_cause_autre,
    lo_amenag_logement,
    lo_amenag_logement_precis,
    lo_emprunt_1:lo_emprunt_6,
    lo_emprunt_precis,
    lo_demenag,
    lo_demenag_cause_1:lo_demenag_cause_999,
    lo_demenag_cause_autre,
    lo_commentaires,
    lo_date_creation
  )

rcd_qcm2 <- function(df, borne, vars){
  df %>%
    mutate(
      all_na_vars = if_all({{vars}}, is.na)
    ) %>% 
    mutate(
      across(
        {{vars}},
        ~ as.integer(case_when(
          is.na({{borne}}) ~ NA_integer_,
          !is.na({{borne}}) & all_na_vars ~ 555L,
          {{borne}} == "P" & is.na(.x) ~ 0L,
          TRUE ~ as.integer(.x)                            
        ))
      )
    ) %>% 
    select(-all_na_vars)
}

vars_Q5 <- P5_F3 %>% 
  select(lo_occup_cause_1:lo_occup_cause_999) %>% 
  colnames()

vars_Q8 <- P5_F3 %>% 
  select(lo_emprunt_1:lo_emprunt_6) %>% 
  colnames()

P5_F4 <- P5_F3 %>% 
  group_by(id_anonymat) %>% 
  rcd_vars(lo_situ_logement, lo_type) %>%
  rcd_years(lo_an_logement, lo_type) %>% 
  rcd_vars(lo_situ_dep, lo_type) %>% 
  mutate(
    lo_situ_pays = case_when(
      (is.na(lo_situ_pays) & !is.na(lo_situ_logement)) ~ lo_situ_logement,
      is.na(lo_type) ~ NA_integer_,
      TRUE ~ lo_situ_pays
    )
  ) %>% 
  rcd_vars(c(lo_type_logement, lo_occup_logement), lo_type) %>% 
  rcd_qcm2(lo_type, all_of(vars_Q5)) %>% 
  rcd_qcm2(lo_type, all_of(vars_Q8))

P5_F3 %>% 
  dfSummary() %>% 
  view()

P5_F4 %>% 
  ungroup() %>% 
  dfSummary() %>% 
  view()

test_miss <- P5_F3 %>% 
  filter(!is.na(lo_type)) %>%
  select(id_anonymat, lo_an_logement:lo_commentaires) %>% 
  miss_case_summary() %>% 
  arrange(case)

vars_chr <- P5_F3 %>% 
  select(starts_with("lo_"), -where(is.character), -where(is.double)) %>% 
  colnames()

test_enc <- P5_F3 %>% 
  mutate(
    nb_miss = rowSums(is.na(select(., all_of(vars_chr))))) %>% 
  relocate(nb_miss)

test_enc2 <- P5_F3 %>%
  mutate(
    nb_miss = if (length(vars_chr) == 0) {
      0
    } else {
      rowSums(is.na(select(., all_of(vars_chr))))
    }
  ) %>%
  relocate(nb_miss)

test_enc3 <- P5_F3 %>%
  rowwise() %>%
  mutate(nb_miss = sum(is.na(c_across(all_of(vars_chr))))) %>%
  ungroup() %>%
  relocate(nb_miss)

tst2 <- P5_F3 %>%
  rowwise() %>% 
  mutate(
    nb_ok = count(complete.cases(c_across(all_of(vars_chr))))
  )

tst2 <- rowSums(is.na(P5_F3) | P5_F3 == "")

P5_F4 <- P5_F3 %>% 
  rowwise() %>% 
  rcd_years(lo_an_logement, lo_type) %>% 
  rcd_vars(lo_situ_logement, lo_type) %>%
  mutate(
    lo_situ_pays_cor = case_when(
      is.na(lo_type) ~ NA_integer_,
      is.na(lo_situ_pays) & is.na(lo_situ_dep) ~ 555L,
      !is.na(lo_situ_dep) ~ 1,
      TRUE ~ lo_situ_pays
      )
    ) %>% 
  mutate(lo_dep_situ_centre1 = case_when(
    lo_situ_dep == lo_dep_centre1 ~ "Même département",
    id_dep_nais == lo_dep_centre1 ~ "Département de naissance",
    is.na(lo_dep_centre1) ~ "Pas déclaré de centre",
    (lo_situ_dep != lo_dep_centre1 & id_dep_nais != lo_dep_centre1) ~ "Départements différents"
  ))
  

  
   mutate(
    lo_traitement_geo = case_when(
      is.na(lo_type) & lo_dep_centre1 == id_dep_nais ~ 22L,
      lo_situ_dep == lo_dep_centre1 ~ 1L,
      id_dep_nais == lo_dep_centre1 ~ 2L,
      is.na(lo_dep_centre1) ~ 3L,
      lo_situ_pays_cor < 555 & lo_situ_pays_cor > 1 & !is.na(lo_dep_centre1) ~ 4L
    )
  )

# Je laisse tomber les 

  mutate(
    lo_situ_geo = case_when(
      lo_situ_dep == lo_dep_centre1 ~ 1L,
      lo_situ_dep == id_dep_nais  ~ 2L,
      is.na(lo_type) ~ NA_integer_,
      TRUE ~ 555L
    )
  ) %>%
  relocate(., any_of(c("lo_dep_centre1_cor", "lo_situ_geo")), .after = lo_dep_centre1)

P5_F4 %>% 
  select(lo_type, lo_situ_pays_cor, id_lieu_nais, lo_situ_dep, id_dep_nais, id_centre1, lo_dep_centre1, lo_dep_situ_centre1) %>% 
  View()


P5_F4 %>% 
  select(lo_type, lo_situ_geo, lo_dep_centre1, lo_situ_dep, id_dep_nais, lo_situ_pays, id_lieu_nais) %>%
  View()


P5_F3 %>% 
  dfSummary() %>% 
  view()

P5_F4 %>% 
  dfSummary(
    valid.col = F,
    max.distinct.values = 5,
    max.string.width = 20,
    labels.col = T
  ) %>% 
  view()

data %>% 
  dfSummary(
    valid.col = F,
    max.distinct.values = 5,
    max.string.width = 20,
    labels.col = T,
    split.cells = 20,
    style = "multiline"
  ) %>% 
  view()

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
