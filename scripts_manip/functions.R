## Retirer nouvelles colonnes vides ####

empty_cols <- M2_rdb_W1 %>%
  mutate(across(
    .cols = where(is.character),
    .fns = ~ na_if(., "")
  )) %>% 
  summarise(across(everything(), ~ sum(is.na(.)))) %>% 
  pivot_longer(
    everything(),
    names_to = "variable",
    values_to = "nb_NA") %>% 
  filter(nb_NA == nrow(M2_rdb_W1))

cols_empty <- M2_rdb_W1 %>% 
  mutate(across(where(is.character), ~ na_if(., "")))

vec <- seq(-5,5,1)

vec_carre <- c()

for (i in c(1:length(vec))) {
  vec_carre[i] = vec[i]^2
}
vec_carre

vars_rdb01 <- M2_rdb_W1 %>% 
  select(all_of(starts_with("sc_rdb01"))) %>% 
  colnames()

table(with(M2_rdb_W1, get(vars_rdb01[1])))

for (i in vars_rdb01) {
  tab <- freq(
    with(M2_rdb_W1,get(i)),
    digits = 1,
    cum = TRUE,
    total = TRUE
  )
  print(tab)
}

vars_rdb02 <- select(M2_rdb_W1, starts_with("sc_rdb02"))
lapply(vars_rdb02, freq)

empty_cols <- function(df) {
  
  df_empty_cols <- df %>% 
    mutate(across(
      .cols = where(is.character),
      .fns = ~ na_if(., "")
    )) %>% 
    summarise(across(everything(), ~ sum(is.na(.)))) %>% 
    pivot_longer(
      everything(),
      names_to = "variable",
      values_to = "nb_NA") %>% 
    filter(nb_NA == nrow(df))
  
  return(df_empty_cols)
}

empty_cols(M2_rdb_W1)



## Functions remove empty cols ####


  vars_empty <- function(df) {
    
    df %>% 
      mutate(across(
        .cols = where(is.character),
        .fns = ~ na_if(., "")
      )) %>% 
      summarise(across(everything(), ~ sum(is.na(.)))) %>% 
      pivot_longer(
        everything(),
        names_to = "variable",
        values_to = "nb_NA") %>% 
      filter(nb_NA == nrow(df)) %>% 
      pull(variable)
  }

vars_TPO <- vars_empty(M1_V1)

##












