variable_names <- names(data)

codebook <- tibble(
    variable = variable_names,
    classe = map_chr(data, ~ class(.x)[1]),
    n_unique = map_int(data, ~ n_distinct(.x, na.rm = TRUE)),
    n_complete = map_int(data, ~ sum(!is.na(.x))),
    n_NA = map_int(data, ~ sum(is.na(.x))),
    data = map(data, ~ .x)
  ) %>% 
  
  mutate(
    afficher_stats = case_when(
      str_detect(variable, "_age") & classe == "integer" ~ TRUE,
      str_ends(variable, "_an") ~ TRUE,
      str_ends(variable, "_nais") ~ TRUE,
      str_ends(variable, "_deces") ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>% 
  
  mutate(
    modalites = case_when(
      str_detect(variable, "_date") ~ "AAAA-MM-JJ",
      str_detect(variable, any_of(c("commentaires", "_autre", "_precis", "_prof"))) ~ "Libre saisie",
      afficher_stats == TRUE ~ pmap_chr(list(variable, data, afficher_stats), function(nom, colonne, afficher_stats) {
        val <- unique(colonne[!is.na(colonne)])
        if (length(val) <= 27) {
          return(paste(val, collapse = ";"))
        } else if (afficher_min_max) {
          # Convertir les dates si besoin
          if (str_detect(nom, "_date_")) {
            suppressWarnings({
              val <- as.Date(val)
            })
          }
          if (all(is.na(val))) return("min: NA / max: NA")
          return(paste0(min(val, na.rm = TRUE), ":", max(val, na.rm = TRUE)))
        } else {
          return("Trop de valeurs")
        }
      })
  )
  )
