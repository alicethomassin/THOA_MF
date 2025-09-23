
M2_F1 %>% 
  filter(id_anonymat == "IMTKF"|
           id_anonymat == "LJHGZ") %>%
  select(id_anonymat, id_sep, id_date_creation, sc_date_creation) %>% 
  View()

M2_F2 %>% 
  filter(id_link == "IMTKF") %>%
  select(id_anonymat, id_sep, id_link, id_date_creation, sc_date_creation) %>% 
  View()



P2_rdb_F1 %>% 
  filter(id_link == "IMTKF") %>%
  select(id_anonymat, id_link, id_date_creation, sc_rdb_date_creation) %>% 
  View()

pre_recodageW <- P2_rdb_W1 %>% 
  filter(id_anonymat == "GLHUX"|
           id_anonymat == "XMKEO"|
           id_anonymat == "ERYBB") %>% 
  select(id_anonymat, ends_with("_an"))

recodageW <- P2_rdb_W2 %>% 
  filter(id_anonymat == "GLHUX"|
           id_anonymat == "XMKEO"|
           id_anonymat == "ERYBB") %>% 
  select(id_anonymat, ends_with("_an"))

pre_recodage <- P2_rdb_F3 %>% 
  filter(id_anonymat == "GLHUX"|
           id_anonymat == "XMKEO"|
           id_anonymat == "ERYBB") %>% 
  select(id_anonymat, rang_sc_rdb, an, classe)


recodageL <- P2_rdb_F4 %>% 
  filter(id_anonymat == "GLHUX"|
           id_anonymat == "XMKEO"|
           id_anonymat == "ERYBB") %>% 
  select(id_anonymat, rang_sc_rdb, an, classe)
