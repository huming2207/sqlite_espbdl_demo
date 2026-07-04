idf_component_register(SRCS "hello_world_main.c"
                       PRIV_REQUIRES esp_partition esp_timer fatfs wear_levelling sqlite_espbdl
                       INCLUDE_DIRS "")
