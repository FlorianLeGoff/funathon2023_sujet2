# utilisation PostgreSQL

#renv::restore()
file.edit("~/.Renviron")

library(RPostgres)
library(dplyr)
library(knitr)
library(sf)
library(janitor)
library(aws.s3)


conn <- dbConnect(Postgres(),
                  user = Sys.getenv("USER_POSTGRESQL"),
                  password = Sys.getenv("PASS_POSTGRESQL"),
                  host = Sys.getenv("HOST_POSTGRESQL"),
                  dbname = "defaultdb",
                  port = 5432,
                  check_interrupts = TRUE)

df <- data.frame(
  a = c("a", "b", "c"),
  b = c(1, 2, 3)
)

res <- dbSendQuery(conn, "CREATE SCHEMA IF NOT EXISTS test_schema")
dbWriteTable(conn, Id(schema = "test_schema", table = "test_table"), df, overwrite = TRUE)
res <- dbGetQuery(conn, "SELECT * FROM test_schema.test_table")

res %>% kable()


parcelles <- s3read_using(
  FUN = sf::read_sf,
  query = 'SELECT * FROM parcelles_graphiques LIMIT 10',
  object = "2023/sujet2/diffusion/ign/rpg/PARCELLES_GRAPHIQUES.gpkg",
  bucket = "projet-funathon",
  opts = list("region" = "")
)

write_sf(parcelles, conn, Id(schema = "test_schema", table = "test_parcelles"), delete_layer = TRUE)

sf <- st_read(
  conn, query = "SELECT * FROM test_schema.test_parcelles"
)

plot(st_geometry(sf))

dbDisconnect(conn)
