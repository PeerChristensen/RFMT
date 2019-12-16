# Old RFM

library(tidyverse)
library(RODBC)
library(ggthemes)

credentials <- read_rds("credentials.rds")

channel <-odbcConnect(credentials[1], uid=credentials[2], pwd=credentials[3])

sqlquery <- "SELECT 
      [Customer_Key]
      ,[RFM_cluster]
  FROM [EDW].[edw].[ChurnData]"

df <- sqlQuery(channel, sqlquery)

close(channel)

df %>%
  group_by(RFM_cluster) %>%
  count()

df %>%
  group_by(RFM_cluster) %>%
  count() %>%
  ggplot(aes(reorder(RFM_cluster,n),n, fill = n)) +
  geom_col() +
  theme_minimal() +
  coord_flip() +
  labs(x="Cluster") +
  scale_fill_gradient_tableau(guide=F)
