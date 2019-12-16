
library(RODBC)
library(lubridate)
library(tidyverse)
library(ggiraphExtra)
library(factoextra)
library(h2o)
library(ggthemes)
library(recipes)


credentials <- read_rds("credentials.rds")

channel <-odbcConnect(credentials[1], uid=credentials[2], pwd=credentials[3])


sqlquery1 <- "SELECT [DateOrdered_Key], t1.[Customer_Key], [5_DB2]
              FROM [EDW].[fact].[OrderFact] [t1]
              INNER JOIN [DataMartMisc].[temp].[PremiumSubscribers_Active] [t2] on t2.[Customer_Key] = t1.[Customer_Key]
                where t1.Customer_Key != -1
                and IsActive = 1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 1, 112)))"

df <- sqlQuery(channel, sqlquery1)

sqlquery2 <- "SELECT DISTINCT [Premium_Subscribed].[Customer_Key], ISNULL(t.[n],0) [n]
                FROM [DataMartMisc].[temp].[Premium_Subscribed] 
                LEFT JOIN 
                (
                SELECT [t1].[Customer_Key]
                      ,count([t1].[Customer_Key]) as [n]
                  FROM [EDW].[fact].[ReaderFact] [t1]
                  Where [Streaming_Key] = 1
                  and [IsBucketRead_Key] = 1
                  and [Date_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 1, 112)))
                  and [Premium_Key] = 1
                 group by [t1].[Customer_Key]
                ) t ON [Premium_Subscribed].[Customer_Key] = t.[Customer_Key]
                ORDER BY [Premium_Subscribed].[Customer_Key] " 

df2 <- sqlQuery(channel, sqlquery2)

sqlquery3 <- "SELECT DISTINCT Customer_Key,Segment
              FROM [DataMartMisc].[r].[CLTVinput]
              where Segment = 'BTC'"

df3 <- sqlQuery(channel, sqlquery3)


close(channel)


# RFMT

df <- df %>%
  as_tibble() %>%
  mutate(Date = ymd(DateOrdered_Key)) %>%
  select(Customer_Key, Date, DB2 = `5_DB2`)


monetary <- df %>% 
  group_by(Customer_Key) %>%
  summarise(Monetary = sum(DB2))


# RECENCY

last_date <- max(as_date(df$Date))

recency <- df %>%
  group_by(Customer_Key) %>%
  summarise(RecencyDays = as.numeric(last_date - max(as_date(Date))))

# FREQUENCY

frequency <- df %>%
  group_by(Customer_Key) %>%
  summarise(Frequency = n()) 

# Tenure: days since first transaction 

tenure <- df %>%
  group_by(Customer_Key) %>%
  summarise(Tenure = as.numeric(last_date - min(as_date(Date))))

# streaming
streaming <- df2 %>%
  rename(Streaming =n)

# --------------------------------------------------------------------------

# JOIN

rfmts <- list(recency,frequency,monetary,tenure,streaming) %>% 
  reduce(inner_join) %>%
  left_join(df3) %>%
  filter(Segment == "BTC") %>%
  select(-Segment)


rfmts_recipe <- rfmts %>%
  select(RecencyDays,Frequency,Monetary,Tenure,Streaming) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfmts_norm <- bake(rfmts_recipe, new_data = rfmts) %>%
  mutate(Customer_Key = rfmts$Customer_Key) %>%
  select(Customer_Key,everything())

# -------------------------------------------------------------------------------------

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfmts_norm[2:6])
x = names(km_training)

km <- h2o.kmeans(training_frame = km_training, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

km@model$centers %>%
  as_tibble() %>%
  mutate(Customer_Segment_km = centroid) %>%
  select(-centroid) %>%
  gather(metric, value, -Customer_Segment_km) %>%
  group_by(Customer_Segment_km,metric) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "recencydays","frequency","monetary","tenure")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  ylim(-2,2) +
  theme_light() +
  scale_colour_tableau() +
  theme(legend.title = element_blank())


cluster <- h2o.predict(km,km_training) %>% as_tibble() 

rfmts_clusters <- tibble(Cluster = factor(cluster$predict+1)) %>% bind_cols(rfmts_norm)

# n per group 
rfmts_clusters %>% 
  group_by(Cluster) %>%
  count() %>% htmlTable::htmlTable()


# cluster 2 is best and will be used for another cluster analysis

# get customer keys

cluster2_keys <- rfmts_clusters %>%
  filter(Cluster == "2") %>%
  pull(Customer_Key)


rfmts2 <- list(recency,frequency,monetary,tenure,streaming) %>% 
  reduce(inner_join) %>%
  left_join(df3) %>%
  filter(Segment == "BTC",
         Customer_Key %in% cluster2_keys) %>%
  select(-Segment)


rfmts_recipe2 <- rfmts2 %>%
  select(RecencyDays,Frequency,Monetary,Tenure,Streaming) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfmts_norm2 <- bake(rfmts_recipe2, new_data = rfmts2) %>%
  mutate(Customer_Key = rfmts2$Customer_Key) %>%
  select(Customer_Key,everything())



# --------------------------------------------------------

# kmeans with H2O
h2o.init(nthreads = -1)

km_training2 <- as.h2o(rfmts_norm2[2:6])
x = names(km_training2)

km2 <- h2o.kmeans(training_frame = km_training2, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

p1 <- km2@model$centers %>%
  as_tibble() %>%
  mutate(Customer_Segment_km = centroid) %>%
  select(-centroid) %>%
  gather(metric, value, -Customer_Segment_km) %>%
  group_by(Customer_Segment_km,metric) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "recencydays","frequency","monetary","tenure")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  #ylim(-2,2) +
  theme_light() +
  scale_colour_tableau() +
  theme(legend.title = element_blank())

cluster2 <- h2o.predict(km2,km_training2) %>% as_tibble() 

rfmts_clusters2 <- tibble(Cluster = factor(cluster2$predict+1)) %>% bind_cols(rfmts_norm2) %>%
  select(-Customer_Key)

# filter(Monetary > -5 & Monetary < 5) %>%

# n per group 
rfmts_clusters2 %>% 
  group_by(Cluster) %>%
  count() %>% htmlTable::htmlTable()


# box plots
p2 <- rfmts_clusters2 %>%
  pivot_longer(-Cluster) %>%
  ggplot(aes(Cluster,value,colour=Cluster)) +
  geom_jitter(alpha= .05, width = .2) +
  geom_boxplot(alpha = .1, outlier.shape = NA) +
  #ylim(-5,5) +
  facet_wrap(~name) +
  scale_colour_tableau(guide=F) +
  theme_minimal()

gridExtra::grid.arrange(p1,p2, ncol =2)



