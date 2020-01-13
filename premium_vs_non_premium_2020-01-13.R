# RFMTS for segments


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


# med Premium
sqlquery1 <- "SELECT [DateOrdered_Key], t1.[Customer_Key], [5_DB2],IsActive
              FROM [EDW].[fact].[OrderFact] [t1]
              inner JOIN [DataMartMisc].[temp].[PremiumSubscribers_Active] [t2] on t2.[Customer_Key] = t1.[Customer_Key]
                where t1.Customer_Key != -1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 1, 112)))
                "

df <- sqlQuery(channel, sqlquery1)

# uden premium

sqlquery2 <- "SELECT [DateOrdered_Key], t1.[Customer_Key], [5_DB2],IsActive
              FROM [EDW].[fact].[OrderFact] [t1]
              left JOIN [DataMartMisc].[temp].[PremiumSubscribers_Active] [t2] on t2.[Customer_Key] = t1.[Customer_Key]
                where t1.Customer_Key != -1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 1, 112)))
                and t2.CUstomer_Key is NULL
                "

df2 <- sqlQuery(channel, sqlquery2)


# segment
sqlquery3 <- "SELECT DISTINCT Customer_Key,Segment
              FROM [DataMartMisc].[r].[CLTVinput]"

df3 <- sqlQuery(channel, sqlquery3)


close(channel)

df <- df %>%
  as_tibble() %>%
  mutate(Date = ymd(DateOrdered_Key)) %>%
  select(Customer_Key, Date, DB2 = `5_DB2`)


monetary <- df %>% 
  group_by(Customer_Key) %>%
  summarise(Monetary = sum(DB2))


# ------------------------------------
# med Premium

# RECENCY

last_date <- max(as_date(df$Date))

recency <- df %>%
  group_by(Customer_Key) %>%
  summarise(RecencyDays = as.numeric(last_date - max(as_date(Date))))

# FREQUENCY

frequency <- df %>%
  group_by(Customer_Key) %>%
  summarise(Frequency = n())


rfmtsPrem <- list(recency,frequency,monetary) %>% 
  reduce(inner_join) %>%
  left_join(df3) %>%
  filter(Segment == "BTC")


df2 <- df2 %>%
  as_tibble() %>%
  mutate(Date = ymd(DateOrdered_Key)) %>%
  select(Customer_Key, Date, DB2 = `5_DB2`)


monetary <- df2 %>% 
  group_by(Customer_Key) %>%
  summarise(Monetary = sum(DB2))


# RECENCY

last_date <- max(as_date(df2$Date))

recency <- df2 %>%
  group_by(Customer_Key) %>%
  summarise(RecencyDays = as.numeric(last_date - max(as_date(Date))))

# FREQUENCY

frequency <- df2 %>%
  group_by(Customer_Key) %>%
  summarise(Frequency = n())


rfmtsNoPrem <- list(recency,frequency,monetary) %>% 
  reduce(inner_join) %>%
  left_join(df3) %>%
  filter(Segment == "BTC")

# ---------------------------------------
# prep data

rfmts_recipe <- rfmtsPrem %>%
  select(RecencyDays,Frequency,Monetary) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfmts_norm_prem <- bake(rfmts_recipe, new_data = rfmtsPrem) %>%
  mutate(Customer_Key = rfmtsPrem$Customer_Key) %>%
  select(-Customer_Key)

rfmts_recipe2 <- rfmtsNoPrem %>%
  select(RecencyDays,Frequency,Monetary) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfmts_norm_no_prem <- bake(rfmts_recipe2, new_data = rfmtsNoPrem) %>%
  mutate(Customer_Key = rfmtsNoPrem$Customer_Key) %>%
  select(-Customer_Key)


# -------------------------------------------
# k means Premium

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfmts_norm_prem)
x = names(km_training)

km <- h2o.kmeans(training_frame = km_training, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

p1 <- km@model$centers %>%
  as_tibble() %>%
  mutate(Customer_Segment_km = centroid) %>%
  select(-centroid) %>%
  gather(metric, value, -Customer_Segment_km) %>%
  group_by(Customer_Segment_km,metric) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "recencydays","frequency","monetary")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  ylim(-2,2) +
  theme_light() +
  ggtitle("Premium") +
  scale_colour_tableau() +
  theme(legend.title = element_blank())



# k means non Premium

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfmts_norm_no_prem)
x = names(km_training)

km <- h2o.kmeans(training_frame = km_training, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

p2 <- km@model$centers %>%
  as_tibble() %>%
  mutate(Customer_Segment_km = centroid) %>%
  select(-centroid) %>%
  gather(metric, value, -Customer_Segment_km) %>%
  group_by(Customer_Segment_km,metric) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "recencydays","frequency","monetary")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  ylim(-2,2) +
  theme_light() +
  ggtitle("non-Premium") +
  scale_colour_tableau() +
  theme(legend.title = element_blank())

gridExtra::grid.arrange(p1,p2, ncol =2)
