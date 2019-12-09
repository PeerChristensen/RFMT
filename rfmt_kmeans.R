
library(RODBC)
library(lubridate)
library(tidyverse)
library(ggiraphExtra)
library(factoextra)
library(h2o)
library(ggthemes)

channel <-odbcConnect("", uid="R", pwd="")

sqlquery <- "SELECT [DateOrdered_Key],[Customer_Key],[5_DB2]
              FROM [EDW].[fact].[OrderFact]
                where Customer_Key != -1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 3, 112)))"

df <- sqlQuery(channel, sqlquery)
close(channel)


df <- df %>%
  as_tibble() %>%
  mutate(Date = ymd(DateOrdered_Key)) %>%
  select(Customer_Key, Date, DB2 = `5_DB2`)


monetary <- df %>% 
  group_by(Customer_Key) %>%
  summarise(Monetary = sum(DB2)) %>%
  mutate(M_Quantile = ntile(Monetary, 4)) # quartile

# check mean and min for each quartile
monetary %>% group_by(M_Quantile) %>% summarise(mean = mean(Monetary))

monetary %>% group_by(M_Quantile) %>% summarise(min = min(Monetary))

# RECENCY

last_date <- max(as_date(df$Date))

recency <- df %>%
  group_by(Customer_Key) %>%
  summarise(RecencyDays = as.numeric(last_date - max(as_date(Date)))) %>%
  mutate(R_Quantile = ntile(desc(RecencyDays), 4)) # quartile

# FREQUENCY

frequency <- df %>%
  group_by(Customer_Key) %>%
  summarise(Frequency = n()) %>%
  mutate(F_Quantile = ntile(Frequency, 4)) # quartile

# Tenure: days since first transaction 

tenure <- df %>%
  group_by(Customer_Key) %>%
  summarise(Tenure = as.numeric(last_date - min(as_date(Date)))) %>%
  mutate(T_Quantile = ntile(Tenure, 4)) # quartile


# --------------------------------------------------------------------------

# JOIN

rfm <- list(recency,frequency,monetary,tenure) %>% 
  reduce(left_join)

# add rfm segment and score 

rfm <- rfm %>%
  mutate(RFM_Segment = paste0(R_Quantile,F_Quantile,M_Quantile,T_Quantile),
         Score   = R_Quantile+F_Quantile+M_Quantile+T_Quantile)

# --------------------------------------------------------------------------

# size of each segment

rfm %>% 
  group_by(RFM_Segment) %>% 
  summarise(n = n()) %>% 
  arrange(desc(n)) %>%
  ggplot(aes(reorder(RFM_Segment,n),n)) +
  geom_col()

# assign customer segment based on rfm score

rfm <- rfm %>%
  mutate(Customer_Segment = case_when(Score > 12 ~ "Gold",
                                      Score > 8 ~ "Silver",
                                      Score > 0 ~ "Bronze")) %>%
  mutate(Customer_Segment = fct_relevel(Customer_Segment,"Gold","Silver","Bronze")) 

# ------------------------------------------------------------------------------
# rfm summary of customer segments

rfm %>%
  group_by(Customer_Segment) %>%
  summarise(R_mean = mean(RecencyDays),
            F_mean = mean(Frequency),
            M_mean = mean(Monetary),
            T_mean = mean(Tenure))

# n per segment
rfm %>%
  group_by((Customer_Segment)) %>%
  count()

# --------------------------------------------------------------------------------
# plots


# plot faceted distribution with long format

rfm %>% 
  filter(Monetary>=0) %>%
  mutate(RecencyDays = RecencyDays,logFrequency = log(Frequency), logMonetary = log(Monetary+1),Tenure = Tenure) %>%
  pivot_longer(c(RecencyDays, logFrequency, logMonetary,Tenure)) %>%
  #gather(metric, value, -CustomerID, - Customer_Segment) %>%
  ggplot(aes(x=value, fill = Customer_Segment)) +
  geom_density(alpha=.7) +
  facet_wrap(~name,scales = "free") +
  scale_fill_manual(values = cols,"") +
  theme_minimal()

# --------------------------------------------------------------------------------- 

# preprocess data: log, center, scale

rfm_norm <- rfm %>%
  select(RecencyDays,Frequency,Monetary,Tenure) %>% 
  mutate(Monetary = ifelse(Monetary<0,NA,Monetary)) %>%
  apply(2,function(x) log(x+1)) %>%
  apply(2, function(x) round(x-mean(x,na.rm=T),1)) %>%
  scale() %>%
  as_tibble %>%
  mutate(Customer_Key = rfm$Customer_Key,
         Customer_Segment = rfm$Customer_Segment) %>%
  select(Customer_Segment,everything()) %>%
  drop_na()

# -------------------------------------------------------------------------------------

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfm_norm[2:5])
x = names(km_training)

km <- h2o.kmeans(training_frame = km_training, k = 10,
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
  theme_light() +
  scale_colour_tableau() +
  theme(legend.title = element_blank())

# with factoextra

#fviz_nbclust(rfm_norm[,2:5], kmeans) too much data

rfm_clust <- kmeans(rfm_norm[,2:5], centers=4, nstart = 25)

#table(rfm$Customer_Segment,rfm_clust$cluster)

fviz_cluster(rfm_clust, data = rfm_norm[,2:5], geom=c("point")) +
  theme_light()

# snake plot with cluster means

rfm_clust$centers %>% 
  as_tibble() %>%
  mutate(Customer_Segment_km = factor(1:4)) %>% # last val = n clusters
  gather(metric, value, -Customer_Segment_km) %>%
  group_by(Customer_Segment_km,metric) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "RecencyDays","Frequency","Monetary")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  theme_light() +
  scale_colour_tableau()
