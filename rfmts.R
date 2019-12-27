# RFMTS with k-means - BTC segment
# december 2019
# Peer Christensen

# ---------------------------------------------------------
# PAckages

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

# RFMT one year
sqlquery1 <- "SELECT [DateOrdered_Key], t1.[Customer_Key], [5_DB2]
              FROM [EDW].[fact].[OrderFact] [t1]
              INNER JOIN [DataMartMisc].[temp].[PremiumSubscribers_Active] [t2] on t2.[Customer_Key] = t1.[Customer_Key]
                where t1.Customer_Key != -1
                and IsActive = 1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 1, 112)))"

df <- sqlQuery(channel, sqlquery1)

# Streaming
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

# customer segments

sqlquery3 <- "SELECT DISTINCT Customer_Key,Segment
              FROM [DataMartMisc].[r].[CLTVinput]"

df3 <- sqlQuery(channel, sqlquery3)

close(channel)

# get BTC customer keys
BTC_keys <- df3 %>%
  filter(Segment == "BTC") %>%
  pull(Customer_Key)


# RFMT

df <- df %>%
  as_tibble() %>%
  filter(Customer_Key %in% BTC_keys) %>%
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
  as_tibble() %>%
  filter(Customer_Key %in% BTC_keys) %>%
  rename(Streaming =n)

# --------------------------------------------------------------------------

# JOIN

rfmts <- list(recency,frequency,monetary,tenure,streaming) %>% 
  reduce(inner_join) 
# 
# # add rfmts segment and score 
# 
# rfmts <- rfmts %>%
#   mutate(RFM_Segment = paste0(R_Quantile,F_Quantile,M_Quantile,T_Quantile,S_Quantile),
#          Score   = R_Quantile+F_Quantile+M_Quantile+T_Quantile+S_Quantile)
# 
# 
# rfmts <- rfmts %>%
#   mutate(Customer_Segment = case_when(Score > 14 ~ "Gold",
#                                       Score > 9 ~ "Silver",
#                                       Score > 0 ~ "Bronze")) %>%
#   mutate(Customer_Segment = fct_relevel(Customer_Segment,"Gold","Silver","Bronze")) 
# 
# rfmts %>%
#   group_by(Customer_Segment) %>%
#   summarise(R_mean = mean(RecencyDays),
#             F_mean = mean(Frequency),
#             M_mean = mean(Monetary),
#             T_mean = mean(Tenure),
#             S_mean = mean(Streaming))

# n per segment
# rfmts %>%
#   group_by((Customer_Segment)) %>%
#   count()

# ----------------------------------------------------------------------
  # plots
  # cols = c("#c9b037", "#b4b4b4", "#6a3805")


# plot faceted distribution with long format

# rfmts %>% 
#   filter(Monetary>=0) %>%
#   mutate(RecencyDays = log(RecencyDays),Frequency = log(Frequency), Monetary = log(Monetary+1),Streaming=log(Streaming),Tenure = Tenure) %>%
#   pivot_longer(c(RecencyDays, Frequency, Monetary,Tenure,Streaming)) %>%
#   #gather(metric, value, -CustomerID, - Customer_Segment) %>%
#   ggplot(aes(x=value, fill = Customer_Segment)) +
#   geom_density(alpha=.7) +
#   facet_wrap(~name,scales = "free") +
#   scale_fill_manual(values = cols,"") +
#   theme_minimal()

rfmts_recipe <- rfmts %>%
  select(RecencyDays,Frequency,Monetary,Tenure,Streaming) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfmts_norm <- bake(rfmts_recipe, new_data = rfmts) %>%
  mutate(Customer_Key = rfmts$Customer_Key)

Customer_Key <- rfmts_norm$Customer_Key

rfmts_norm <- rfmts_norm %>% select(-Customer_Key)
# -------------------------------------------------------------------------------------

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfmts_norm)
x = names(km_training)

# scree plot
max_groups <- 16
wss_h2o <- numeric(max_groups)

for(i in 1:max_groups){
  
  km1 <- h2o.kmeans(km_training, x= x, estimate_k = FALSE, k = i, standardize = FALSE, 
                        nfolds = 5, max_iterations = 25)   
  wss_h2o[i] <- h2o.tot_withinss(km1, xval = TRUE) # xval=TRUE means cross validation used
}

par(font.main = 1)
plot(1:max_groups, wss_h2o, type="b", xlab="Number of Clusters",
     ylab="Within groups sum of squares", bty = "l")
grid()

# train

km <- h2o.kmeans(training_frame = km_training, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

h2o.saveModel(km,paste0("rfmts_kmeans_model_",today()))



# plots
p1 <- km@model$centers %>%
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
  theme(legend.title = element_blank(),
        legend.position = "top")

cluster <- h2o.predict(km,km_training) %>% as_tibble() 

rfmts_clusters <- tibble(Cluster = factor(cluster$predict+1)) %>% bind_cols(rfmts_norm)

# n per group 
rfmts_clusters %>% 
  group_by(Cluster) %>%
  count() %>% htmlTable::htmlTable()


# box plots
p2 <- rfmts_clusters %>%
  pivot_longer(-Cluster) %>%
  ggplot(aes(Cluster,value,colour=Cluster)) +
  geom_jitter(alpha= .05, width = .2) +
  geom_boxplot(alpha = .1, outlier.shape = NA) +
  ylim(-5,5) +
  facet_wrap(~name) +
  scale_colour_tableau(guide=F) +
  theme_minimal()

gridExtra::grid.arrange(p1,p2, ncol =2)

