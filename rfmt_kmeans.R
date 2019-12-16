
library(RODBC)
library(lubridate)
library(tidyverse)
library(ggiraphExtra)
library(factoextra)
library(h2o)
library(ggthemes)
library(recipes)

<<<<<<< HEAD
credentials <- read_rds("credentials.rds")
=======
channel <-odbcConnect("", uid="R", pwd="")
>>>>>>> 3ac67030154dac1bcf55026f744daea7f17078d8

channel <-odbcConnect(credentials[1], uid=credentials[2], pwd=credentials[3])

sqlquery <- "SELECT [DateOrdered_Key], t1.[Customer_Key], [5_DB2]
              FROM [EDW].[fact].[OrderFact] [t1]
              INNER JOIN [DataMartMisc].[temp].[PremiumSubscribers_Active] [t2] on t2.[Customer_Key] = t1.[Customer_Key]
                where t1.Customer_Key != -1
                and IsActive = 1
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
cols = c("#c9b037", "#b4b4b4", "#6a3805")


# plot faceted distribution with long format

rfm %>% 
  filter(Monetary>=0) %>%
  mutate(RecencyDays = log(RecencyDays),Frequency = log(Frequency), Monetary = log(Monetary+1),Tenure = Tenure) %>%
  pivot_longer(c(RecencyDays, Frequency, Monetary,Tenure)) %>%
  #gather(metric, value, -CustomerID, - Customer_Segment) %>%
  ggplot(aes(x=value, fill = Customer_Segment)) +
  geom_density(alpha=.7) +
  facet_wrap(~name,scales = "free") +
  scale_fill_manual(values = cols,"") +
  theme_minimal()

# --------------------------------------------------------------------------------- 

# preprocess data: log, center, scale

# old way 
# rfm_norm <- rfm %>%
#   select(RecencyDays,Frequency,Monetary,Tenure) %>% 
#   mutate(Monetary = ifelse(Monetary<0,NA,Monetary)) %>%
#   apply(2,function(x) log(x+1)) %>%
#   apply(2, function(x) round(x-mean(x,na.rm=T),1)) %>%
#   scale() %>%
#   as_tibble %>%
#   mutate(Customer_Key = rfm$Customer_Key,
#          Customer_Segment = rfm$Customer_Segment) %>%
#   select(Customer_Segment,everything()) %>%
#   drop_na()

rfm_recipe <- rfm %>%
  select(RecencyDays,Frequency,Monetary,Tenure) %>%
  recipe() %>%
  step_YeoJohnson(all_numeric()) %>%
  step_center(all_numeric()) %>%
  step_scale(all_numeric()) %>%
  prep(data = train_data)

rfm_norm <- bake(rfm_recipe, new_data = rfm) %>%
  mutate(Customer_Key = rfm$Customer_Key,
         Customer_Segment = rfm$Customer_Segment) %>%
  select(Customer_Segment,everything())

#write_csv(rfm_norm, "data_2019-12-12.csv")

rfm_norm <- read_csv("data_2019-12-12.csv")

# -------------------------------------------------------------------------------------

# kmeans with H2O
h2o.init(nthreads = -1)

km_training <- as.h2o(rfm_norm[2:5])
x = names(km_training)

km <- h2o.kmeans(training_frame = km_training, 
                 k = 10,
                 x = x,
                 standardize = F,
                 estimate_k = T)

saveRDS(km, "km_model_2019-12-12.rds")

km <- readRDS("km_model_2019-12-12.rds")

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

rfm_clusters <- tibble(Cluster = factor(cluster$predict+1)) %>% bind_cols(rfm_norm) %>%
  select(-Customer_Segment,-Customer_Key)
 # filter(Monetary > -5 & Monetary < 5) %>%

# n per group 
rfm_clusters %>% 
  group_by(Cluster) %>%
  count() %>% htmlTable::htmlTable()

# plotly 3d scatter
library(plotly)

rfm_clusters_plot <- rfm_clusters %>%
  filter(Monetary >= -5 & Monetary <= 5) %>%
  sample_frac(.2)
  
colors <- ggthemes_data[["tableau"]][["color-palettes"]][["regular"]]$`Tableau 10`[1:3,] %>% 
  pull(value)

p <- plot_ly(rfm_clusters_plot, x = ~RecencyDays, y = ~Frequency, z = ~Monetary, 
        color = ~Cluster, size = ~Tenure, colors = colors, opacity = .7) %>%
  add_markers() %>%
  layout(scene = list(xaxis = list(title = 'Recency'),
                      yaxis = list(title = 'Frequency'),
                      zaxis = list(title = 'Monetary')))

p

#not used

# text = ~paste('Customer Key:', Customer_Key,
#               "<br>Recency:", RecencyDays1,
#               "<br>Frequency:", Frequency1,
#               "<br>Monetary:", Monetary1,
#               "<br>Tenure:", Tenure1)

# with factoextra
# object = list(data = rfm_clusters[3:6], cluster = rfm_clusters$Cluster)
# 
# fviz_cluster(object, data = rfm_norm[,2:5], geom=c("point")) +
#     theme_light()

# -------------------------------------------------------------------------------------------

# with factoextra

# fviz_nbclust(rfm_norm[,2:5], kmeans) too much data
# 
# rfm_clust <- kmeans(rfm_norm[,2:5], centers=4, nstart = 25)
# 
# #table(rfm$Customer_Segment,rfm_clust$cluster)
# 
# fviz_cluster(rfm_clust, data = rfm_norm[,2:5], geom=c("point")) +
#   theme_light()
# 
### object = list(data = mydata, cluster = myclust))

# # snake plot with cluster means
# 
# rfm_clust$centers %>% 
#   as_tibble() %>%
#   mutate(Customer_Segment_km = factor(1:4)) %>% # last val = n clusters
#   gather(metric, value, -Customer_Segment_km) %>%
#   group_by(Customer_Segment_km,metric) %>%
#   ungroup() %>%
#   mutate(metric = fct_relevel(metric, "RecencyDays","Frequency","Monetary")) %>%
#   ggplot(aes(x=factor(metric),y=value,group=Customer_Segment_km,colour = Customer_Segment_km)) +
#   geom_line(size=1.5) +
#   geom_point(size=2) +
#   theme_light() +
#   scale_colour_tableau()
