
library(RODBC)
library(lubridate)
library(tidyverse)
library(ggiraphExtra)
library(recipes)


channel <-odbcConnect("saxo034", uid="R", pwd="sqlR2017")

sqlquery <- "SELECT [DateOrdered_Key],[Customer_Key],[5_DB2]
              FROM [EDW].[fact].[OrderFact]
                where Customer_Key != -1
                and DateCancelled_Key = -1
                and [DateOrdered_Key] >= (SELECT CONVERT(INT, CONVERT(VARCHAR(8), GETDATE()-365 * 3, 112)))"

df <- sqlQuery(channel, sqlquery)
close(channel)

cols = c("#c9b037", "#b4b4b4", "#6a3805")

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
  count() # %>% htmlTable()

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

# preprocess data for plots: log, center, scale

# rfm_norm <- rfm %>%
#   select(RecencyDays,Frequency,Monetary,Tenure) %>% 
#   mutate(Monetary = ifelse(Monetary<0,NA,Monetary)) %>%
#   apply(2,function(x) log(x+1)) %>%
#   apply(2, function(x) round(x-mean(x,na.rm=T),1)) %>%
#   scale() %>%
#   as_tibble %>%
#   mutate(Customer_Key = rfm$Customer_Key,
#          Customer_Segment = rfm$Customer_Segment)

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

# --------------------------------------------------------------------------------- 
# snake plot

rfm_norm %>%
  group_by(Customer_Segment,Customer_Key) %>%
  gather(metric, value, -Customer_Key, - Customer_Segment) %>%
  group_by(Customer_Segment,metric) %>%
  summarise(value = mean(value, na.rm = T)) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "RecencyDays","Frequency","Monetary","Tenure")) %>%
  ggplot(aes(x=factor(metric),y=value,group=Customer_Segment,colour = Customer_Segment)) +
  geom_line(size=1.5) +
  geom_point(size=2) +
  scale_colour_manual(values = cols, "") +
  ylim(-2, 2) +
  theme_light()

# --------------------------------------------------------------------------------- 
# radar plot

rfm_norm %>%
  group_by(Customer_Segment,Customer_Key) %>%
  gather(metric, value, -Customer_Key, - Customer_Segment) %>%
  group_by(Customer_Segment,metric) %>%
  summarise(value = mean(value, na.rm = T)) %>%
  ungroup() %>%
  mutate(metric = fct_relevel(metric, "RecencyDays","Frequency","Monetary","Tenure")) %>%
  pivot_wider(names_from = metric) %>%
  ggRadar(aes(group = Customer_Segment), 
          rescale = FALSE, legend.position = "none",
          size = 2.5, interactive = FALSE, use.label = TRUE,alpha=0) +
  scale_y_discrete(breaks = NULL) +
 # facet_wrap(~Customer_Segment) +
  theme(axis.text.x = element_text(size = 10)) +
  geom_line(size=1.5) +
  theme_minimal() +
  scale_colour_manual(values = cols) +
  theme(legend.position = "top") 

# ------------------------------------------------------------------------------------

group_means <- rfm %>%
  select(Customer_Segment, RecencyDays,Frequency,Monetary,Tenure) %>%
  group_by(Customer_Segment) %>%
  summarise(gr_recency = mean(RecencyDays),
            gr_frequency = mean(Frequency),
            gr_monetary = mean(Monetary,na.rm=T),
            gr_tenure = mean(Tenure)) 

pop_means <- rfm %>%
  select(Customer_Segment, RecencyDays,Frequency,Monetary,Tenure) %>%
  summarise(pop_recency = mean(RecencyDays),
            pop_frequency = mean(Frequency),
            pop_monetary = mean(Monetary,na.rm=T),
            pop_tenure = mean(Tenure))

relative_imp <- group_means %>%
  mutate(Recency = gr_recency / pop_means$ pop_recency -1,
         Frequency = gr_frequency / pop_means$pop_frequency - 1,
         Monetary = gr_monetary / pop_means$pop_monetary - 1,
         Tenure = gr_tenure / pop_means$pop_tenure -1) %>%
  mutate(Customer_Segment = levels(rfm$Customer_Segment)) %>%
  select(-starts_with("gr"))

# relative_imp <- group_means %>% 
#   apply(2,function(x) x / pop_means - 1) %>%
#   as_tibble() %>%
#   mutate(Customer_Segment = levels(rfm$Customer_Segment)) %>%
#   select(Customer_Segment, everything())

# relative variable importance heatmap

relative_imp %>%
  gather(metric, value, - Customer_Segment) %>%
  mutate(Customer_Segment = fct_relevel(Customer_Segment, "Bronze","Silver","Gold"),
         metric = fct_relevel(metric, "Recency", "Frequency", "Monetary","Tenure")) %>%
  ggplot(aes(x = metric, y = Customer_Segment)) +
  geom_raster(aes(fill= value)) +
  geom_text(aes(label = glue::glue("{round(value,2)}")), size = 10, color = "snow") +
  theme_light() +
  theme(axis.text = element_text(size = 16)) +
  scale_fill_gradient()
