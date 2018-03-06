# Databricks notebook source
# MAGIC %md
# MAGIC # Scaling \\(R\\) workload using `spark_apply`

# COMMAND ----------

library("sparklyr")
library("dplyr")
library("ggplot2")

# COMMAND ----------

sc <- sparklyr::spark_connect(method = "databricks")

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Number of Groups", "4", list("32", "64", "128"))

# COMMAND ----------

create_training <- function(group_id)tibble(
  group = toString(group_id), 
  t = 0:23,
  y = group_id*(1 + t %/% 6)
)

# COMMAND ----------

predict <- function(group_data){
  library("forecast")
  library("tseries")
  
  prediction <- ts(group_data[c("y")], frequency = 12) %>%
    forecast::Arima(order=c(0, 1, 0), seasonal = list(order = c(0, 1, 0), period = 12)) %>%
    forecast::forecast(12)
  p_df <- data.frame(y=data.frame(y=prediction)[,1])
  data.frame(t=0:35, rbind(group_data[c("y")], p_df))
}

# COMMAND ----------

# MAGIC %md
# MAGIC # For `1` group

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training Data
# MAGIC $$t \in [0, 24)$$

# COMMAND ----------

create_training(1) %>% 
  ggplot(aes(x = t, y=y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Predictions
# MAGIC $$t \in [0, 36)$$

# COMMAND ----------

create_training(1) %>% 
  predict() %>%
  ggplot(aes(x = t, y=y)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC # LOCAL: For `>1` groups

# COMMAND ----------

group_count <- dbutils.widgets.get("Number of Groups") %>% 
  as.numeric()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training Data
# MAGIC $$t \in [0, 24)$$

# COMMAND ----------

data.frame(ID = 1:group_count) %>%
  mutate(points = purrr::map(ID, create_training)) %>%
  tidyr::unnest(points) %>%
  select(-ID) %>%
  ggplot(aes(x = t, y=y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Predictions
# MAGIC $$t \in [0, 36)$$

# COMMAND ----------

data.frame(ID = 1:group_count) %>%
  mutate(points = purrr::map(ID, create_training)) %>%
  tidyr::unnest(points) %>%
  select(-ID) %>%
  dplyr::group_by(group) %>%
  tidyr::nest() %>%
  dplyr::mutate(group_ys = purrr::map(data, predict)) %>%
  tidyr::unnest(group_ys) %>%
  ggplot(aes(x = t, y=y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC # DISTRIBUTED: For `>1` groups

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training Data
# MAGIC $$t \in [0, 24)$$

# COMMAND ----------

training <- data.frame(ID = 1:group_count) %>%
  mutate(points = purrr::map(ID, create_training)) %>%
  tidyr::unnest(points) %>%
  select(-ID)

training_dist <- copy_to(sc, training, name='training', overwrite=TRUE, memory = FALSE, repartition=16)

# COMMAND ----------

training_dist %>%
  collect() %>%
  ggplot(aes(x = t, y=y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Predictions
# MAGIC $$t \in [0, 36)$$

# COMMAND ----------

training_dist %>%
  sparklyr::spark_apply(
    predict, 
    names = c("t", "y"), 
    group_by = 'group'
  ) %>%
  collect %>%
  ggplot(aes(x = t, y=y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

