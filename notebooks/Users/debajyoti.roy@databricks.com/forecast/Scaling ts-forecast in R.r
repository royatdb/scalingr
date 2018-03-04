# Databricks notebook source
# MAGIC %md
# MAGIC # Horizontal scaling of R workloads using `sparklyr`
# MAGIC ![](https://media.licdn.com/dms/image/C5612AQGAryHIpVWYGw/article-inline_image-shrink_1500_2232/0?e=2119305600&v=alpha&t=fpo0YHH0Um_4p9VYUP5NFnWL2b_O_mZ0d9aob51RYNk)

# COMMAND ----------

library('sparklyr')
library('dplyr')
library('ggplot2')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mock time-series data

# COMMAND ----------

df_a = data.frame(t=c(0:17), y=c(0,0,1,3,5,6,6,7,9,11,12,12,13,15,17,18,18,19))
df_a$group='a'

df_b = data.frame(t=c(0:17), y=c(0,0,2,6,10,12,12,14,18,22,24,24,26,30,34,36,36, 38))
df_b$group='b'

df_c = data.frame(t=c(0:17), y=c(0,0,0,0,1,3,5,6,6,6,6,7,9,11,12,12,12,12))
df_c$group='c'

df_d = data.frame(t=c(0:17), y=c(4,4,5,7,9,10,10,11,13,15,16,16,17,19,21,22,22,23))
df_d$group='d'

local_df <- rbind(df_a, df_b, df_c, df_d)

# COMMAND ----------

ggplot(data = local_df, aes(x = t, y = y, color = group)) + geom_point() + geom_line() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Seasonal ARIMA forecasting (`next 6 observations`)

# COMMAND ----------

forecast_group <- function(data_group){
  library('forecast')
  library('tseries')
  
  ts_group <- ts(data_group[, c('y')], frequency=6)
  fit_group <- Arima(ts_group, order = c(0, 1, 0), seasonal = list(order = c(0, 1, 0), period = 6))
  forecast_group <- forecast(fit_group, 6)
  forecast_df_group <- data.frame(t =c(18:23), y=data.frame(forecast_group)[,1] )
  result_group <- rbind(forecast_df_group, data_group[c('t','y')])
  return(result_group)
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Single node \\(R\\)
# MAGIC ![](https://media.licdn.com/dms/image/C5612AQFuAFK7k9msZg/article-inline_image-shrink_1500_2232/0?e=2119258800&v=alpha&t=bnLbsb06QzYdSeU7p5ahZr6YRTNzHUf3PyYOkxTA1vo)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Forecast all groups serially

# COMMAND ----------

groups <- c('a','b','c', 'd')

serial_results <- NULL
for(group in groups){
  data_group <- local_df[local_df$group == group, ]
  result_group <- forecast_group(data_group)
  result_group$group <- group
  serial_results <- rbind(serial_results, result_group)
}

ggplot(data = serial_results, aes(x = t, y = y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC # Scaling \\(R\\)

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

dist_df <- copy_to(sc, local_df, name='ts', overwrite=TRUE, memory = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hello world of `spark_apply`

# COMMAND ----------

dist_df %>%
  spark_apply(
    function(data){
      data[1,c('t', 'y')]
    },
    group_by = 'group') %>% 
  collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Forecast all groups in parallel

# COMMAND ----------

dist_results <- dist_df %>%
  spark_apply(
    forecast_group,
    group_by = 'group') %>% 
  collect()

# COMMAND ----------

ggplot(data = dist_results, aes(x = t, y = y, color = group)) + geom_point() + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://media.licdn.com/media/gcrc/dms/image/C4D12AQEpH5UzTr8SkA/article-cover_image-shrink_720_1280/0?e=2119384800&v=alpha&t=ck6Xm2lRvHGHwdBnoXYUMgCQuN92CZY_zigGf8Jt7aQ)

# COMMAND ----------

# MAGIC %md
# MAGIC # References:
# MAGIC * [spark_apply](https://spark.rstudio.com/reference/spark_apply/)
# MAGIC * [forecast package](https://www.statmethods.net/advstats/timeseries.html)