# Databricks notebook source
spark

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Project-scope", key="App-Id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Project-scope", key="Project20-Secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='Project-scope',key='Tenant-Id')}/oauth2/token"
}

# COMMAND ----------

try:
    dbutils.fs.mount(
        source="abfss://bronze@project20adls.dfs.core.windows.net/",
        mount_point="/mnt/bronze",
        extra_configs=configs
    )
    
    dbutils.fs.mount(
        source="abfss://silver@project20adls.dfs.core.windows.net/",
        mount_point="/mnt/silver",
        extra_configs=configs
    )
    
    dbutils.fs.mount(
        source="abfss://gold@project20adls.dfs.core.windows.net/",
        mount_point="/mnt/gold",
        extra_configs=configs
    )
except Exception as e:
    print(f"Error mounting: {e}")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/")


# COMMAND ----------

dbutils.fs.ls("/mnt/gold")
