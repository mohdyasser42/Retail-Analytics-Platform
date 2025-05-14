# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Data Lake Storage Mount Points
# MAGIC This notebook establishes persistent mount points to connect Azure Databricks to our ADLS Gen2 storage account.
# MAGIC
# MAGIC **Note**: This notebook should be run by an administrator with appropriate permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC Define the storage account details and container paths.

# COMMAND ----------

# Storage account configuration
storage_account_name = "gfrdatastorage"
container_name = "global-fashion-retails-data"

# Variables for keys and IDs
# client_secret = "YQ28Q~lZTFse-aQHPOqkeoKEcb7f4v5kqJ8gvcmb"
scope = "db-client-secret"
secret_key_name = "db-client-secret"
service_credential = dbutils.secrets.get(scope= scope,key= secret_key_name)
application_id = "1c29e7f1-aef9-4c3d-a4df-fd3f11c8bf3a"
directory_id = "02045b3b-9790-4008-b83c-d155576e6c7e"

# ADLS directory paths
bronze_directory = "bronze"
silver_directory = "silver"
gold_directory = "gold"

# Mount point paths
bronze_mount = "/mnt/global_fashion/bronze"
silver_mount = "/mnt/global_fashion/silver"
gold_mount = "/mnt/global_fashion/gold"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication Setup
# MAGIC
# MAGIC We use OAuth for authentication and Azure Key Vault with service principals to get Client Secret.

# COMMAND ----------

print(service_credential)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC
# MAGIC These functions help us manage our mount points.

# COMMAND ----------

def mount_adls(storage_account_name, container_name, directory, mount_point):
    """
    Creates a mount point to ADLS if it doesn't already exist.
    
    Args:
        storage_account_name: The ADLS storage account name
        container_name: The container name
        directory: The directory within the container to mount
        mount_point: The desired mount point path
    
    Returns:
        Boolean indicating whether the mount was created successfully
    """
    
    # Configure the source ADLS path using abfss protocol
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory}"

    # Check if the mount point already exists
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == mount_point:
            print(f"Mount point {mount_point} already exists, pointing to {mount.source}")
            return True
        
    # Configure the storage account access using OAuth
    configs = {f"fs.azure.account.auth.type": "OAuth",
          f"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          f"fs.azure.account.oauth2.client.id": application_id,
          f"fs.azure.account.oauth2.client.secret": service_credential,
          f"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

    
    # Mount the storage using abfss protocol
    try:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"Mounted {source} to {mount_point}")
        return True
    except Exception as e:
        print(f"Error mounting {source} to {mount_point}: {e}")
        return False

# COMMAND ----------

def unmount_if_exists(mount_point):
    """
    Unmounts a mount point if it exists.
    
    Args:
        mount_point: Mount point path to remove
    
    Returns:
        Boolean indicating success
    """
    try:
        if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(mount_point)
            print(f"Unmounted {mount_point}")
            return True
        else:
            print(f"Mount point {mount_point} doesn't exist")
            return False
    except Exception as e:
        print(f"Error unmounting {mount_point}: {e}")
        return False

# COMMAND ----------

def list_all_mounts():
    """Lists all current mount points in the workspace."""
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Mount Points
# MAGIC
# MAGIC Establish mount points for each layer of our medallion architecture.

# COMMAND ----------

# Create the bronze layer mount point
bronze_success = mount_adls(
    storage_account_name=storage_account_name,
    container_name=container_name,
    directory=bronze_directory,
    mount_point=bronze_mount
)

# Create the silver layer mount point
silver_success = mount_adls(
    storage_account_name=storage_account_name,
    container_name=container_name,
    directory=silver_directory,
    mount_point=silver_mount
)

# Create the gold layer mount point
gold_success = mount_adls(
    storage_account_name=storage_account_name,
    container_name=container_name,
    directory=gold_directory,
    mount_point=gold_mount
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Mount Points
# MAGIC
# MAGIC List all mount points to verify they were created successfully.

# COMMAND ----------

# Display all active mount points
list_all_mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Mount Point Access
# MAGIC
# MAGIC List the contents of each mounted directory to verify access.

# COMMAND ----------

# Check if bronze mount directory has contents
try:
    # List files in the bronze directory
    bronze_files = dbutils.fs.ls(bronze_mount)
    
    # Check if there are any files
    if len(bronze_files) == 0:
        print("\nBronze layer is empty. No files to display.")
    else:
        print("\nBronze layer contents:")
        display(bronze_files)
except Exception as e:
    print(f"\nError accessing bronze layer: {e}")

# COMMAND ----------

# Check if silver mount directory has contents
try:
    # List files in the silver directory
    silver_files = dbutils.fs.ls(silver_mount)
    
    # Check if there are any files
    if len(silver_files) == 0:
        print("\nSilver layer is empty. No files to display.")
    else:
        print("\nSilver layer contents:")
        display(silver_files)
except Exception as e:
    print(f"\nError accessing silver layer: {e}")

# COMMAND ----------

# Check if gold mount directory has contents
try:
    # List files in the gold directory
    gold_files = dbutils.fs.ls(gold_mount)
    
    # Check if there are any files
    if len(gold_files) == 0:
        print("\nGold layer is empty. No files to display.")
    else:
        print("\nGold layer contents:")
        display(gold_files)
except Exception as e:
    print(f"\nError accessing gold layer: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Point Usage Instructions
# MAGIC
# MAGIC Now that these mount points are established, you can use them in your processing notebooks as follows:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from bronze layer
# MAGIC
# MAGIC bronze_stores_df = spark.read.csv(f"{bronze_mount}/GlobalFashion.Stores.csv", header=True, inferSchema=True)
# MAGIC
# MAGIC ### Write to silver layer
# MAGIC silver_stores_df.write.format("delta").mode("overwrite").save(f"{silver_mount}/stores")
# MAGIC
# MAGIC ### Read from silver for gold transformations
# MAGIC silver_stores_df = spark.read.format("delta").load(f"{silver_mount}/stores")
# MAGIC
# MAGIC ### Write to gold layer
# MAGIC store_metrics_df.write.format("delta").mode("overwrite").save(f"{gold_mount}/store_metrics")
# MAGIC
