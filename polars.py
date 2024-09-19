import polars as pl
from datetime import datetime

# Assuming df is already a Polars DataFrame
# Standardizing data

region_map = {
    1: "NAM",
    2: "EMEA",
    3: "APAC",
    4: "LATAM"
}

df = df.with_columns([
    pl.col("REGION_ID").apply(lambda region: region_map.get(region, "Unknown")).alias("REGION_ID"),
    pl.lit("SECTOR").alias("SECTOR"),
    pl.lit(datetime.utcnow()).alias("LOG_DATE"),
    pl.col("CN_CERT_EXPIRY_DATE").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"),
    pl.col("CN_CERT_START_DATE").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"),
    (pl.col("CN_CERT_EXPIRY_DATE") - pl.lit(datetime.utcnow())).dt.days().alias("DAYS_TO_EXPIRE"),
    (pl.col("DAYS_TO_EXPIRE").apply(lambda x: "Valid" if x > 1 else "Expired")).alias("STATUS")
])

# Selecting columns
df = df.select([
    "CN_URL",
    "CN_SERIAL",
    "CN_CERT_START_DATE",
    "CN_CERT_EXPIRY_DATE",
    "STATUS",
    "CN_CERT_OWNER",
    "CN_EMAIL",
    "CN_GROUNNAME",
    "CN_ESEMAIL",
    "CN_CSI_APP_ID",
    "CN_STATUS",
    "CN_ENV",
    "CN_CERT_TYPE",
    "DAYS_TO_EXPIRE",
    "SECTOR",
    "REGION_ID",
    "LOG_DATE"
])

# Renaming columns
df = df.rename({
    "CN_URL": "distinguished_name",
    "CN_SERIAL": "serial_number",
    "CN_CERT_START_DATE": "start_date",
    "CN_CERT_EXPIRY_DATE": "expiration_date",
    "STATUS": "status",
    "CN_CERT_OWNER": "certificate_owner",
    "CN_EMAIL": "owner_email",
    "CN_GROUNNAME": "support_group",
    "CN_ESEMAIL": "support_group_email",
    "CN_CSI_APP_ID": "csi_application_id",
    "CN_STATUS": "csi_cm_status",
    "CN_ENV": "environment",
    "CN_CERT_TYPE": "certificate_type",
    "DAYS_TO_EXPIRE": "days_to_expiration",
    "SECTOR": "csi_cm_sector",
    "REGION_ID": "csi_cm_region",
    "LOG_DATE": "log_date"
})