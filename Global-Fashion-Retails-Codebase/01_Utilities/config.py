# Databricks notebook source
# MAGIC %md
# MAGIC # Project Configuration
# MAGIC
# MAGIC This Notebook Defines Configuration Parameters for The Retail Analytics Platform.

# COMMAND ----------

# Environment Configuration

# These parameters define the execution environment.
# Environment selector (dev, test, prod)
ENVIRONMENT = "dev"

# COMMAND ----------

# Storage Configuration

# These parameters define storage locations and settings.
# Storage account settings
STORAGE_ACCOUNT = "globalretaildatastorage"
CONTAINER_NAME = "global-fashion-retail-data"

# Mount points
MOUNT_POINTS = {
  "bronze": "/mnt/global_fashion/bronze",
  "silver": "/mnt/global_fashion/silver",
  "gold": "/mnt/global_fashion/gold",
  "monitoring": "/mnt/global_fashion/monitoring"
}

# File formats
FILE_FORMATS = {
    "bronze": "csv",
    "silver": "delta",
    "gold": "delta"
}

# Data paths
BRONZE_PATHS = {
    "stores": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Stores.csv",
    "employees": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Employees.csv",
    "products": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Products.csv",
    "discounts": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Discounts.csv",
    "customers": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Customers.csv",
    "transactions": f"{MOUNT_POINTS['bronze']}/GlobalFashion.Transactions.csv"
}

SILVER_PATHS = {
    "stores": f"{MOUNT_POINTS['silver']}/stores",
    "employees": f"{MOUNT_POINTS['silver']}/employees", 
    "products": f"{MOUNT_POINTS['silver']}/products",
    "discounts": f"{MOUNT_POINTS['silver']}/discounts",
    "customers": f"{MOUNT_POINTS['silver']}/customers",
    "transactions": f"{MOUNT_POINTS['silver']}/transactions"
}

GOLD_PATHS = {
    "stores": f"{MOUNT_POINTS['gold']}/stores",
    "employees": f"{MOUNT_POINTS['gold']}/employees", 
    "products": f"{MOUNT_POINTS['gold']}/products",
    "discounts": f"{MOUNT_POINTS['gold']}/discounts",
    "customers": f"{MOUNT_POINTS['gold']}/customers",
    "invoice_fact": f"{MOUNT_POINTS['gold']}/invoice_fact",
    "invoice_line_items": f"{MOUNT_POINTS['gold']}/invoice_line_items",
    "exchange_rates": f"{MOUNT_POINTS['gold']}/exchange_rates",
    "customer_segments": f"{MOUNT_POINTS['gold']}/customer_segments"

}

# COMMAND ----------

# Processing Configuration

# These parameters control data processing behavior.
# Write modes
WRITE_MODES = {
    "dev": "overwrite",  # For development, always overwrite 
    "test": "overwrite", # For Testing, always overwrite
    "prod": "append"     # For production, append to maintain history
}

# Get the appropriate write mode for current environment
WRITE_MODE = WRITE_MODES.get(ENVIRONMENT, "overwrite")

# Schema inference vs. explicit schema
INFER_SCHEMA = True if ENVIRONMENT == "dev" else False

# Delta options for optimized writing
DELTA_OPTIONS = {
# Performance optimizations
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "overwriteSchema": "true"
}

# COMMAND ----------

## Data Configuration

# Data translation configurations
TRANSLATION_CONFIG = {
    "country_replacements": {
        "中国": "China", 
        "España": "Spain", 
        "Deutschland": "Germany"
    },
    "city_replacements": {
        "München": "munich",
        "Köln": "cologne",
        "Frankfurt am Main": "frankfurt",
        "Lisboa": "lisbon",
        "Guimarães": "guimaraes",
        "万州": "Wanzhou",
        "上海": "Shanghai",
        "东莞": "Dongguan",
        "中山": "Zhongshan",
        "丰台": "Fengtai",
        "乐山": "Leshan",
        "佛山": "Foshan",
        "保定": "Baoding",
        "北京": "Beijing",
        "南山": "Nanshan",
        "南开": "Nankai",
        "南通": "Nantong",
        "双流": "Shuangliu",
        "合川": "Hechuan",
        "吴中": "Wuzhong",
        "和平": "Heping",
        "咸宁": "Xianning",
        "咸阳": "Xianyang",
        "唐山": "Tangshan",
        "嘉兴": "Jiaxing",
        "嘉定": "Jiading",
        "大足": "Dazu",
        "天河": "Tianhe",
        "天津": "Tianjin",
        "太仓": "Taicang",
        "姑苏": "Gusu",
        "宝安": "Bao'an",
        "宝山": "Baoshan",
        "宝鸡": "Baoji",
        "常熟": "Changshu",
        "广州": "Guangzhou",
        "廊坊": "Langfang",
        "张家口": "Zhangjiakou",
        "张家港": "Zhangjiagang",
        "德阳": "Deyang",
        "惠州": "Huizhou",
        "成都": "Chengdu",
        "承德": "Chengde",
        "新城": "Xincheng",
        "新洲": "Xinzhou",
        "无锡": "Wuxi",
        "昆山": "Kunshan",
        "朝阳": "Chaoyang",
        "杭州": "Hangzhou",
        "武侯": "Wuhou",
        "武昌": "Wuchang",
        "武汉": "Wuhan",
        "汉中": "Hanzhong",
        "汉阳": "Hanyang",
        "江北": "Jiangbei",
        "江岸": "Jiang'an",
        "江汉": "Jianghan",
        "沙坪坝": "Shapingba",
        "沧州": "Cangzhou",
        "河东": "Hedong",
        "河西": "Hexi",
        "浦东": "Pudong",
        "海淀": "Haidian",
        "涪陵": "Fuling",
        "深圳": "Shenzhen",
        "清远": "Qingyuan",
        "渝中": "Yuzhong",
        "渝北": "Yubei",
        "渭南": "Weinan",
        "滨海": "Binhai",
        "澳门": "Macau",
        "珠海": "Zhuhai",
        "璧山": "Bishan",
        "白云": "Baiyun",
        "相城": "Xiangcheng",
        "眉山": "Meishan",
        "石家庄": "Shijiazhuang",
        "硚口": "Qiaokou",
        "碑林": "Beilin",
        "福田": "Futian",
        "秦皇岛": "Qinhuangdao",
        "绵阳": "Mianyang",
        "苏州": "Suzhou",
        "苏州（江苏省）": "Suzhou (Jiangsu)",
        "莲湖": "Lianhu",
        "虎丘": "Huqiu",
        "虹口": "Hongkou",
        "西安": "Xi'an",
        "资阳": "Ziyang",
        "越秀": "Yuexiu",
        "通州": "Tongzhou",
        "鄂州": "Ezhou",
        "重庆": "Chongqing",
        "铜川": "Tongchuan",
        "锦江": "Jinjiang",
        "长安": "Chang'an",
        "闵行": "Minhang",
        "雁塔": "Yanta",
        "青羊": "Qingyang",
        "香港": "Hong Kong",
        "黄冈": "Huanggang",
        "黄陂": "Huangpi",
        "黔江": "Qianjiang",
        "龙岗": "Longgang",
        "龙泉驿": "Longquanyi"
    }
}

# COMMAND ----------

# Defining the pattern to match all the specified academic titles and abbrivatives (case insensitive)
name_pattern = r'(?i)\b(B\.Sc\.|B\.A\.|Univ\.Prof\.|B\.Eng\.|MBA\.|Dipl\.-Ing\.|Ing\.|Prof\.|MD|DDS|Dr\.|Mrs\.|DVM)\s*'

# COMMAND ----------

# API Key for freecurrencyrates
API_KEY = "fca_live_51FEQf9X9RgeNzLLvIS117ux7GnWnAuOlRyOztcx"

# COMMAND ----------

# Helper Functions

# These functions provide easy access to configuration values.

def get_bronze_path(dataset_name):
    """Gets the bronze layer path for a dataset."""
    return BRONZE_PATHS.get(dataset_name)

def get_silver_path(dataset_name):
    """Gets the silver layer path for a dataset."""
    return SILVER_PATHS.get(dataset_name)
    
def get_gold_path(dataset_name):
    """Gets the gold layer path for a dataset."""
    return GOLD_PATHS.get(dataset_name)
