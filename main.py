import logging
import json

from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import MapType, StringType, StructType, StructField, TimestampType, IntegerType, DoubleType, BooleanType, ArrayType, LongType, FloatType, DateType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
def load_only_new_and_modified(spark):
    # spark.sql(f"show tables").show(truncate=False)
    # spark.sql(f"select * from default.SOURCE_CUSTOMERS_TAB").show(truncate=False)
    # spark.sql(f"select * from default.S2_COUNTRY_TAB").show(truncate=False)
    current_data = spark.createDataFrame([
        Row(key1=1, key2=1 , col1='rec1_tab1', col2=None, col3='rec1_tab1'),
        Row(key1=2, key2=2 , col1=None  , col2=None, col3='rec2_tab1'),
        Row(key1=3, key2=3 , col1='rec3_tab1', col2='rec3_tab1', col3='rec3_tab1')
    ])
    # s1_data.createOrReplaceTempView(f"SOURCE_TABLE_1")
    # spark.sql(f"select * from SOURCE_TABLE_1").show(truncate=False)
    # df.write.partitionBy('org_unit_code', 'cob_date').format('parquet').saveAsTable(""))
    # spark.sql(f"DROP TABLE IF EXISTS SOURCE_CUSTOMERS_TAB")
    # df.write.format('parquet').saveAsTable("SOURCE_CUSTOMERS_TAB")

    new_icomming_data = spark.createDataFrame([
        Row(key1=1, key2=1 , col1='rec1_tab1', col2=None, col3='rec1_tab1'),
        Row(key1=2, key2=2 , col1='rec2_tab2'  , col2='rec2_tab2', col3='rec2_tab2'),
        Row(key1=3, key2=3 , col1='rec3_tab2', col2='rec3_tab2', col3='rec3_tab2'),
        Row(key1=4, key2=4 , col1='rec4_tab2', col2='rec4_tab2', col3='rec4_tab2'),
        Row(key1=4, key2=4, col1='rec5_tab2', col2='rec5_tab2', col3='rec5_tab2')
    ])

    primary_key = ['key1', 'key2']
    data_columns = ['col1','col2','col3']
    finally_ordered_columns = ['col3','col2','col1', 'key1', 'key2', 'RECORD_STATUS']

    merged_df = current_data \
        .select(current_data['*'], F.concat_ws('||', *primary_key).alias("s1_key_id"), F.sha2(F.concat_ws('||', *data_columns), 256).alias("hash") ) \
        .join( new_icomming_data.select( F.concat_ws('||', *primary_key).alias("s2_key_id"),
                              F.sha2(F.concat_ws('||', *data_columns), 256).alias("s2_hash"),
                                                                   F.col("key1").alias("s2_key1"),
                                                                   F.col("key2").alias("s2_key2"),
                                                                   F.col("col1").alias("s2_col1"),
                                                                   F.col("col2").alias("s2_col2"),
                                                                   F.col("col3").alias("s2_col3")
                             )
              , F.col("s1_key_id") == F.col("s2_key_id")
              , how='fullouter').select(
         F.when(F.col('col1').isNull(), F.col('s2_col1') ).otherwise(F.col('col1')).alias("col1")
        ,F.when(F.col('col2').isNull(), F.col('s2_col2') ).otherwise(F.col('col2')).alias("col2")
        ,F.when(F.col('col3').isNull(), F.col('s2_col3')).otherwise(F.col('col3')).alias("col3")
        ,F.when(F.col('key1').isNull(), F.col('s2_key1')).otherwise(F.col('key1')).alias("key1")
        ,F.when(F.col('key2').isNull(), F.col('s2_key2')).otherwise(F.col('key2')).alias("key2")
        ,F.col('hash')
        ,F.col('s2_hash')
        ,F.when(F.col('s1_key_id').isNull(), F.col('s2_key_id')).otherwise(F.col('s1_key_id')).alias("key_id")
    ).withColumn("RECORD_STATUS", F.when(F.col('hash').isNull(), F.lit("NEW_REC")).otherwise(
        F.when(F.col('hash') == F.col('s2_hash'), F.lit("NO_CHANGE")).otherwise(F.lit("CHANGED"))
    )).select(*finally_ordered_columns).show(truncate=False)





def merge_two_datasets(spark):
    # spark.sql(f"show tables").show(truncate=False)
    # spark.sql(f"select * from default.SOURCE_CUSTOMERS_TAB").show(truncate=False)
    # spark.sql(f"select * from default.S2_COUNTRY_TAB").show(truncate=False)
    s1_data = spark.createDataFrame([
        Row(key1=1, key2=1 , col1='rec1_tab1', col2=None, col3='rec1_tab1'),
        Row(key1=2, key2=2 , col1=None  , col2=None, col3='rec2_tab1'),
        Row(key1=3, key2=3 , col1='rec3_tab1', col2='rec3_tab1', col3='rec3_tab1')
    ])
    # s1_data.createOrReplaceTempView(f"SOURCE_TABLE_1")
    # spark.sql(f"select * from SOURCE_TABLE_1").show(truncate=False)
    # df.write.partitionBy('org_unit_code', 'cob_date').format('parquet').saveAsTable(""))
    # spark.sql(f"DROP TABLE IF EXISTS SOURCE_CUSTOMERS_TAB")
    # df.write.format('parquet').saveAsTable("SOURCE_CUSTOMERS_TAB")
    s2_data = spark.createDataFrame([
        Row(key1=1, key2=1 , col1='rec1_tab2', col2='rec1_tab2', col3='rec1_tab2'),
        Row(key1=2, key2=2 , col1='rec2_tab2'  , col2='rec2_tab2', col3='rec2_tab2'),
        Row(key1=3, key2=3 , col1='rec3_tab2', col2='rec3_tab2', col3='rec3_tab2'),
        Row(key1=4, key2=4 , col1='rec4_tab2', col2='rec4_tab2', col3='rec4_tab2')
    ])

    primary_key = ['key1', 'key2']
    data_columns = ['col1','col2','col3']
    finally_ordered_columns = ['col3','col2','col1', 'key1', 'key2']

    s2_data.createOrReplaceTempView(f"SOURCE_TABLE_2")
    # spark.sql(f"select * from SOURCE_TABLE_2").show(truncate=False)

    s2_data.show(truncate=False)

    s2_data.select(F.concat_ws('||', *data_columns).alias("s2_key_id"),
                   F.sha2(F.concat_ws('||', *data_columns), 256).alias("s2_hash"),
                   F.col("col1").alias("s2_col1"),
                   F.col("col2").alias("s2_col2"),
                   F.col("col3").alias("s2_col3")
                   ).show(truncate=False)

    merged_df = s1_data \
        .select(s1_data['*'], F.concat_ws('||', *primary_key).alias("s1_key_id"), F.sha2(F.concat_ws('||', *data_columns), 256).alias("s1_hash") ) \
        .join( s2_data.select( F.concat_ws('||', *primary_key).alias("s2_key_id"),
                              F.sha2(F.concat_ws('||', *data_columns), 256).alias("s2_hash"),
                                                                   F.col("key1").alias("s2_key1"),
                                                                   F.col("key2").alias("s2_key2"),
                                                                   F.col("col1").alias("s2_col1"),
                                                                   F.col("col2").alias("s2_col2"),
                                                                   F.col("col3").alias("s2_col3")
                             )
              , F.col("s1_key_id") == F.col("s2_key_id")
              , how='fullouter').select(
         F.when(F.col('col1').isNull(), F.col('s2_col1') ).otherwise(F.col('col1')).alias("col1")
        ,F.when(F.col('col2').isNull(), F.col('s2_col2') ).otherwise(F.col('col2')).alias("col2")
        ,F.when(F.col('col3').isNull(), F.col('s2_col3')).otherwise(F.col('col3')).alias("col3")
        ,F.when(F.col('key1').isNull(), F.col('s2_key1')).otherwise(F.col('key1')).alias("key1")
        ,F.when(F.col('key2').isNull(), F.col('s2_key2')).otherwise(F.col('key2')).alias("key2")
        , F.when(F.col('s1_key_id').isNull(), F.col('s2_key_id')).otherwise(F.col('s1_key_id')).alias("key_id")
    ).select(*finally_ordered_columns)



    # F.when(
    #         ( ~F.col('TENANT').isin(self.tenant_code_noat_in.split(',')) )
    #         , F.lit(self.at_tenant_code_num))\
    #     .otherwise(F.col('TENANT'))
    #


    merged_df.show(truncate=False)

def GPS_test(spark):
    print("Input data: \n ")
    # adastra 50.0949405, 14.4441918
    # karlin 50.0927718, 14.4433674
    # jinonice 50.0635877, 14.3730089
    # brno 49.1679774,16.5581405
    # 50.0643304, 14.3607352
    # zlata 40.7673944,-73.9299537
    df = spark.createDataFrame([
        Row(start='Adastra', start_x_center=50.0949405, start_y_center=14.4441918, cil='Dva Kohouti', cil_x_center=50.0927718, cil_y_center=14.4433674),
        Row(start='Adastra', start_x_center=50.0949405, start_y_center=14.4441918, cil='Uhelne Sklady Košíře', cil_x_center=50.0643304, cil_y_center=14.3607352),
        Row(start='Adastra', start_x_center=50.0949405, start_y_center=14.4441918, cil='Restaurace Plzeňský Dvůr Brno', cil_x_center=49.2366693, cil_y_center=16.5581405),
        Row(start='Adastra', start_x_center=50.0949405, start_y_center=14.4441918, cil='Zlatá Praha Queens, New York', cil_x_center=40.7673944, cil_y_center=-73.9299537)
    ])
    df.createOrReplaceTempView(f"start_cil")
    df.show()

    print (f'''select  start, cil, 
    2 * asin(
        sqrt(
          cos(radians(start_x_center)) *
          cos(radians(cil_x_center)) *
          pow(sin(radians((start_y_center - cil_y_center)/2)), 2)
              +
          pow(sin(radians((start_x_center - cil_x_center)/2)), 2)
        )
      ) * 6371 as distance_km from  start_cil; \n\n''')

    spark.sql(f'''select  start, cil, 
    2 * asin(
        sqrt(
          cos(radians(start_x_center)) *
          cos(radians(cil_x_center)) *
          pow(sin(radians((start_y_center - cil_y_center)/2)), 2)
              +
          pow(sin(radians((start_x_center - cil_x_center)/2)), 2)
        )
      ) * 6371 as distance_km from  start_cil;''').show(truncate=False)
    return


def multi_value_filed_test(spark):
    # cdd_more_nationalities comma-separated list of countries that we want to verify against the country code.
    # The result should be a list of countries that are also in the master table .S2_COUNTRY_TAB
    print("cdd_more_nationalities comma-separated list of countries that we want to verify against the country code. "
          "The result should be a list of countries that are also in the master table .S2_COUNTRY_TAB")
    df = spark.createDataFrame([
        Row(customer_key=1, cdd_more_nationalities='CZ,SK,valerr,US,GB,LIT'),
        Row(customer_key=2, cdd_more_nationalities='valerr,US'),
        Row(customer_key=3, cdd_more_nationalities='SK,CZ,valerr')
    ])

    df.createOrReplaceTempView(f"SOURCE_CUSTOMERS_TAB")
    spark.sql(f"select * from SOURCE_CUSTOMERS_TAB").show(truncate=False)

    df = spark.createDataFrame([
        Row(a=1, COUNTRY_CODE='CZ'),Row(a=1, COUNTRY_CODE='SK'),Row(a=1, COUNTRY_CODE='US'),Row(a=1, COUNTRY_CODE='LIT')
    ])
    df.createOrReplaceTempView(f"S2_COUNTRY_TAB")

    print("Čísleník zemí")
    spark.sql(f"select * from S2_COUNTRY_TAB").show(truncate=False)

#     spark.sql(f'''
#     select customer.cdd_more_nationalities orig_value,
#     case when coalesce(country_t.cdd_more_nationalities, customer.cdd_more_nationalities) = '' then null else coalesce(country_t.cdd_more_nationalities, customer.cdd_more_nationalities) end as CDD_MORE_NATIONALITIES
#     from  SOURCE_CUSTOMERS_TAB as customer
# left join
# (
# select country_c_key, concat_ws(',', collect_set(res_data.COUNTRY_CODE)) as cdd_more_nationalities from
# (
#     select country.COUNTRY_CODE, explode_t.* from (
#     select DISTINCT cc_country, cust.customer_key as country_c_key from SOURCE_CUSTOMERS_TAB cust
#     lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country
#     where
#     cdd_more_nationalities is not null
#     ) explode_t
#     left join S2_COUNTRY_TAB country on country.COUNTRY_CODE = explode_t.cc_country) as res_data
#     group by country_c_key
# ) country_t on customer.customer_key = country_t.country_c_key
    print("From the field cdd_more_nationalitieies of constructions by combinations of split and lateral \n"
          "lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country\n"
           "we create a table of country identifiers for cc_country\n")

    spark.sql('''
        select DISTINCT cc_country, cust.customer_key as country_c_key 
        from SOURCE_CUSTOMERS_TAB cust
        lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country
        where
        cdd_more_nationalities is not null order by country_c_key
    ''').show(truncate=False)
# ''').show(truncate=False)

    print("Validated data before final assembly of collect_set\n")

    spark.sql('''
    select country_c_key, res_data.COUNTRY_CODE as cdd_more_nationalities from
    (
        select country.COUNTRY_CODE, explode_t.* from (
        select DISTINCT cc_country, cust.customer_key as country_c_key from SOURCE_CUSTOMERS_TAB cust
        lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country
        where
        cdd_more_nationalities is not null
        ) explode_t
        left join S2_COUNTRY_TAB country on country.COUNTRY_CODE = explode_t.cc_country) as res_data
        order by country_c_key
    ''').show(truncate=False)

    print("Finally, we put the records back together with the collect_set and concat_ws functions according to group by country_c_key\n")

    spark.sql('''
select country_c_key, collect_set(res_data.COUNTRY_CODE), concat_ws(',', collect_set(res_data.COUNTRY_CODE)) as cdd_more_nationalities from
(
    select country.COUNTRY_CODE, explode_t.* from (
    select DISTINCT cc_country, cust.customer_key as country_c_key from SOURCE_CUSTOMERS_TAB cust
    lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country
    where
    cdd_more_nationalities is not null
    ) explode_t
    left join S2_COUNTRY_TAB country on country.COUNTRY_CODE = explode_t.cc_country) as res_data
    group by country_c_key  order by country_c_key
''').show(truncate=False)

    print("To compare the original data in SOURCE_CUSTOMERS_TAB \n")
    spark.sql(f"select * from SOURCE_CUSTOMERS_TAB").show(truncate=False)


def choose_test(spark):
    print("Vstupní data: \n "
          "")
    df = spark.createDataFrame([
        Row(T3_COUNTRY_CODE=None, GRP_DF_INDEX_GEN=1),
        Row(T3_COUNTRY_CODE='T3_COUNTRY_CODE value', GRP_DF_INDEX_GEN=2),
        Row(T3_COUNTRY_CODE=None, GRP_DF_INDEX_GEN=3),
        Row(T3_COUNTRY_CODE=None, GRP_DF_INDEX_GEN=4)
    ])

    df.createOrReplaceTempView(f"SOURCE_CUSTOMERS_TAB")
    print(''' select T3_COUNTRY_CODE, GRP_DF_INDEX_GEN, 
    coalesce(T3_COUNTRY_CODE,case GRP_DF_INDEX_GEN when 1 then '11' when 2 then '22' when 3 then '33' when 4 then '44' when 5 then '55' when 6 then '66' end ) COUNTRY_OF_RESIDENCE
    from SOURCE_CUSTOMERS_TAB
    ''')

    spark.sql(''' select T3_COUNTRY_CODE, GRP_DF_INDEX_GEN, 
    coalesce(T3_COUNTRY_CODE,case GRP_DF_INDEX_GEN when 1 then '11' when 2 then '22' when 3 then '33' when 4 then '44' when 5 then '55' when 6 then '66' end ) COUNTRY_OF_RESIDENCE
    from SOURCE_CUSTOMERS_TAB
    ''').show(truncate=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    warehouse_location = abspath('spark-warehouse')
    spark: SparkSession = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .master("local[1]") \
        .getOrCreate()

    # SQL highilights
    multi_value_filed_test(spark)
    merge_two_datasets(spark)
    GPS_test(spark)
    # advanced pyspark dataframe operations
    load_only_new_and_modified(spark)
    choose_test(spark)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
