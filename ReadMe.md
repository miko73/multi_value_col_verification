# pyspark interesting SQL examples
##  SQL highilights
### multi_value_filed_test(spark)
   cdd_more_nationalities comma-separated list of countries that we want to verify against the country code. The result should be a list of countries that are also in the master table .S2_COUNTRY_TAB
 
    +------------+----------------------+
    |customer_key|cdd_more_nationalities|
    +------------+----------------------+
    |1           |CZ,SK,valerr,US,GB,LIT|
    |2           |valerr,US             |
    |3           |SK,CZ,valerr          |
    +------------+----------------------+
    
    Čísleník zemí
    +---+------------+
    |a  |COUNTRY_CODE|
    +---+------------+
    |1  |CZ          |
    |1  |SK          |
    |1  |US          |
    |1  |LIT         |
    +---+------------+
    
    From the field cdd_more_nationalitieies of constructions by combinations of split and lateral 
    lateral view outer explode( split(cdd_more_nationalities,',') ) sc_ex as cc_country
    we create a table of country identifiers for cc_country
    
    +----------+-------------+
    |cc_country|country_c_key|
    +----------+-------------+
    |CZ        |1            |
    |GB        |1            |
    |valerr    |1            |
    |LIT       |1            |
    |US        |1            |
    |SK        |1            |
    |valerr    |2            |
    |US        |2            |
    |SK        |3            |
    |valerr    |3            |
    |CZ        |3            |
    +----------+-------------+
    
    Validated data before final assembly of collect_set
    
    22/09/01 09:41:25 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
    +-------------+----------------------+
    |country_c_key|cdd_more_nationalities|
    +-------------+----------------------+
    |1            |US                    |
    |1            |null                  |
    |1            |null                  |
    |1            |CZ                    |
    |1            |SK                    |
    |1            |LIT                   |
    |2            |US                    |
    |2            |null                  |
    |3            |CZ                    |
    |3            |SK                    |
    |3            |null                  |
    +-------------+----------------------+
    
    Finally, we put the records back together with the collect_set and concat_ws functions according to group by country_c_key
    
    +-------------+-------------------------+----------------------+
    |country_c_key|collect_set(COUNTRY_CODE)|cdd_more_nationalities|
    +-------------+-------------------------+----------------------+
    |1            |[LIT, US, CZ, SK]        |LIT,US,CZ,SK          |
    |2            |[US]                     |US                    |
    |3            |[CZ, SK]                 |CZ,SK                 |
    +-------------+-------------------------+----------------------+
    
    To compare the original data in SOURCE_CUSTOMERS_TAB 
    
    +------------+----------------------+
    |customer_key|cdd_more_nationalities|
    +------------+----------------------+
    |1           |CZ,SK,valerr,US,GB,LIT|
    |2           |valerr,US             |
    |3           |SK,CZ,valerr          |
    +------------+----------------------+
 

### GPS_test(spark)
    calculate the distance between two GPS coordinates 
### choose_test(spark)
    example how to reimplement Oracle function choose in HIVE
## advanced pyspark dataframe operations
### merge_two_datasets(spark)
### load_only_new_and_modified(spark)
