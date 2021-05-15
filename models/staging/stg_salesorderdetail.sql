with
    stg_salesorderdetail as (
        select
            /* Primary key */
            salesorderdetailid
            
            /* Foreign key */
            , salesorderid
            , productid	
            , specialofferid
            , rowguid

            , orderqty	
            , unitprice
            , unitpricediscount	
            , carriertrackingnumber
            , modifieddate	
            
            /* Stich columns */
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence	
            , _sdc_batched_at	
            , _sdc_extracted_at	as last_etl_run
            
            
        from {{source('adventure_works', 'salesorderdetail')}}
    )

select *
from stg_salesorderdetail