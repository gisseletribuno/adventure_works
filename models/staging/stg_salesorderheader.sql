with
    stg_salesorderheader as (
        select
            /* Primary key */
            salesorderid

            /* Foreign key */
            , purchaseordernumber
            , customerid
            , creditcardid
            , shipmethodid	
            , shiptoaddressid
            , billtoaddressid	
            , salespersonid	
            , territoryid
            , currencyrateid
            , rowguid	
            
            , status		
            , subtotal		
            , taxamt	
            , onlineorderflag            
            , freight
            , orderdate
            , duedate	
            , totaldue	
            , shipdate	
            , creditcardapprovalcode
            , revisionnumber
            , accountnumber
            , modifieddate	
            
            /* Stich columns */
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence	            	
            , _sdc_batched_at	
            , _sdc_extracted_at	as last_etl_run
            
            
        from {{source('adventure_works', 'salesorderheader')}}
    )

select *
from stg_salesorderheader