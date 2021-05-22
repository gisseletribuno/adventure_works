with
    stg_employee as (
        select
            /* Primary key */
            businessentityid
            	
            , nationalidnumber
            , loginid	
            , jobtitle
            , birthdate	        
            , gender	
            , maritalstatus
            , hiredate	
            , salariedflag	
            , sickleavehours	
            , organizationnode		
            , vacationhours
            , currentflag
            , modifieddate	
            , rowguid	            
            
            /* Stich columns */	
            , _sdc_table_version	
            , _sdc_received_at	
            , _sdc_sequence
            , _sdc_batched_at	
            , _sdc_extracted_at as last_etl_run
            	

        from {{source('adventure_works', 'employee')}}
    )

select *
from stg_employee