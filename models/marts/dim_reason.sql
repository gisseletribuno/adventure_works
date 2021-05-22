with
    selected1 as (
        select
            salesorderid
            , salesreasonid		
            , modifieddate	

        from {{ref('stg_salesorderheaderreason')}}
    )
,   transformed1 as (
        select
        row_number () over (order by salesorderid) as salesreason_sk
        , *
    from selected1
)

,   selected2 as (
        select
            salesreasonid
            , name	
            , modifieddate
            , reasontype

        from {{ref('stg_salesreason')}}            
    )
,   transformed2 as (
        select
        row_number () over (order by salesreasonid) as salesreason_sk
        , *
    from selected2
)

,   final as (
        select
            transformed1.salesreason_sk
            , transformed2.name	
            , transformed2.modifieddate
            , transformed2.reasontype

            from transformed1
            left join transformed2 on transformed1.salesreason_sk = transformed2.salesreasonid	        
)

select *
from final