with
    selected1 as (
        select
            salesreasonid	
            , salesorderid	
            , modifieddate	

        from {{ref('stg_salesorderheaderreason')}}
    )
,   transformed1 as (
        select
        row_number () over (order by salesreasonid) as salesreason_sk
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
            transformed1.salesreasonid
            , transformed2.name	
            , transformed2.modifieddate
            , transformed2.reasontype

            from transformed1
            left join transformed2 on transformed1.salesreasonid = transformed2.salesreasonid	        
)

select *
from final