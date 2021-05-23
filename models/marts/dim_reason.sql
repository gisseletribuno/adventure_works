with
    selected1 as (
        select
            salesorderid
            , salesreasonid		
            , modifieddate	

        from {{ref('stg_salesorderheaderreason')}}
    )

,   selected2 as (
        select
            salesreasonid
            , name	
            , reasontype
            , modifieddate

        from {{ref('stg_salesreason')}}            
    )

,   final1 as (
        select
            selected1.salesorderid
            , selected2.name	
            , selected2.reasontype
            , selected2.modifieddate


            from selected1
            left join selected2 on selected1.salesreasonid = selected2.salesreasonid	        
)

,   final2 as (
        select
        row_number () over (order by salesorderid) as reason_sk
        , *
    from final1
    )

select *
from final2