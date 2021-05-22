with
    selected1 as (
        select
            customerid
            , personid
            , territoryid
            , storeid
            , rowguid
            , modifieddate	

        from {{ref('stg_customer')}}
    )
,   transformed1 as (
        select
        row_number () over (order by customerid) as customer_sk
        , *
    from selected1
)

,   selected2 as (
        select
            businessentityid
            , title
            , lastname
            , firstname
            , middlename
            , rowguid
            , emailpromotion
            , persontype
            , namestyle
            , suffix
            , modifieddate

        from {{ref('stg_person')}}            
    )
,   transformed2 as (
        select
        row_number () over (order by businessentityid) as person_sk
        , *
    from selected2
)

,   selected3 as (
        select
            businessentityid
            , creditcardid	
            , modifieddate

        from {{ref('stg_personcreditcard')}}            
    )
,   transformed3 as (
        select
        row_number () over (order by businessentityid) as personcreditcard_sk
        , *
    from selected3
)

,   selected4 as (
        select
            creditcardid
            , cardtype
            , cardnumber
            , expmonth
            , expyear	
            , modifieddate

        from {{ref('stg_creditcard')}}            
    )
,   transformed4 as (
        select
        row_number () over (order by creditcardid) as creditcard_sk
        , *
    from selected4
)

,   final as (
        select
            transformed1.personid
            , transformed1.customerid
            , transformed1.territoryid
            , transformed1.storeid
            , transformed2.businessentityid
            , transformed2.title
            , transformed2.lastname
            , transformed2.firstname
            , transformed2.middlename
            , transformed2.suffix
            , transformed2.emailpromotion
            , transformed2.persontype
            , transformed2.namestyle
            , transformed3.creditcardid
            , transformed4.cardtype
            , transformed4.cardnumber
            , transformed4.expmonth
            , transformed4.expyear	
            , transformed1.rowguid
            , transformed1.modifieddate
            from transformed1
            left join transformed2 on transformed1.personid = transformed2.businessentityid
            left join transformed3 on transformed2.businessentityid = transformed3.businessentityid 
            left join transformed4 on transformed3.creditcardid = transformed4.creditcardid  
)

select *
from final