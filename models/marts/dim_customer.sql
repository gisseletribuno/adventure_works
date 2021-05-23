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

,   selected3 as (
        select
            businessentityid
            , creditcardid	
            , modifieddate

        from {{ref('stg_personcreditcard')}}            
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

,   final1 as (
        select
            selected1.customerid
            , selected1.personid
            , selected1.territoryid
            , selected1.storeid
            , selected2.businessentityid
            , selected2.title
            , selected2.lastname
            , selected2.firstname
            , selected2.middlename
            , selected2.suffix
            , selected2.emailpromotion
            , selected2.persontype
            , selected2.namestyle
            , selected3.creditcardid
            , selected4.cardtype
            , selected4.cardnumber
            , selected4.expmonth
            , selected4.expyear	
            , selected1.rowguid
            , selected1.modifieddate
            from selected1
            left join selected2 on selected1.personid = selected2.businessentityid
            left join selected3 on selected2.businessentityid = selected3.businessentityid 
            left join selected4 on selected3.creditcardid = selected4.creditcardid  
)

,   final2 as (
        select
        row_number () over (order by customerid) as customer_sk
        , *
    from final1
    )

select *
from final2