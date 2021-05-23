with
    customer as (
        select *
        from {{ ref ('dim_customer')}}
    )
    , employee as (
        select *
        from {{ ref ('dim_employee')}}
    )
    , product as (
        select *
        from {{ ref ('dim_product')}}
    )
    , reason as (
        select *
        from {{ ref ('dim_reason')}}
    )
    , location as (
        select *
        from {{ ref ('dim_location')}}
    )
    
    , salesorderheader_with_sk as (
        select
        salesorderheader.salesorderid
        , customer.customer_sk as customer_fk
        , employee.employee_sk as employee_fk
        , reason.reason_sk as reason_fk
        , location.location_sk as location_fk
        , salesorderheader.purchaseordernumber
        --, creditcardid
        , salesorderheader.shipmethodid	
        , salesorderheader.shiptoaddressid
        , salesorderheader.billtoaddressid	
        , salesorderheader.salespersonid	
        , salesorderheader.territoryid
        , salesorderheader.currencyrateid
        , salesorderheader.rowguid	
        , salesorderheader.status		
        , salesorderheader.subtotal		
        , salesorderheader.taxamt	
        , salesorderheader.onlineorderflag            
        , salesorderheader.freight
        , salesorderheader.orderdate
        , salesorderheader.duedate	
        , salesorderheader.totaldue	
        , salesorderheader.shipdate	
        , salesorderheader.creditcardapprovalcode
        , salesorderheader.revisionnumber
        , salesorderheader.accountnumber
        , salesorderheader.modifieddate	        

        from {{ ref ('stg_salesorderheader')}} as salesorderheader
        left join customer on salesorderheader.customerid = customer.customerid
        left join employee on salesorderheader.salespersonid = employee.businessentityid
        left join reason on salesorderheader.salesorderid = reason.salesorderid
        left join location on salesorderheader.shiptoaddressid = location.addressid

    )

    , salesorderdetail_with_sk as (
        select
        salesorderdetail.salesorderdetailid
        , product.product_sk as product_fk		
        , salesorderdetail.salesorderid
        , salesorderdetail.specialofferid
        , salesorderdetail.rowguid
        , salesorderdetail.orderqty	
        , salesorderdetail.unitprice
        , salesorderdetail.unitpricediscount	
        , salesorderdetail.carriertrackingnumber
        , salesorderdetail.modifieddate	

        from {{ref ('stg_salesorderdetail')}} as salesorderdetail
        left join product on salesorderdetail.productid = product.productid
    )

    , final as (
        select
        salesorderdetail_with_sk.salesorderdetailid
        , salesorderheader_with_sk.customer_fk
        , salesorderheader_with_sk.employee_fk
        , salesorderheader_with_sk.reason_fk
        , salesorderheader_with_sk.location_fk
        , salesorderheader_with_sk.purchaseordernumber
       -- , salesorderheader_with_sk.creditcardid
        , salesorderheader_with_sk.shipmethodid	
        , salesorderheader_with_sk.shiptoaddressid
        , salesorderheader_with_sk.billtoaddressid	
        , salesorderheader_with_sk.salespersonid	
        , salesorderheader_with_sk.territoryid
        , salesorderheader_with_sk.currencyrateid
        , salesorderheader_with_sk.rowguid	
        , salesorderheader_with_sk.status		
        , salesorderheader_with_sk.subtotal		
        , salesorderheader_with_sk.taxamt	
        , salesorderheader_with_sk.onlineorderflag            
        , salesorderheader_with_sk.freight
        , salesorderheader_with_sk.orderdate
        , salesorderheader_with_sk.duedate	
        , salesorderheader_with_sk.totaldue	
        , salesorderheader_with_sk.shipdate	
        , salesorderheader_with_sk.creditcardapprovalcode
        , salesorderheader_with_sk.revisionnumber
        , salesorderheader_with_sk.accountnumber
        , salesorderheader_with_sk.modifieddate	        
        , salesorderdetail_with_sk.product_fk		
        , salesorderdetail_with_sk.salesorderid
        , salesorderdetail_with_sk.specialofferid
        , salesorderdetail_with_sk.orderqty	
        , salesorderdetail_with_sk.unitprice
        , salesorderdetail_with_sk.unitpricediscount	
        , salesorderdetail_with_sk.carriertrackingnumber

        from salesorderheader_with_sk
        left join salesorderdetail_with_sk on salesorderheader_with_sk.salesorderid = salesorderdetail_with_sk.salesorderid
    )

select *
from final