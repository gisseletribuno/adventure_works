with
    selected1 as (
        select
            businessentityid	
            , territoryid
            , salesquota
            , saleslastyear
            , salesytd
            , commissionpct
            , bonus
            , modifieddate		
            , rowguid			

        from {{ref('stg_salesperson')}}
    )
,   selected2 as (
        select
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

        from {{ref('stg_employee')}}            
    )
,  selected3 as (
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

,   final1 as (
        select
            selected1.businessentityid
            , selected2.nationalidnumber
            , selected2.loginid	
            , selected2.jobtitle
            , selected2.birthdate	        
            , selected2.gender	
            , selected2.maritalstatus
            , selected2.hiredate	
            , selected3.title
            , selected3.lastname
            , selected3.firstname
            , selected3.middlename
            , selected3.emailpromotion
            , selected3.persontype
            , selected3.namestyle
            , selected3.suffix
            , selected2.salariedflag	
            , selected2.sickleavehours	
            , selected2.organizationnode		
            , selected2.vacationhours
            , selected2.currentflag
            , selected2.modifieddate	
            , selected2.rowguid	      
            , selected1.territoryid
            , selected1.salesquota
            , selected1.saleslastyear
            , selected1.salesytd
            , selected1.commissionpct
            , selected1.bonus
            from selected1
            left join selected2 on selected1.businessentityid = selected2.businessentityid	
            left join selected3 on selected2.businessentityid = selected3.businessentityid	        
)

,   final2 as (
        select
        row_number () over (order by businessentityid) as employee_sk
        , *
    from final1
    )

select *
from final2