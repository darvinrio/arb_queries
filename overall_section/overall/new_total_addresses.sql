

with first_occurence_eoa as (
    select  
        s.user as "from",
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
    where 1=1
    group by 1
),
new_users_eoa as (
    select        
        fod as date,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1
),
total_addresses_eoa as (
    select 
        date,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
)
select * from total_addresses_eoa

