

with 
first_occurence_eoa as (
    select  
        s.user as "from",
        s.action as segment,
        s.protocol,
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
    where 1=1
    -- and protocol = 'Camelot'
    group by 1,2,3
),
new_users_eoa as (
    select        
        fod as date,
        segment,
        protocol,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1,2,3
),
total_addresses_eoa as (
    select 
        date,
        segment,
        protocol,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (partition by protocol, segment order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
)

select * from total_addresses_eoa

