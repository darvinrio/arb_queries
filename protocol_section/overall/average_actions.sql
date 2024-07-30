with 
data as (
    select 
        date,
        protocol,
        user,
        count(*) as actions
    from dune.pyor_xyz.result_arb_base_all_materialize_final
    -- where protocol = 'Camelot'
    group by 1,2,3
),
aggr as (
    select 
        date,
        protocol,
        avg(actions) as avg_actions_per_user,
        approx_percentile(actions, 0.5) as median_actions_per_user
    from data
    group by 1,2
)

select *,
    avg(avg_actions_per_user) over(partition by protocol order by date rows between 7 preceding and current row) as avg_actions_per_user_7ma
from aggr
