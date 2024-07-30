with 
data as (
    select 
        date,
        protocol,
        s.user,
        s.action as segment,
        count(*) as actions
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
    -- where protocol = 'Camelot'
    group by 1,2,3,4
),
aggr as (
    select 
        date,
        protocol,
        segment,
        avg(actions) as avg_actions_per_user,
        approx_percentile(actions, 0.5) as median_actions_per_user
    from data
    group by 1,2,3
)

select *,
    avg(avg_actions_per_user) over(partition by protocol,segment order by date rows between 7 preceding and current row) as avg_actions_per_user_7ma
from aggr
