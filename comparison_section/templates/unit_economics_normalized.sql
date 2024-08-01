with 
segmentation as (
    select 
        protocol,
        segment
    from query_{{segment_query}}
),
fee_normalization_curve as (
    select *
    from dune.pyor_xyz.result_arb_gwei_normalization_curve
),
protocol_summary as (
    select 
        s.date,
        coalesce(u.segment, {{fallback_value}}) as segment,
        sum(s.txs) as txs,
        sum(s.calls) as calls,
        sum(s.giga_gas) as giga_gas,
        sum(s.fee_paid) as fee_paid,
        sum(s.l1_fee_paid) as l1_fee_paid,
        sum(s.seq_fee_paid) as seq_fee_paid,
        sum(s.seq_fee_paid)/sum(n.avg_gas_price_gwei) as norm_seq_fee_paid
    from dune.pyor_xyz.result_arb_protocol_summary_daily s
        join fee_normalization_curve n
            on s.date = n.date
        left join segmentation u 
            on s.protocol = u.protocol
    group by 1,2
),
protocol_usage as (
    select * from dune.pyor_xyz.result_arb_base_all_materialize_final
    -- where protocol = '{{protocol}}'
),
usage_aggr as (
    select 
        date,
        coalesce(u.segment, {{fallback_value}}) as segment,
        count(distinct user) as users,
        count(*) as actions
    from protocol_usage s
        left join segmentation u 
            on s.protocol = u.protocol
    group by 1,2
)

select 
    s.date,
    s.segment,
    s.txs,
    s.calls,
    s.giga_gas,
    s.fee_paid,
    s.l1_fee_paid,
    s.seq_fee_paid,
    s.norm_seq_fee_paid,
    u.users,
    u.actions,
    1e9*s.seq_fee_paid/s.txs as fee_paid_per_tx,
    1e9*s.seq_fee_paid/u.users as fee_paid_per_user,
    s.txs/u.users as txs_per_user,
    
    1e9*approx_percentile(s.seq_fee_paid/s.txs, 0.5) over(partition by s.segment order by s.date rows between 20 preceding and current row) as fee_paid_per_tx_med,
    1e9*approx_percentile(s.seq_fee_paid/u.users, 0.5) over(partition by s.segment order by s.date rows between 20 preceding and current row) as fee_paid_per_user_med,
    approx_percentile(s.txs/u.users, 0.5) over(partition by s.segment order by s.date rows between 20 preceding and current row) as txs_per_user_med,


    1e9*s.norm_seq_fee_paid/s.txs as norm_fee_paid_per_tx,
    1e9*s.norm_seq_fee_paid/u.users as norm_fee_paid_per_user,
    1e9*approx_percentile(s.norm_seq_fee_paid/s.txs, 0.5) over(partition by s.segment order by s.date rows between 20 preceding and current row) as norm_fee_paid_per_tx_med,
    1e9*approx_percentile(s.norm_seq_fee_paid/u.users, 0.5) over(partition by s.segment order by s.date rows between 20 preceding and current row) as norm_fee_paid_per_user_med
from protocol_summary s
    left join usage_aggr u   
        on s.date = u.date
        and s.segment = u.segment
-- limit 100