
select 
    user,
    case 
        when coalesce(stip_bal,0) <= 0 then 'zero'
        when coalesce(stip_bal,0) <= 25 then '0 to 25'
        when coalesce(stip_bal,0) <= 100 then '25 to 100'
        when coalesce(stip_bal,0) <= 600 then '100 to 600'
        else 'more 600'
    end as segment
from dune.pyor_xyz.result_arb_arb_balances_snapshot


set fallback_value = 'zero'