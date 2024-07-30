
select 
    user,
    case 
        when coalesce(stip_bal,0) <= 0 then 'zero'
        when coalesce(stip_bal,0) <= 20 then '0 to 20'
        when coalesce(stip_bal,0) <= 70 then '20 to 70'
        when coalesce(stip_bal,0) <= 500 then '70 to 500'
        else 'more 50'
    end as segment
from dune.pyor_xyz.result_arb_arb_balances_snapshot


set fallback_value = 'zero'