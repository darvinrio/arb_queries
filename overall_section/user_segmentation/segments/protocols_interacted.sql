
select 
    user,
    case 
        when protocols_interacted = 1 then 'one protocol'
        when protocols_interacted = 2 then 'two protocols'
        when protocols_interacted = 3 then 'three protocols'
        when protocols_interacted <= 5 then 'four or five protocols'
        else 'more than 5 protocols'
    end as segment
from dune.pyor_xyz.result_arb_user_summary


set fallback_value = 'zero protocol'