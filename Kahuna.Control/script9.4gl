let rate_limit = get @rate_limit_param
let last_refill = get @last_refill_param

let tokens = to_int(rate_limit)
let last_refill = to_int(last_refill)

let current_time = current_time()
let elapsed = current_time - last_refill
let refill = floor(elapsed / @refill_interval_param)

if tokens <= 0 then
  return 0
end

set @rate_limit_param tokens - 1
set @last_refill_param current_time
return 1
