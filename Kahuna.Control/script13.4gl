set 'doctors/alice' true nx
set 'doctors/bob' true nx

let oncall = get by prefix 'doctors'
if count(oncall) = 2 then
   set 'doctors/alice' false ex 10000
end
