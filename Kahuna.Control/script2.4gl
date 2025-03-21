
let s = GET status
if s = "active" then
 set status "updated"
else
 set status "active"
end
