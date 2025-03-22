begin
 eset balance_user1 1000 nx
 eset balance_user2 1000 nx

 let balance_user1_value = eget balance_user1
 let balance_user2_value = eget balance_user2
 
 let balance_user1_num = to_int(balance_user1_value)
 let balance_user2_num = to_int(balance_user2_value)
     
 if balance_user1_num > 0 then
    eset balance_user1 balance_user1_num - 50
    eset balance_user2 balance_user2_num + 50
    commit
 end
end
