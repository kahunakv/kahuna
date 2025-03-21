begin
 let balance_user1_value = get balance_user1
 let balance_user2_value = get balance_user2
 
 let balance_user1_num = to_int(balance_user1_value)
 let balance_user2_num = to_int(balance_user2_value)
     
 if balance_user1_num > 0 then
    set balance_user1 balance_user1_num - 50
    set balance_user2 balance_user2_num + 50
    commit
 end
end
