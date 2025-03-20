begin
          balance_user1_value = get balance_user1
          balance_user2_value = get balance_user2
       
          if balance_user1_val 0 then
             set balance_user1 balance_user1_value - 50
             set balance_user2 balance_user1_value + 50
             commit
          end
        end
