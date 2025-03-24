BEGIN 
 SET ppa 1000 NX 
 LET x = GET ppa
 LET xn = to_int(x) + 1
 SET ppa xn
 COMMIT
END
