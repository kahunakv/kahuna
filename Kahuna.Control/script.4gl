
BEGIN
  x = GET pp
  IF x = "201" THEN
    SET uu "super nice"
  ELSE
    SET uu "not nice"
  END
  COMMIT
  u = GET uu
  RETURN u
END

