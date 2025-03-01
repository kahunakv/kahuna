
Generate a self-signed certificate (no password):

```sh
openssl genrsa -out development-private.key 2048
openssl req -new -nodes -config san.cnf -keyout development-private.key -out development-certificate.csr
openssl x509 -req -in development-certificate.csr -signkey development-private.key -out
development-certificate.crt -days 365 -extensions req_ext -extfile san.cnf
openssl pkcs12 -export -out development-certificate.pfx -inkey development-private.key -in development-certificate.crt -passout pass:
```

Trust certificate on mac:

```sh
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain development-certificate.crt
```
