Step 1 — Create the certificate

      $cert = New-SelfSignedCertificate `
          -DnsName "localhost" `
          -CertStoreLocation "Cert:\CurrentUser\My" `
          -KeyExportPolicy Exportable

📌 Step 2 — Export cert.pem

    $certBytes = $cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Cert)
    $base64 = [System.Convert]::ToBase64String($certBytes, "InsertLineBreaks")

    @"
    -----BEGIN CERTIFICATE-----
    $base64
    -----END CERTIFICATE-----

    "@ | Set-Content -Path ".\cert.pem" -Encoding ascii

📌 Step 3 — Export key.pem (PKCS#8 PRIVATE KEY)

This part uses .NET's RSA APIs to export the private key.

    $privateKey = $cert.GetRSAPrivateKey()
    $pkcs8Bytes = $privateKey.ExportPkcs8PrivateKey()
    $base64Key = [Convert]::ToBase64String($pkcs8Bytes, "InsertLineBreaks")

    @"
    -----BEGIN PRIVATE KEY-----
    $base64Key
    -----END PRIVATE KEY-----
    "@ | Set-Content -Path ".\key.pem" -Encoding ascii
