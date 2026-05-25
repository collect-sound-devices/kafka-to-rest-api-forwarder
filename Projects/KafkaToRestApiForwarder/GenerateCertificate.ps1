param(
    [Parameter(Mandatory=$true)] [string] $CertificatePassword
)

# Parameters
$subject = "CN=Eduard Danziger (Dev Signing), O=Private, C=DE"    
$friendlyName = "Eduard Danziger Code Signing"
$validYears = 9
$pfxPath = "$PSScriptRoot\CodeSign.pfx"
$pfxPasswordPlain = $CertificatePassword
$pfxPassword = ConvertTo-SecureString -String $pfxPasswordPlain -Force -AsPlainText

# Create the self-signed certificate in the CurrentUser\My store.
$cert = New-SelfSignedCertificate `
    -CertStoreLocation "Cert:\CurrentUser\My" `
    -Subject $subject `
    -KeyExportPolicy Exportable `
    -KeySpec Signature `
    -NotAfter (Get-Date).AddYears($validYears) `
    -Type CodeSigning `
    -FriendlyName $friendlyName

Write-Host "Certificate created with thumbprint:" $cert.Thumbprint

# Export the certificate (including private key) to a PFX file.
Export-PfxCertificate -Cert $cert -FilePath $pfxPath -Password $pfxPassword

Write-Host "Certificate exported as PFX to:" $pfxPath