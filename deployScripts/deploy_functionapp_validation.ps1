param(
[Parameter(Mandatory=$True)]
 [string]
 $credsFilePath,

[Parameter(Mandatory = $True)]
[string]
$subscriptionName,

[Parameter(Mandatory = $True)]
[string]
$parametersFile,

[Parameter(Mandatory = $True)]
[string]
$resourceGroup
)

# sign in
Write-Host "Logging in...";
Import-AzureRmContext -Path $credsFilePath  -erroraction stop

# select subscription name
Write-Host "Selecting subscription '$subscriptionName'";
Select-AzureRmSubscription -SubscriptionName $subscriptionName -erroraction stop

# get zip file from bitbucket 
# git archive --remote=ssh://git@bitbucket.coke.com:7999/nsr/nsr-dm-file-validation.git --format=zip --output="functionapp.zip" feature/azure-function
# now manually download app content from afuse2tccc...

# Build zip file
Write-Host 'Start Building..'
Write-Host 'Nuget Restore..'
nuget restore
Write-Host 'MS build..'
cmd.exe /c "msbuild.exe" "AzureFunctions.sln" /nologo /nr:false /p:DeployOnBuild=true /p:WebPublishMethod=Package /p:platform="any cpu" /p:configuration="release" /p:CreatePackageOnPublish=true /p:OutDir="C:\zipDeploy\pkg" /t:rebuild
Write-Host 'Build Zip'
Remove-Item 'C:/zipDeploy/nsr-AF.zip'
powershell.exe -nologo -noprofile -command "& { Add-Type -A 'System.IO.Compression.FileSystem'; [IO.Compression.ZipFile]::CreateFromDirectory('C:/zipDeploy/pkg', 'C:/zipDeploy/nsr-AF.zip'); }"

# upload zip file
$paramjson = (Get-Content "$parametersFile" | Out-String | ConvertFrom-Json)
$functionAPPname = $paramjson.parameters.functionapp_name.value
#functionappPath = 'C:\Users\windows1\bamboo-agent-home\xml-data\build-dir\NSR-NSRDMDEV1-JOB1\testnsriac\functionapp'
#get functionapp credentials 
$creds = Invoke-AzureRmResourceAction -ResourceGroupName $resourceGroup -ResourceType Microsoft.Web/sites/config `
            -ResourceName $functionAPPname/publishingcredentials -Action list -ApiVersion 2015-08-01 -Force
$username = $creds.Properties.PublishingUserName
$password = $creds.Properties.PublishingPassword

#Kudo API to deploy zip file into server

$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $username, $password)))
$apiUrl = "https://$functionAPPname.scm.azurewebsites.net/api/zip/site/wwwroot"
Invoke-RestMethod -Uri $apiUrl -Headers @{Authorization = ("Basic {0}" -f $base64AuthInfo)} -Method PUT -InFile 'C:/zipDeploy/nsr-AF.zip' -ContentType "multipart/form-data" | Out-Null