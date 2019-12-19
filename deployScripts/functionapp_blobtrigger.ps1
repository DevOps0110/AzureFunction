param(
 [Parameter(Mandatory = $True)]
 [string]
 $credsFilePath,

 [Parameter(Mandatory = $True)]
 [string]
 $subscriptionName, 

 [Parameter(Mandatory=$True)]
 [string]
 $configFile,

 [Parameter(Mandatory=$True)]
 [string]
 $parameterFile,

 [Parameter(Mandatory=$True)]
 [string]
 $resourceGroup
)
## IMPORTANT: make sure there is no event trigger already exists in storage,ow it will fail. 
# sign in
Write-Host "Logging in...";
Import-AzureRmContext -Path $credsFilePath  -erroraction stop

# select subscription name
Write-Host "Selecting subscription '$subscriptionName'";
Select-AzureRmSubscription -SubscriptionName $subscriptionName -erroraction stop

#Get all the functionname from zip folder
#$folder = Get-ChildItem -Directory ".\$functionAppFolderName"
#$functions = $folder.Name| Where-Object {$_ -ne 'bin'}
#Get blob storageaccount name from config
#in April 19th requirement, trigger is not set to all functions, only set to EnsureAllFiles and ProcessNonNSRFiles
$functions = "EnsureAllFiles", "ProcessNonNSRFiles"
$configjson = (Get-Content $configFile | Out-String | ConvertFrom-Json)
$configVals = $configjson.storageConfigVars
$storageName = $configVals.StorageAccountName
$storageId = (Get-AzureRmStorageAccount -ResourceGroupName $resourceGroup -AccountName $storageName).Id
# get functionapp name from parameter file
$paramjson = (Get-Content $parameterFile | Out-String | ConvertFrom-Json)
$functionappname = $paramjson.parameters.functionapp_name.value

function Get-PublishingProfileCredentialsAzure($resourceGroup, $functionappname){   
 
    $resourceType = "Microsoft.Web/sites/config"
    $resourceName = "$functionappname/publishingcredentials"
 
    $publishingCredentials = Invoke-AzureRmResourceAction -ResourceGroupName $resourceGroup -ResourceType $resourceType -ResourceName $resourceName -Action list -ApiVersion 2015-08-01 -Force
 
    return $publishingCredentials
}
function Get-KuduApiAuthorisationHeaderValueAzure($resourceGroup, $functionappname){
 
    $publishingCredentials = Get-PublishingProfileCredentialsAzure $resourceGroup $functionappname
 
    return ("Basic {0}" -f [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $publishingCredentials.Properties.PublishingUserName, $publishingCredentials.Properties.PublishingPassword))))
}

$accessToken = Get-KuduApiAuthorisationHeaderValueAzure $resourceGroup $functionappname
function Get-MasterAPIKey($kuduApiAuthorisationToken, $functionappname ){
 
    $apiUrl = "https://$functionappname.scm.azurewebsites.net/api/functions/admin/masterkey"
    
    $result = Invoke-RestMethod -Uri $apiUrl -Headers @{"Authorization"=$kuduApiAuthorisationToken;"If-Match"="*"} 
     
    return $result
}

function Get-HostAPIKeys($kuduApiAuthorisationToken, $functionappname, $masterKey,$functionNamr ){
    $apiUrl2 = "https://$functionappname.azurewebsites.net/admin/functions/$functionNamr/keys?code="
    $apiUrl=$apiUrl2 + $masterKey.masterKey.ToString()
	Write-Host "api url is " $apiUrl
    $result = Invoke-WebRequest $apiUrl
   return $result
}

$masterKey=Get-MasterAPIKey $accessToken $functionappname



foreach ($item in $functions ) {
	Write-Host "item " is $item
	Write-Host "access token is " $accessToken
	Write-Host "master key is " $masterkey
    $allkeys = Get-HostAPIKeys $accessToken $functionappname $masterkey $item
	Write-Host "all keys " $allkeys
    $Endpointurl = "https://" + $functionappname + ".azurewebsites.net/api/" + $item + "?code=" + ($allkeys.Content | ConvertFrom-Json).Keys[0].Value
	Write-Host "end point url is " $Endpointurl
    New-AzureRmEventGridSubscription -EventSubscriptionName "$item" -Endpoint "$Endpointurl" -ResourceId $storageId -EndpointType "WebHook" -IncludedEventType "Microsoft.Storage.BlobCreated" 
}





