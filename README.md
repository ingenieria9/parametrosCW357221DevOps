# parametrosCW357221DevOps

Proyecto que despliega infraestructura serverless para el proyecto medición de parametros

lambdas, api gateway, buckets, etc

En infra se encuentra el codigo en CDK para el despliegue
En src el codigo fuente de las lambdas

Para probar localmente las lambdas antes de hacer un push al main se requiere: 


aws cli, sam, docker instalados 


en la carpeta localTest tener .env con las variables de entorno de la lambda a probar 
en stacks debe estar .env con las variables globales necesarias (las mismas que estan en github secrets) para hacer el cdk synth y asi generar las templates

tambien se requiere descomprimir los zip de las layers y nombrar las carpetas igual que el zip

pasos para probar por ejemplo la lambda updateCron que hace parte del stack  CW357221ParametrosDevOps-ArcGISIntStack


desde  /infra 

cargar variables de ./stacks/.env en la terminal
```powershell
Get-Content ./stacks/.env | foreach {
    if ($_ -match '^(.*)=(.*)$') { set-item env:$($matches[1]) $($matches[2]) }
}
 ```
generar templates locales:

 ```powershell
 .\generate-local-templates.ps1  
 ```

cargar variables de entorno para lambda (localTest/.env)
 ```powershell
$envVars = Get-Content localTest/.env | ForEach-Object {    if ($_ -match '^(.*)=(.*)$') { set-item env:$($matches[1]) $($matches[2]) }}
 ```

ejecutar con event inline: 

 ```powershell
Write-Output '{"Records": [{"id": 123, "name": "test"}]}' | sam local invoke CW357221ParametrosDevOps-updateCron `  --template ./cdk.out/local/CW357221ParametrosDevOps-ArcGISIntStack.template.json
 ```
 
ejecutar con event json file 

 ```powershell
sam local invoke CW357221ParametrosDevOps-updateCron `  --template ./cdk.out/local/CW357221ParametrosDevOps-ArcGISIntStack.template.json --event localTest/evento_updateCron.json
 ```

