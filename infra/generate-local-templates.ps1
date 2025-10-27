# ========================================
# generate-local-templates.ps1
# ========================================
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- Configuración ---
$OutDir = "cdk.out"
$LocalDir = Join-Path $OutDir "local"

# Mapeo de Layers remotas a locales
$layersMap = @{
    "CW357221ParametrosDevOps-LayersStack:ExportsOutputRefCW357221ParametrosDevOpsOpenpyxlLayerD45504B2401284F7" = "../../layers/openpyxl"
    "CW357221ParametrosDevOps-LayersStack:ExportsOutputRefCW357221ParametrosDevOpsDocxtplLayer1DBD86B9D558AC25"   = "../../layers/docxtpl"
    "CW357221ParametrosDevOps-LayersStack:ExportsOutputRefCW357221ParametrosDevOpsrequestsLayer4A7F77A5353CDF8A" = "../../layers/requests"
    "CW357221ParametrosDevOps-LayersStack:ExportsOutputRefCW357221ParametrosDevOpsgoogleLayer2681B514AEF58F30" = "../../layers/google"
}

# --- Paso 1: Ejecutar CDK Synth ---
Write-Host "Running 'cdk synth'..."
cdk synth | Out-Null

# --- Paso 2: Preparar carpeta local ---
if (-Not (Test-Path $LocalDir)) {
    New-Item -ItemType Directory -Path $LocalDir | Out-Null
}
Write-Host "Copying templates to $LocalDir..."
Copy-Item "$OutDir\*.template.json" $LocalDir -Force

# --- Paso 3: Procesar cada template ---
Write-Host "Replacing remote Layers with local LayerVersion resources..."

Get-ChildItem "$LocalDir" -Filter "*.template.json" | ForEach-Object {
    $file = $_.FullName
    Write-Host "  Processing $($_.Name)..."

    try {
        $jsonText = Get-Content -Raw -Path $file | ConvertFrom-Json
    } catch {
        Write-Warning "  ⚠️ Error reading JSON in file $($file): $_"
        return
    }

    if ($null -eq $jsonText.Resources) {
        Write-Host "    (No Resources found, skipping)"
        return
    }

    # --- Ajustar paths locales de assets ---
    foreach ($resKey in $jsonText.Resources.PSObject.Properties.Name) {
        $resource = $jsonText.Resources.$resKey

        if ($null -eq $resource) { continue }

        if ($resource.PSObject.Properties.Name -contains "Metadata") {
            $metadata = $resource.Metadata
            if ($metadata -and ($metadata.PSObject.Properties.Name -contains "aws:asset:path")) {
                $assetPath = $metadata."aws:asset:path"
                if ($assetPath -and -not ($assetPath -like "./*")) {
                    $metadata."aws:asset:path" = "../$assetPath"
                    Write-Host "    Adjusted asset path → ../$assetPath"
                }
            }
        }
    }

    # --- Reemplazar layers ---
    foreach ($resKey in $jsonText.Resources.PSObject.Properties.Name) {
        $resource = $jsonText.Resources.$resKey

        if ($null -eq $resource.Properties) { continue }
        if (-not ($resource.Properties.PSObject.Properties.Name -contains "Layers")) { continue }

        $layers = $resource.Properties.Layers
        if (-not $layers) { continue }

        $newLayers = @()

        foreach ($layer in $layers) {
            if ($layer -and ($layer.PSObject.Properties.Name -contains "Fn::ImportValue")) {
                $importValue = $layer."Fn::ImportValue"

                if ($importValue -and $layersMap.ContainsKey($importValue)) {
                    $layerPath = $layersMap[$importValue]
                    $layerName = Split-Path $layerPath -Leaf
                    $layerLogicalId = "LocalLayer$layerName"

                    Write-Host ("    Creating local Layer resource for {0} at {1}" -f $importValue, $layerPath)

                    # Crear recurso Layer local si no existe
                    if (-not $jsonText.Resources.PSObject.Properties.Name.Contains($layerLogicalId)) {
                        $layerResource = @{
                            Type = "AWS::Serverless::LayerVersion"
                            Properties = @{
                                LayerName = "local-$layerName"
                                ContentUri = "./$layerPath"
                                CompatibleRuntimes = @("python3.13", "python3.12", "python3.11", "python3.10", "python3.9", "python3.8")
                            }
                        }
                        $jsonText.Resources | Add-Member -MemberType NoteProperty -Name $layerLogicalId -Value $layerResource
                    }

                    # Referenciar el nuevo Layer en la función
                    $newLayers += @{ "Ref" = $layerLogicalId }
                } else {
                    $newLayers += $layer
                }
            } else {
                $newLayers += $layer
            }
        }

        $resource.Properties.Layers = $newLayers
    }

    # --- Guardar cambios ---
    try {
        $jsonText | ConvertTo-Json -Depth 50 | Out-File -FilePath $file -Encoding utf8
    } catch {
        Write-Warning "  ⚠️ Error writing JSON for file $($file): $_"
    }
}

Write-Host "`n✅ Done! Local templates generated in $LocalDir"
