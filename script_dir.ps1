# Crear carpetas del proyecto
$folders = @("data", "install", "src", "logs", "test")

foreach ($folder in $folders) {
    if (-Not (Test-Path -Path $folder)) {
        New-Item -ItemType Directory -Path $folder
        Write-Host "Carpeta creada: $folder"
    } else {
        Write-Host "La carpeta ya existe: $folder"
    }
}

# Crear archivos iniciales
$files = @("README.md", "requirements.txt", ".env")

foreach ($file in $files) {
    if (-Not (Test-Path -Path $file)) {
        New-Item -ItemType File -Path $file
        Write-Host "Archivo creado: $file"
    } else {
        Write-Host "El archivo ya existe: $file"
    }
}

Write-Host "`n✅ Estructura del proyecto creada con éxito."
