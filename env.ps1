# Load .env into the current PowerShell session:  . .\env.ps1
#
# The leading dot matters. `.\env.ps1` runs this in a child process that exits
# immediately, taking the variables with it. Dot-sourcing runs it in the
# current session, which is the only place the variables are useful.
#
# This happens at the shell level rather than inside Python because dbt and
# Dagster read these too; profiles.yml and staging.yml both call env_var().
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$') {
        $name, $value = $matches[1], $matches[2].Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($name, $value)
        Write-Host "  set $name"
    }
}
