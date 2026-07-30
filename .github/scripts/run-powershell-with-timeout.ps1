# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

param(
  [Parameter(Mandatory = $true)]
  [ValidateRange(1, 2147483)]
  [int]$TimeoutSeconds,

  [Parameter(Mandatory = $true)]
  [string]$ScriptPath
)

$ErrorActionPreference = "Stop"

$startInfo = [System.Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = (Get-Command pwsh -ErrorAction Stop).Source
$startInfo.UseShellExecute = $false
$startInfo.ArgumentList.Add("-NoProfile")
$startInfo.ArgumentList.Add("-File")
$startInfo.ArgumentList.Add((Resolve-Path $ScriptPath).Path)

$process = [System.Diagnostics.Process]::new()
$process.StartInfo = $startInfo
if (!$process.Start()) {
  Write-Error "Failed to start $ScriptPath"
  Exit 1
}

if (!$process.WaitForExit($TimeoutSeconds * 1000)) {
  taskkill /PID $($process.Id) /T /F | Out-Host
  $process.WaitForExit()
  Write-Output "::error::Command timed out after $TimeoutSeconds seconds."
  Exit 1
}

Exit $process.ExitCode
