# This script enables you running RocksDB tests by running
# All the tests in paralell and utilizing all the cores
# For db_test the script first lists and parses the tests
# and then fires them up in parallel using async PS Job functionality
# Run the script from the enlistment
Param(
  [switch]$EnableJE = $false,  # Use je executable
  [switch]$EnableRerun = $false, # Rerun failed tests sequentially at the end
  [string]$WorkFolder = "",  # Direct tests to use that folder
  [int]$Limit = -1, # -1 means run all otherwise limit for testing purposes
  [string]$Exclude = "", # Expect a comma separated list, no spaces
  [string]$Run = "db_test",  # Run db_test|tests|testname1,testname2...
   # Number of async tasks that would run concurrently. Recommend a number below 64.
   # However, CPU utlization really depends on the storage media. Recommend ram based disk.
   # a value of 1 will run everything serially
  [int]$Concurrency = 16
)

# Folders and commands must be fullpath to run assuming
# the current folder is at the root of the git enlistment
Get-Date

# If running under Appveyor assume that root
[string]$Appveyor = $Env:APPVEYOR_BUILD_FOLDER
if($Appveyor -ne "") {
    $RootFolder = $Appveyor
} else {
    $RootFolder = $PSScriptRoot -replace '\\build_tools', ''
}

$LogFolder = -Join($RootFolder, "\db_logs\")
$BinariesFolder = -Join($RootFolder, "\build\Debug\")

if($WorkFolder -eq "") {

    # If TEST_TMPDIR is set use it    
    [string]$var = $Env:TEST_TMPDIR
    if($var -eq "") {
        $WorkFolder = -Join($RootFolder, "\db_tests\")
        $Env:TEST_TMPDIR = $WorkFolder
    } else {
        $WorkFolder = $var
    }
} else {
# Override from a command line
  $Env:TEST_TMPDIR = $WorkFolder
}

# Use JEMALLOC executables
if($EnableJE) {
    $db_test = -Join ($BinariesFolder, "db_test_je.exe")
} else {
    $db_test = -Join ($BinariesFolder, "db_test.exe")
}

Write-Output "Root: $RootFolder, WorkFolder: $WorkFolder"
Write-Output "Binaries: $BinariesFolder db_test: $db_test"

#Exclusions that we do not want to run
$ExcludeTests = New-Object System.Collections.Generic.HashSet[string]


if($Exclude -ne "") {
    Write-Host "Exclude: $Exclude"
    $l = $Exclude -split ' '
    ForEach($t in $l) { $ExcludeTests.Add($t) | Out-Null }
}

# Create test directories in the current folder
md -Path $WorkFolder -ErrorAction Ignore | Out-Null
md -Path $LogFolder -ErrorAction Ignore | Out-Null

# Extract the names of its tests by running db_test with --gtest_list_tests.
# This filter removes the "#"-introduced comments, and expands to
# fully-qualified names by changing input like this:
#
#   DBTest.
#     Empty
#     WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.
#     MultiThreaded/0  # GetParam() = 0
#     MultiThreaded/1  # GetParam() = 1
#
# into this:
#
#   DBTest.Empty
#   DBTest.WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/0
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/1
# Output into the parameter in a form TestName -> Log File Name
function Normalize-DbTests($HashTable) {

    $Tests = @()
# Run db_test to get a list of tests and store it into $a array
    &$db_test --gtest_list_tests | tee -Variable Tests | Out-Null

    # Current group
    $Group=""

    ForEach( $l in $Tests) {
      # Trailing dot is a test group
      if( $l -match "\.$") {
        $Group = $l
      }  else {
        # Otherwise it is a test name, remove leading space
        $test = $l -replace '^\s+',''
        # remove trailing comment if any and create a log name
        $test = $test -replace '\s+\#.*',''
        $test = "$Group$test"

        if($ExcludeTests.Contains($test)) {
            continue
        }

        $test_log = $test -replace '[\./]','_'
        $test_log += ".log"
        $log_path = -join ($LogFolder, $test_log)

        # Add to a hashtable
        $HashTable.Add($test, $log_path);
      }
    }
}

# The function removes trailing .exe siffix if any,
# creates a name for the log file
function MakeAndAdd([string]$token, $HashTable) {
    $test_name = $token -replace '.exe$', ''
    $log_name =  -join ($test_name, ".log")
    $log_path = -join ($LogFolder, $log_name)
    if(!$ExcludeTests.Contains($test_name)) {
        $HashTable.Add($test_name, $log_path)
    } else {
        Write-Warning "Test $test_name is excluded"
    }
}

# The function scans build\Debug folder to discover
# Test executables. It then populates a table with
# Test executable name -> Log file
function Discover-TestBinaries([string]$Pattern, $HashTable) {

    $Exclusions = @("db_test*", "db_sanity_test*")

    $p = -join ($BinariesFolder, $pattern)

    Write-Host "Path: $p"

    dir -Path $p -Exclude $Exclusions | ForEach-Object {
       MakeAndAdd -token ($_.Name) -HashTable $HashTable
    }
}

$TestsToRun = [ordered]@{}

if($Run -ceq "db_test") {
    Normalize-DbTests -HashTable $TestsToRun
} elseif($Run -ceq "tests") {
    if($EnableJE) {
        $pattern = "*_test_je.exe"
    } else {
        $pattern = "*_test.exe"
    }
    Discover-TestBinaries -Pattern $pattern -HashTable $TestsToRun
} else {

    $test_list = $Run -split ' '

    ForEach($t in $test_list) {
       MakeAndAdd -token $t -HashTable $TestsToRun
    }
}

$NumTestsToStart = $TestsToRun.Count
if($Limit -ge 0 -and $NumTestsToStart -gt $Limit) {
    $NumTestsToStart = $Limit
}

Write-Host "Attempting to start: $NumTestsToStart tests"

# Invoke a test with a filter and redirect all output
$InvokeTestCase = {
    param($exe, $test, $log);
    &$exe --gtest_filter=$test > $log 2>&1
}

# Invoke all tests and redirect output
$InvokeTestAsync = {
    param($exe, $log)
    &$exe > $log 2>&1
}

# Hash that contains tests to rerun if any failed
# Those tests will be rerun sequentially
$Rerun = [ordered]@{}
# Test limiting factor here
$count = 0
# Overall status
[bool]$success = $true;

function RunJobs($TestToLog, [int]$ConcurrencyVal, [bool]$AddForRerun)
{
    # Array to wait for any of the running jobs
    $jobs = @()
    # Hash JobToLog
    $JobToLog = @{}

    # Wait for all to finish and get the results
    while(($JobToLog.Count -gt 0) -or
          ($TestToLog.Count -gt 0)) {

        # Make sure we have maximum concurrent jobs running if anything
        # and the $Limit either not set or allows to proceed
        while(($JobToLog.Count -lt $ConcurrencyVal) -and
              (($TestToLog.Count -gt 0) -and
              (($Limit -lt 0) -or ($count -lt $Limit)))) {


            # We only need the first key
            foreach($key in $TestToLog.keys) {
                $k = $key
                break
            }

            Write-Host "Starting $k"
            $log_path = ($TestToLog.$k)

            if($Run -ceq "db_test") {
              $job = Start-Job -Name $k -ScriptBlock $InvokeTestCase -ArgumentList @($db_test,$k,$log_path)
            } else {
              [string]$Exe =  -Join ($BinariesFolder, $k)
               $job = Start-Job -Name $k -ScriptBlock $InvokeTestAsync -ArgumentList @($exe,$log_path)
            }

            $JobToLog.Add($job, $log_path)
            $TestToLog.Remove($k)

            ++$count
        }

        if($JobToLog.Count -lt 1) {
          break
        }

        $jobs = @()
        foreach($k in $JobToLog.Keys) { $jobs += $k }

        $completed = Wait-Job -Job $jobs -Any
        $log = $JobToLog[$completed]
        $JobToLog.Remove($completed)

        $message = -join @($completed.Name, " State: ", ($completed.State))

        $log_content = @(Get-Content $log)

        if($completed.State -ne "Completed") {
            $success = $false
            Write-Warning $message
            $log_content | Write-Warning
        } else {
            # Scan the log. If we find PASSED and no occurence of FAILED
            # then it is a success
            [bool]$pass_found = $false
            ForEach($l in $log_content) {

                if(($l -match "^\[\s+FAILED") -or
                   ($l -match "Assertion failed:")) {
                    $pass_found = $false
                    break
                }

                if(($l -match "^\[\s+PASSED") -or
                   ($l -match " : PASSED$") -or
                    ($l -match "^PASS$") -or   # Special c_test case
                    ($l -match "Passed all tests!") ) {
                    $pass_found = $true
                }
            }

            if(!$pass_found) {
                $success = $false;
                Write-Warning $message
                $log_content | Write-Warning
                if($AddForRerun) {
                    $Rerun.Add($completed.Name, $log)
                }
            } else {
                Write-Host $message
            }
        }

        # Remove cached job info from the system
        # Should be no output
        Receive-Job -Job $completed | Out-Null
    }
}

RunJobs -TestToLog $TestsToRun -ConcurrencyVal $Concurrency -AddForRerun $EnableRerun

if($Rerun.Count -gt 0) {
    Write-Host "Rerunning " ($Rerun.Count) " tests sequentially"
    $success = $true
    $count = 0
    RunJobs -TestToLog $Rerun -ConcurrencyVal 1 -AddForRerun $false
}

Get-Date


if(!$success) {
# This does not succeed killing off jobs quick
# So we simply exit
#    Remove-Job -Job $jobs -Force
# indicate failure using this exit code
    exit 12345
 }

 