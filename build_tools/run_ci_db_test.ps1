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

# Folders and commands must be fullpath to run assuming
# the current folder is at the root of the git enlistment
Get-Date
# Limit the number of tests to start for debugging purposes
$limit = -1

$RootFolder = $pwd -replace '\\build_tools', ''
$LogFolder = -Join($RootFolder, "\db_logs\")
$TmpFolder = -Join($RootFolder, "\db_tests\")
$Env:TEST_TMPDIR = $TmpFolder
$global:db_test = -Join ($RootFolder, "\build\Debug\db_test.exe")

#Exclusions that we do not want to run
$ExcludeTests = @{
<#
"DBTest.HugeNumberOfLevels" = ""
"DBTest.SparseMerge"  = ""
"DBTest.RateLimitingTest"  = ""
"DBTest.kAbsoluteConsistency"  = ""
"DBTest.GroupCommitTest" = ""
"DBTest.FileCreationRandomFailure"  = ""
"DBTest.kTolerateCorruptedTailRecords"  = ""
"DBTest.kSkipAnyCorruptedRecords"  = ""
"DBTest.kPointInTimeRecovery"  = ""
"DBTest.Randomized"  = ""
#>
}

# Create test directories in the current folder
md -Path $TmpFolder -ErrorAction Ignore
md -Path $LogFolder -ErrorAction Ignore

function Normalize-Tests([System.Array]$Tests, $HashTable) {
    # Current group
    $Group=""

    ForEach( $l in $tests) {
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

        $test_log = $test -replace '[./]','_'
        $test_log += ".log"

        # Add to a hashtable
        $HashTable.Add($test, $test_log);
      }
    }
}

# Run db_test to get a list of tests and store it into $a array
&$db_test --gtest_list_tests | tee -Variable TestList | Out-Null

# Parse the tests and store along with the log name into a hash
$TestToLog = [ordered]@{}

Normalize-Tests -Tests $TestList -HashTable $TestToLog

Write-Host "Attempting to start: " ($TestToLog.Count) " tests"

# Start jobs async each running a separate test
$AsyncScript = {
    param($exe, $test, $log);
    &$exe --gtest_filter=$test > $log 2>&1
}

$jobs = @()
$JobToLog = @{}
# Test limiting factor here
$count = 0

ForEach($k in $TestToLog.keys) {

    Write-Host "Starting $k"
    $log_path = -join ($LogFolder, ($TestToLog.$k))
    $job = Start-Job -Name $k -ScriptBlock $AsyncScript -ArgumentList @($db_test,$k,$log_path)
    $JobToLog.Add($job, $log_path)

    # Limiting trial runs
    if(($limit -gt 0) -and (++$count -ge $limit)) {
         break
    }
}


$success = 1;

# Wait for all to finish and get the results
while($JobToLog.Count -gt 0) {

    $jobs = @()
    foreach($k in $JobToLog.Keys) { $jobs += $k }

<#
    if(!$success) {
        break
    }
#>

    $completed = Wait-Job -Job $jobs -Any
    $log = $JobToLog[$completed]
    $JobToLog.Remove($completed)

    $message = -join @($completed.Name, " State: ", ($completed.State))

    $log_content = @(Get-Content $log)

    if($completed.State -ne "Completed") {
        $success = 0
        Write-Warning $message
        $log_content | Write-Warning
    } else {
        # Scan the log. If we find PASSED and no occurence of FAILED
        # then it is a success
        $pass_found = 0
        ForEach($l in $log_content) {

            if($l -match "^\[\s+FAILED") {
                $pass_found = 0
                break
            }

            if($l -match "^\[\s+PASSED") {
                $pass_found = 1
            }
        }

        if(!$pass_found) {
            $success = 0;
            Write-Warning $message
            $log_content | Write-Warning
        } else {
            Write-Host $message
        }
    }

    # Remove cached job info from the system
    # Should be no output
    Receive-Job -Job $completed | Out-Null
}

Get-Date

if(!$success) {
# This does not succeed killing off jobs quick
# So we simply exit
#    Remove-Job -Job $jobs -Force
# indicate failure using this exit code
    exit 12345
 }

 