[CmdletBinding()]
Param(
  [string]$DatabaseUpgradeScriptsPath = "",
  [string]$MySqlDllFullPath = "",   
  [switch]$DropDatabase = $false,
  [switch]$CreateDatabase = $true,
  [string]$versionDbTableName = "VersionForDb",
  [string]$DatabaseServerName = "dk-mysql-svr-dr.mysql.database.azure.com",
  [string]$DatabaseName = "testdb",
  [string]$DatabasePort = "3306",
  [string]$DatabaseLogin = "",
  [string]$DatabasePassword = ""
)

Write-Output "DatabaseUpgradeScriptsPath: $DatabaseUpgradeScriptsPath"
Write-Output "MySqlDllFullPath: $MySqlDllFullPath"
Write-Output "DropDatabase: $DropDatabase"
Write-Output "CreateDatabase: $CreateDatabase"
Write-Output "versionDbTableName: $versionDbTableName"
Write-Output "DatabaseServerName: $DatabaseServerName"
Write-Output "DatabaseName: $DatabaseName"
Write-Output "DatabasePort: $DatabasePort"
Write-Output "DatabaseLogin: $DatabaseLogin"
Write-Output "DatabasePassword: $DatabasePassword"

[void][system.reflection.Assembly]::LoadFrom("$MySqlDllFullPath")

$ConnectionStringWithDb = "server=" + $DatabaseServerName + ";port=$DatabasePort;uid=" + $DatabaseLogin + ";pwd=" + $DatabasePassword + ";database="+$DatabaseName
$ConnectionStrNoDb = "server=" + $DatabaseServerName + ";port=$DatabasePort;uid=" + $DatabaseLogin + ";pwd=" + $DatabasePassword

function DropAndCreateDb
{
    Try
    {
        $ConnectionSys = New-Object MySql.Data.MySqlClient.MySqlConnection($ConnectionStrNoDb)
        $ConnectionSys.Open()

        if ($DropDatabase)
        {
            $dropCommand = New-Object MySql.Data.MySqlClient.MySqlCommand("DROP DATABASE IF EXISTS $DatabaseName", $ConnectionSys)
            Write-Host "Dropping DB: $DatabaseName ..."
            $dropCommand.ExecuteNonQuery()
            Write-Host "Dropping DB: $DatabaseName complete"
        }

        if($CreateDatabase) {
            $GetDbCommand = New-Object MySql.Data.MySqlClient.MySqlCommand("SHOW DATABASES", $ConnectionSys)
            $DataAdapterDb = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($GetDbCommand)
            $DataSetDb = New-Object System.Data.DataSet
            $dataAdapterDb.Fill($DataSetDb, "data")
            $DataSetDb.Tables[0]

            $DbDoesExist=$false
            for($i=0;$i -lt $DataSetDb.Tables[0].Rows.Count; $i++) {
                if($($DataSetDb.Tables[0].Rows[$i][0]) -eq "$DatabaseName")
                {
                    $DbDoesExist=$true
                }
            }
        
            if($DbDoesExist -eq $false)
            {
                $createCommand = New-Object MySql.Data.MySqlClient.MySqlCommand("CREATE DATABASE $DatabaseName;", $ConnectionSys)
                Write-Host "Creating DB: $DatabaseName ..."
                $createCommand.ExecuteNonQuery()
                Write-Host "Creating DB: $DatabaseName complete"

                #Now lets create the version table
                Write-Host "Creating Version Table: $versionDbTableName ..."
                RunNonQuery "CREATE TABLE $versionDbTableName (id serial PRIMARY KEY, name VARCHAR(200));" "Creating VersionTable"
                Write-Host "Creating Version Table: $versionDbTableName complete"
            }
            else
            {
                Write-Host "DB: $DatabaseName already created"
            }
        }

        Write-Host "DoDataBaseWork Finished"
    }
    Catch [System.Exception]
    {
            Write-Error $_.Exception.Message
            Exit 1
    }
    Finally {
        $ConnectionSys.Close()
    }

    return $true
}

function RunNonQuery
{
    param([string]$SqlCommandText, [string]$ScriptName)

    Try {
        $Connection = New-Object MySql.Data.MySqlClient.MySqlConnection($ConnectionStringWithDb)
        $Connection.Open()

        $createCommand = New-Object MySql.Data.MySqlClient.MySqlCommand($SqlCommandText, $Connection)
        $createCommand.ExecuteNonQuery()
        Write-Host "Script: $ScriptName complete"

    }
    Catch [System.Exception]
    {
        Write-Error $_.Exception.Message
        Exit 1
    }
    Finally {
        $Connection.Close()
    }
}

function RunExecuteScalar
{
    param([string]$SqlcommandText)

    Try {
        $Connection = New-Object MySql.Data.MySqlClient.MySqlConnection($ConnectionStringWithDb)
        $Connection.Open()

        $createCommand = New-Object MySql.Data.MySqlClient.MySqlCommand($SqlcommandText, $Connection)
        $scalarValue = $createCommand.ExecuteScalar()
        Write-Host "Script complete with scalar value: $scalarValue"
        return $scalarValue

    }
    Catch [System.Exception]
    {
        Write-Error $_.Exception.Message
        Exit 1
    }
    Finally {
        $Connection.Close()
    }
}

function VerifyVersionTable
{
    Try {
        $sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + $DatabaseName + "' AND table_name = '" + $versionDbTableName + "';"
        $tableName = RunExecuteScalar $sql 

        if(!$tableName)
        {
            Write-Error "Version Table not found - database in unknown state"
            Exit 1
        }

        Write-Host "Version table found: $tableName"
    }
    Catch {
            Write-Error $_.Exception.Message
            Exit 1
        }
    Finally {
    }
}

function GetVersionTableRows
{
    Try {
        $Connection = New-Object MySql.Data.MySqlClient.MySqlConnection($ConnectionStringWithDb)
        $Connection.Open()
      
        $Query = "Select name From $versionDbTableName Order By id;"
        $Command = New-Object MySql.Data.MySqlClient.MySqlCommand($Query, $Connection)
        $DataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($Command)
        $DataSet = New-Object System.Data.DataSet
        $dataAdapter.Fill($dataSet, "data")
        
        if($DataSet.Tables[0].Rows.Count -gt 0)
        {
            Write-Host "The version table has rows!"
            $myArray = @()

            for($i=0;$i -lt $DataSet.Tables[0].Rows.Count; $i++) {
                $myArray += $($DataSet.Tables[0].Rows[$i][0])
            }

            [array]::Sort($myArray)
            return $myArray
        }
        else
        {
            return $null
        }
    }
    Catch {
        Write-Error $_.Exception.Message
        Exit 1
    }
    Finally {
        $Connection.Close()
    }
}

function GetScriptFiles
{
    Try {
        $filesToUse = @()

        #Get database script files From folder location pased in the script
        $AllScripts = (Get-ChildItem -Path $DatabaseUpgradeScriptsPath -Filter "*.sql").Name
        
        #Make sure we have scripts, if not get out of here and return null
        if($AllScripts)
        {
            foreach($item in $AllScripts){
                Write-Host "Files: $item"
                $filesToUse += $item
            }

            #we have the scripts now lets sort to make sure we are good
            [array]::Sort($filesToUse)
            return $filesToUse
        }
        else
        {
            Write-Host "NO SCRIPTS"
            return $null
        }
    }
    Catch {
            Write-Error $_.Exception.Message
            Break;
        }
    Finally {
    }
}

function RunAllWork
{
    Try {
        # Scenario 1 - DB doesn't exist - We should run CreateDB.sql to create it, then create the version table with 1 row
        # Scenario 2 - DB exists, no version table - we should error
        # Scenario 3 - DB exists, version table exists - run the missing scripts, update version table

        # Run Database deletion/creation if neccessary work and appropiate flags are set.
        DropAndCreateDb
        VerifyVersionTable

        #Get the list of database Scripts files from database folder location
        $ScriptsFilesForSql = GetScriptFiles

        #If we have scripts lets continue.
        if($ScriptsFilesForSql -eq $null)
        {
            Write-Host "We have NO FILES in folder $DatabaseUpgradeScriptsPath! No further work will be done."
            return
        }
          
        Write-Host "We have database files in folder $DatabaseUpgradeScriptsPath! process will continue"

        #Get Array of the scripts database values
        $ScriptsInDB = GetVersionTableRows

        #Lets diff the 2 arrays. We should get back the files in the folder location not in db
        $filesNotInDb = $ScriptsFilesForSql | Where-Object {$ScriptsInDB -NotContains $_}

        if($filesNotInDb -eq $null)
        {
            Write-Host "We have NO FILES that are different between the filesystem and db! No further work will be done."
            return
        }

        [array]::Sort($filesNotInDb)

        foreach ($fileNotInDb in $filesNotInDb) {
                
            $fullFileLocation = Join-Path $DatabaseUpgradeScriptsPath $fileNotInDb

            Write-Host "Getting content for For $fullFileLocation"
	        $content = Get-Content -Path  $fullFileLocation
                
            #Run sql that was gotten from the file
            RunNonQuery $content $fullFileLocation

            Write-Host "Finished writing content for $fullFileLocation"

            #Run Insert to version table
            $sqlToRun = "INSERT INTO $versionDbTableName (name) VALUES ('" + $fileNotInDb + "');" 
            RunNonQuery $sqlToRun "Updating Version Table"

            write-host "Version Table: $versionDbTableName updating with File: $fileNotInDb"
        }
    }
    Catch {
        Write-Error $_.Exception.Message
        Exit 1
    }
    Finally {
  
    }
}

#Entry point for running the script
RunAllWork
