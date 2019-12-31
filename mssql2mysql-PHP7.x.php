<?php
/*
 * SOURCE: MS SQL
 * PHP 7.2/7.3 
 * 2019-12-31
 */
define('MSSQL_HOST','MSSQLHOST');
define('MSSQL_USER','MSSQLUSER');
define('MSSQL_PASSWORD','MSSQLPASS');
define('MSSQL_DATABASE','MSSQLDB');

$connectionOptions = array(
    "database" => MSSQL_DATABASE,
    "uid" => MSSQL_USER,
    "pwd" => MSSQL_PASSWORD
);


/*
 * DESTINATION: MySQL
 */
define('MYSQL_HOST', 'MYSQLHOST');
define('MYSQL_USER', 'MYSQLUSER');
define('MYSQL_PASSWORD','MYSQLPASS');
define('MYSQL_DATABASE','MYSQLDB');

/*
 * SOME HELPER CONSTANT
 */
define('CHUNK_SIZE', 1000);

/*
 * STOP EDITING!
 */

set_time_limit(0);

function addQuote($string)
{
	return "'".$string."'";
}

function addTilde($string)
{
	return "`".$string."`";
}

// Connect MS SQL
$sqlsrv_connect = sqlsrv_connect(MSSQL_HOST, $connectionOptions) or die("Couldn't connect to SQL Server on '".MSSQL_HOST."'' user '".MSSQL_USER."'\n");
echo "=> Connected to Source MS SQL Server on '".MSSQL_HOST."'\n";

// Select MS SQL Database
//$sqlsrv_db = sqlsrv_select_db(MSSQL_DATABASE, $sqlsrv_connect) or die("Couldn't open database '".MSSQL_DATABASE."'\n"); 
//echo "=> Found database '".MSSQL_DATABASE."'\n";

// Connect to MySQL
$mysqli_connect = mysqli_connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD) or die("Couldn't connect to MySQL on '".MYSQL_HOST."'' user '".MYSQL_USER."'\n");
echo "\n=> Connected to Source MySQL Server on ".MYSQL_HOST."\n";

// Select MySQL Database
$sqlsrv_db = mysqli_select_db($mysqli_connect, MYSQL_DATABASE) or die("Couldn't open database '".MYSQL_DATABASE."'\n"); 
echo "=> Found database '".MYSQL_DATABASE."'\n";

mysqli_set_charset ($mysqli_connect, "utf8");
//mysqli_query($mysqli_connect,"set names 'utf8'");


$sqlsrv_tables = array();

// Get MS SQL tables
$sql = "SELECT * FROM sys.Tables;";
$sql = "SELECT * from VRs_Emp;";
$res = sqlsrv_query($sqlsrv_connect, $sql);
echo "\n=> Getting tables..\n";
while ($row = sqlsrv_fetch_array($res,SQLSRV_FETCH_ASSOC))
{


var_dump($row);
	array_push($sqlsrv_tables, $row['name']);
	//echo ($row['name'])."\n";
}
echo "==> Found ". number_format(count($sqlsrv_tables),0,',','.') ." tables\n\n";



// Get Table Structures
if (!empty($sqlsrv_tables))
{
	$i = 1;
	foreach ($sqlsrv_tables as $table)
	{
		echo '====> '.$i.'. '.$table."\n";
		echo "=====> Getting info table ".$table." from SQL Server\n";

		$sql = "select * from information_schema.columns where table_name = '".$table."'";
		$res = sqlsrv_query($sqlsrv_connect, $sql);

		if ($res) 
		{
			$sqlsrv_tables[$table] = array();

			$mysqli = "DROP TABLE IF EXISTS `".$table."`";
			mysqli_query($mysqli_connect,$mysqli);
			$mysqli = "CREATE TABLE `".$table."`";
			$strctsql = $fields = array();

			while ($row = sqlsrv_fetch_array($res, SQLSRV_FETCH_ASSOC))
			{
				//print_r($row); echo "\n";
				array_push($sqlsrv_tables[$table], $row);

				switch ($row['DATA_TYPE']) {
					case 'bit':
					case 'tinyint':
					case 'smallint':
					case 'int':
					case 'bigint':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].')' : '' );
						break;
					
					case 'money':
						$data_type = 'decimal(19,4)';
						break;
					case 'smallmoney':
						$data_type = 'decimal(10,4)';
						break;
					
					case 'real':
					case 'float':
					case 'decimal':
					case 'numeric':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].(!empty($row['NUMERIC_SCALE']) ? ','.$row['NUMERIC_SCALE'] : '').')' : '' );
						break;

					case 'date':
					case 'datetime':
					case 'timestamp':
					case 'time':
						$data_type = $row['DATA_TYPE'];
					case 'datetime2':
					case 'datetimeoffset':
					case 'smalldatetime':
						$data_type = 'datetime';
						break;

					case 'nchar':
					case 'char':
						$data_type = 'char'.(!empty($row['CHARACTER_MAXIMUM_LENGTH']) && $row['CHARACTER_MAXIMUM_LENGTH'] > 0 ? '('.$row['CHARACTER_MAXIMUM_LENGTH'].')' : '(255)' );
						break;
					case 'nvarchar':
					case 'varchar':
						$data_type = 'varchar'.(!empty($row['CHARACTER_MAXIMUM_LENGTH']) && $row['CHARACTER_MAXIMUM_LENGTH'] > 0 ? '('.$row['CHARACTER_MAXIMUM_LENGTH'].')' : '(255)' );
						break;
					case 'ntext':
					case 'text':
						$data_type = 'text';
						break;

					case 'binary':
					case 'varbinary':
						$data_type = $data_type = $row['DATA_TYPE'];
					case 'image':
						$data_type = 'blob';
						break;

					case 'uniqueidentifier':
						$data_type = 'char(36)';//'CHAR(36) NOT NULL';
						break;

					case 'cursor':
					case 'hierarchyid':
					case 'sql_variant':
					case 'table':
					case 'xml':
					default:
						$data_type = false;
						break;
				}

				if (!empty($data_type))
				{
					$ssql = "`".$row['COLUMN_NAME']."` ".$data_type." ".($row['IS_NULLABLE'] == 'YES' ? 'NULL' : 'NOT NULL');
					array_push($strctsql, $ssql);
					array_push($fields, $row['COLUMN_NAME']);	
				}
				
			}

			$mysqli .= "(".implode(',', $strctsql).");";
			echo "======> Creating table ".$table." on MySQL... ";
			$q = mysqli_query($mysqli_connect, $mysqli);
			echo (($q) ? 'Success':'Failed!'."\n".$mysqli."\n")."\n";
			
			echo "=====> Getting data from table ".$table." on SQL Server\n";
			$sql = "SELECT * FROM ".$table;
			$qres = sqlsrv_query($sqlsrv_connect, $sql);
			$numrow = sqlsrv_num_rows($qres);
			echo "======> Found ".number_format($numrow,0,',','.')." rows\n";

			if ($qres)
			{
				echo "=====> Inserting to table ".$table." on MySQL\n";
				$numdata = 0;
				if (!empty($fields))
				{
					$sfield = array_map('addTilde', $fields);
					while ($qrow = sqlsrv_fetch_array($qres, SQLSRV_FETCH_ASSOC))
					{
						$datas = array();
						foreach ($fields as $field) 
						{
							$ddata = (!empty($qrow[$field])) ? $qrow[$field] : '';
							array_push($datas,"'".mysqli_real_escape_string($mysqli_connect, $ddata)."'");
						}

						if (!empty($datas))
						{
							//$datas = array_map('addQuote', $datas);
							//$fields = 
							$mysqli = "INSERT INTO `".$table."` (".implode(',',$sfield).") VALUES (".implode(',',$datas).");";
							//$mysqli = mysqli_real_escape_string($mysqli);
							//echo $mysqli."\n";
							$q = mysqli_query($mysqli_connect, $mysqli);
							$numdata += ($q ? 1 : 0 );
						}
						if ($numdata % CHUNK_SIZE == 0) {
							echo "===> ".number_format($numdata,0,',','.')." data inserted so far\n";
						}
					}
				}
				echo "======> ".number_format($numdata,0,',','.')." data inserted total\n\n";
			}
		}
		$i++;
	}

}

echo "Done!\n";

sqlsrv_close($sqlsrv_connect);
mysqli_close($mysqli_connect);
