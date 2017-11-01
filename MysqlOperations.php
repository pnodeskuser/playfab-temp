<?php

Class MysqlOperations extends AwsOperations {

	public $host;
	public $user;
	public $password;
	public $db;
	public $port;
	public $mysqlObj;

	public function __construct(){
		parent::__construct();

		$this->host 	= MYSQL_HOST;
		$this->user 	= MYSQL_USER;
		$this->password = MYSQL_PASSWORD;
		$this->db 		= MYSQL_DB_NAME;
		$this->port 	= MYSQL_PORT;

		// Creating mysql connections
		$this->mysqlObj = new mysqli($this->host, $this->user, $this->password, $this->db);
		if($this->mysqlObj->connect_error) {
		    die("Connection failed: " . $this->mysqlObj->connect_error);
		}
	}

	public function setObjectsMysql($object_table, $objects_array){
		$created_at = date("Y-m-d h:i:s");
		$sql = "INSERT INTO $object_table (object_key, created_at) VALUES ";
		foreach($objects_array as $key=>$value){
			if($key != 0) $sql .= ",";
			$sql .= "('$value','$created_at')";
		}

		//echo $sql;
		$result = $this->mysqlObj->query($sql);
		return $result;
	}

	public function getObjectListOld($object_table, $offset=0,$limit=1000){
		$sql = "SELECT id,object_key FROM $object_table LIMIT $offset, $limit";
		$result = $this->mysqlObj->query($sql);
		if($result->num_rows > 0) {
		    $return = array();
		    $count = 0;
		    while($row = $result->fetch_assoc()) {
		    	$return[$count]['object_key'] = $row['object_key'];
		    	$return[$count]['id'] = $row['id'];
		    	$count++;
		    }
		    return $return;
		}
		return null;
	}

	public function getObjectDataMysql($object_table){
		//$sql = "SELECT id, object_key FROM objects_player_logged_in WHERE id > (SELECT id FROM objects_player_logged_in WHERE object_key='$object_key') ORDER BY id ASC";
		$sql = "SELECT id, object_key FROM $object_table WHERE processed=0 ORDER BY id ASC";
		$result = $this->mysqlObj->query($sql);
		if($result->num_rows > 0) {
		    $return = array();
		    $count = 0;
		    while($row = $result->fetch_assoc()) {
		    	$return[$count]['id'] = $row['id'];
		    	$return[$count]['object_key'] = $row['object_key'];
		    	$count++;
		    }
		    return $return;
		}
		return null;
	}

	public function addPlayerEventMysql($event_table,$columns,$values, $object_table_id_column){
		$sql = "INSERT INTO $event_table ($columns) VALUES ($values) ON DUPLICATE KEY UPDATE $object_table_id_column=VALUES($object_table_id_column)";
		$result = $this->mysqlObj->query($sql);
	}

	public function getObjectMarkerMysql($object_table){
		$sql = "SELECT object_key FROM $object_table ORDER BY id DESC LIMIT 1";
		$result = $this->mysqlObj->query($sql);
		$result = $result->fetch_assoc();
		return $result['object_key'];
	}

	public function countObjectEventsMysql($event_table, $objectTableIdColumn,$object_id){
		$sql = "SELECT COUNT(*) AS events_count FROM $event_table WHERE $objectTableIdColumn=$object_id";
		$result = $this->mysqlObj->query($sql);
		$result = $result->fetch_assoc();
		return $result['events_count'];
	}

	public function updateObjectRecordMysql($object_table,$object_id){
		$sql = "UPDATE $object_table SET processed=1 WHERE id=$object_id";
		$result = $this->mysqlObj->query($sql);
	}
}