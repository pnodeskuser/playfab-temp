<?php  
date_default_timezone_set('UTC');
ini_set('display_errors', '1');
error_reporting(E_ALL);
ini_set("log_errors", 1);
ini_set("error_log", "php-error.log");

require_once "../config.php";
require_once "../AwsOperations.php";
require_once "../MysqlOperations.php";
require_once "../aws/aws-autoloader.php";

class Sync_playfab_events extends MysqlOperations {

    public $config_api_key;
    public $events_data;

    public function __construct(){
        parent::__construct();
        $this->config_api_key = API_KEY;
        $this->events_data = array (
            array (
                "event_name" => "player_created",
                "event_table_name" => "event_player_created",
                "object_table_name" => "objects_player_created",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_created"
            ),
            array (
                "event_name" => "player_logged_in",
                "event_table_name" => "event_player_logged_in",
                "object_table_name" => "objects_player_logged_in",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_logged_in"
            ),
            array (
                "event_name" => "player_added_title",
                "event_table_name" => "event_player_added_title",
                "object_table_name" => "objects_player_added_title",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_added_title"
            ),
            array (
                "event_name" => "player_inventory_item_added",
                "event_table_name" => "event_player_inventory_item_added",
                "object_table_name" => "objects_player_inventory_item_added",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_inventory_item_added"
            ),
            array (
                "event_name" => "player_vc_item_purchased",
                "event_table_name" => "event_player_vc_item_purchased",
                "object_table_name" => "objects_player_vc_item_purchased",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_vc_item_purchased"
            ),
            array (
                "event_name" => "player_virtual_currency_balance_changed",
                "event_table_name" => "event_virtual_currency_balance_changed",
                "object_table_name" => "objects_virtual_currency_balance_changed",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_virtual_currency_balance_changed"
            ),
            array (
                "event_name" => "practice_round_start",
                "event_table_name" => "event_practice_round_start",
                "object_table_name" => "objects_practice_round_start",
                "aws_bucket_prefix" => "HypernetArena/a746/title.A746.practice_round_start"
            ),
            array (
                "event_name" => "1v1_round_start",
                "event_table_name" => "event_1v1_round_start",
                "object_table_name" => "objects_1v1_round_start",
                "aws_bucket_prefix" => "HypernetArena/a746/title.A746.1v1_round_start"
            ),
            array (
                "event_name" => "tournament_round_start",
                "event_table_name" => "event_tournament_round_start",
                "object_table_name" => "objects_tournament_round_start",
                "aws_bucket_prefix" => "HypernetArena/a746/title.A746.tournament_round_start"
            ),
            array (
                "event_name" => "player_statistic_changed",
                "event_table_name" => "event_player_statistic_changed",
                "object_table_name" => "objects_player_statistic_changed",
                "aws_bucket_prefix" => "HypernetArena/a746/com.playfab.player_statistic_changed"
            )
        );        
    }

    public function SyncObjects($api_key) {

        if($api_key != $this->config_api_key){
            return "Invalid API Key";
        }

        if($this->events_data && sizeof($this->events_data)>0){
            foreach($this->events_data as $key=>$val){
                $event_table = $val["event_table_name"];
                $object_table = $val["object_table_name"];
                $aws_bucket_prefix = $val["aws_bucket_prefix"];

                // Fetching AWS Bucket objects
                $marker =  $this->getObjectMarkerMysql($object_table);
                $objectsArray = $this->ListObjectsAWS($aws_bucket_prefix, $marker);
                $objectsReceived = sizeof($objectsArray);

                // Inserting them in mysql table
                if($objectsReceived>0){
                    $this->setObjectsMysql($object_table,$objectsArray);
                }    
            }
        }
    }

    public function ProcessObjects($api_key){

        if($api_key != $this->config_api_key){
            return "Invalid API Key";
        }

        if($this->events_data && sizeof($this->events_data)>0){
            foreach($this->events_data as $key=>$val){
                $event_name = $val["event_name"];
                $event_table = $val["event_table_name"];
                $object_table = $val["object_table_name"];
                $object_table_id_column = $object_table."_id";
                $aws_bucket_prefix = $val["aws_bucket_prefix"];

                // get Object list
                $objectsArray = $this->getObjectDataMysql($object_table);

                if($objectsArray && sizeof($objectsArray)>0) {

                    foreach($objectsArray as $key=>$objectData){

                        $object_key = $objectData['object_key'];
                        $object_id = $objectData['id'];

                        $fileKeyArray   = explode('/',$object_key);
                        $fileNameArray  = explode('.',end($fileKeyArray));

                        $zip_file_name  = '../files/'.end($fileKeyArray);
                        $json_file_name = '../files/'.$fileNameArray[0].'.'.$fileNameArray[1].'.'.$fileNameArray[2].'.json';
                        $objectsArray   = $this->getObjectDataAWS($object_key, $zip_file_name);

                        stream_copy_to_stream(gzopen($zip_file_name, 'rb'), fopen($json_file_name, 'wb'));

                        $jsonFile = fopen($json_file_name, 'r');
                        $jsonData = fread($jsonFile,filesize($json_file_name));
                        fclose($jsonFile);

                        $data = (array) json_decode($jsonData);

                        $objectSize=0;
                        if(isset($data['EventName'])){
                            $objectSize++;
                            if($event_table == "event_player_created"){
                                
                                $columns = "Created,EventName,EventId,EntityId,Timestamp,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['Created']."','".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                            }
                            else if($event_table == "event_player_logged_in" || $event_table == "event_player_added_title" || $event_table == "event_1v1_round_start" || $event_table == "event_tournament_round_start" || $event_table == "event_practice_round_start"){
                                
                                $columns = "EventName,EventId,EntityId,Timestamp,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$object_id."','".date('Y-m-d h:i:s')."'";
                            }
                            else if($event_table == "event_player_inventory_item_added"){
                                
                                $columns = "EventName,EventId,EntityId,Timestamp,CatalogVersion,ItemId,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['CatalogVersion']."','".$data['ItemId']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                            }
                            else if($event_table == "event_player_vc_item_purchased"){
                                
                                $columns = "EventName,EventId,EntityId,Timestamp,CatalogVersion,ItemId,UnitPrice,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['CatalogVersion']."','".$data['ItemId']."','".$data['UnitPrice']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                            }
                            else if($event_table == "event_virtual_currency_balance_changed"){
                                
                                $columns = "EventName,EventId,EntityId,Timestamp,VirtualCurrencyBalance,VirtualCurrencyPreviousBalance,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['VirtualCurrencyBalance']."','".$data['VirtualCurrencyPreviousBalance']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                            }
                            else if($event_table == "event_player_statistic_changed"){
                                
                                $columns = "EventName,EventId,EntityId,Timestamp,StatisticName,StatisticId,Version,StatisticValue,StatisticPreviousValue,".$object_table_id_column.",CreatedAt";

                                $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['StatisticName']."','".$data['StatisticId']."','".$data['Version']."','".$data['StatisticValue']."','".$data['StatisticPreviousValue']."','".$object_id."','".date('Y-m-d h:i:s')."'";
                            }

                            $this->addPlayerEventMysql($event_table, $columns, $values, $object_table_id_column);
                        }
                        else {
                            $jsonData = str_replace(array("\r\n","\r","\n"), "", $jsonData);
                            $jsonData = rtrim(str_replace('}', '},', $jsonData),',');
                            $jsonData = "[".rtrim(str_replace('},,', '},', $jsonData),',')."]";
                            $jsonData = json_decode($jsonData);

                            foreach($jsonData as $data){
                                $objectSize++;
                                $data = (array)$data;

                                if($event_table == "event_player_created"){
                                    
                                    $columns = "Created,EventName,EventId,EntityId,Timestamp,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['Created']."','".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                                }
                                else if($event_table == "event_player_logged_in" || $event_table == "event_player_added_title" || $event_table == "event_1v1_round_start" || $event_table == "event_tournament_round_start" || $event_table == "event_practice_round_start"){
                                    
                                    $columns = "EventName,EventId,EntityId,Timestamp,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$object_id."','".date('Y-m-d h:i:s')."'";
                                }
                                else if($event_table == "event_player_inventory_item_added"){
                                    
                                    $columns = "EventName,EventId,EntityId,Timestamp,CatalogVersion,ItemId,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['CatalogVersion']."','".$data['ItemId']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                                }
                                else if($event_table == "event_player_vc_item_purchased"){
                                    
                                    $columns = "EventName,EventId,EntityId,Timestamp,CatalogVersion,ItemId,UnitPrice,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['CatalogVersion']."','".$data['ItemId']."','".$data['UnitPrice']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                                }
                                else if($event_table == "event_virtual_currency_balance_changed"){
                                    
                                    $columns = "EventName,EventId,EntityId,Timestamp,VirtualCurrencyBalance,VirtualCurrencyPreviousBalance,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['VirtualCurrencyBalance']."','".$data['VirtualCurrencyPreviousBalance']."','".$object_id."','".date('Y-m-d h:i:s')."'";

                                }
                                else if($event_table == "event_player_statistic_changed"){
                                    
                                    $columns = "EventName,EventId,EntityId,Timestamp,StatisticName,StatisticId,Version,StatisticValue,StatisticPreviousValue,".$object_table_id_column.",CreatedAt";

                                    $values = "'".$data['EventName']."','".$data['EventId']."','".$data['EntityId']."','".$data['Timestamp']."','".$data['StatisticName']."','".$data['StatisticId']."','".$data['Version']."','".$data['StatisticValue']."','".$data['StatisticPreviousValue']."','".$object_id."','".date('Y-m-d h:i:s')."'";
                                }

                                $this->addPlayerEventMysql($event_table, $columns, $values, $object_table_id_column);
                            }
                        }

                        $events_count = $this->countObjectEventsMysql($event_table, $object_table_id_column,$object_id);
                        if($events_count == $objectSize){
                            $this->updateObjectRecordMysql($object_table,$object_id);
                            unlink($zip_file_name);
                            unlink($json_file_name);
                        }
                    }
                }    
            }
        }
    }

}

$api_key = $_GET["api_key"];
if($api_key != ""){
    $obj = new Sync_playfab_events();
    $result = $obj->SyncObjects($api_key);
    $result = $obj->ProcessObjects($api_key);
    echo $result;
}
else {
    echo "API Key is a required input field";
}
