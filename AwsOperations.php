<?php

CLASS AwsOperations {

	public $access_key;
	public $secret_key;
	public $version;
	public $region;
	public $s3ClientObj;
	public $plafab_bucket_name;

	public function __construct(){

		$this->access_key 			= AWS_ACCESS_KEY;
		$this->secret_key 			= AWS_SECRET_KEY;
		$this->version 				= AWS_VERSION;
		$this->region 				= AWS_REGION;
		$this->plafab_bucket_name 	= AWS_PLAYFAB_BUCKET_NAME;

		// Creating AWS connections
		$this->s3ClientObj = new Aws\S3\S3Client([
		    'credentials' => array(
		        'key'    => trim($this->access_key),
		        'secret' => trim($this->secret_key)
		    ),	
		    'version' => $this->version,
		    'region'  => $this->region
		]);
	}

	public function ListObjectsAWS($prefix, $marker = "", $return = array()){

		$ObjectsArrayResponse = $this->s3ClientObj->listObjects([
			'Bucket' => $this->plafab_bucket_name,
			'Prefix' => $prefix,
			'Marker' => $marker
		]);

		if(isset($ObjectsArrayResponse['Contents']) && sizeof($ObjectsArrayResponse['Contents'])>0){
			foreach($ObjectsArrayResponse['Contents'] as $key=>$content){
				$return[] = $content['Key'];
				if($key==999){
					return $this->ListObjectsAWS($prefix, $content['Key'], $return);
				}
			}
			return $return;
		}
		return null;
	}

	public function getObjectDataAWS($objectKey, $file){
		$result = $this->s3ClientObj->getObject([
		    'Bucket' => $this->plafab_bucket_name,
		    'Key' => $objectKey,
		    'SaveAs' => $file
		]);
	}


}
