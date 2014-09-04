<?php
namespace Cassandra\Type;

class Custom extends Blob{
	/**
	 * 
	 * @var string
	 */
	protected $_name;
	
	public function __construct($value, $name){
		if (!is_string($value)) throw new Exception('Incoming value must be of type string.');
	
		$this->value = $value;
		$this->_name = $name;
	}
}