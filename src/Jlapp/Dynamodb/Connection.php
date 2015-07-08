<?php namespace Jlapp\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Sdk;

class Connection extends \Illuminate\Database\Connection {

    /**
     * The DynamoDbClient connection handler.
     *
     * @var DynamoDbClient
     */
    protected $connection;

    /**
     * Create a new database connection instance.
     *
     * @param  array   $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        // Create the connection
        $this->connection = $this->createConnection($config);

        // Select database
        //$this->db = $this->getDb($config);

        $this->useDefaultPostProcessor();
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor;
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string  $collection
     * @return QueryBuilder
     */
    public function collection($collection)
    {
        $processor = $this->getPostProcessor();

        $query = new Query\Builder($this, $processor);

        return $query->from($collection);
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string  $table
     * @return QueryBuilder
     */
    public function table($table)
    {
        return $this->collection($table);
    }

    /**
     * Get a MongoDB collection.
     *
     * @param  string   $name
     * @return MongoDB
     */
    public function getCollection($name)
    {
        return new Collection($this, $name);
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return Schema\Builder
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * Create a new DynamoDbClient connection.
     *
     * @param  array   $config
     * @return DynamoDbClient
     */
    protected function createConnection(array $config)
    {
        return new DynamoDbClient($config);
    }

    /**
     * Disconnect from the underlying MongoClient connection.
     *
     * @return void
     */
    public function disconnect()
    {
        $this->connection->close();
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param  array   $config
     * @return string
     */
    protected function getDb(array $config)
    {
        $this->connection = $this->createConnection($config);

        return $this->connection;
    }

    /**
     * Get the elapsed time since a given starting point.
     *
     * @param  int    $start
     * @return float
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
     * Get the PDO driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return 'dynamodb';
    }

    public function find($tableName, $id, $columns = array('*'), $timeout, $orders, $offset, $limit) {
        if (is_array($id)) {
            //if (count($id)) {
            $scanFilter = [];
            foreach ($id as $idValue) {
                $scanFilter[$idValue] =
                    array(
                        'AttributeValueList' => array(
                            array('S' => 'overflow')
                        ),
                        'ComparisonOperator' => 'EQUALS'
                    );
            }
            //}

        } else {
            $scanFilter = [
                $id => array(
                    'AttributeValueList' => array(
                        array('S' => 'overflow')
                    ),
                    'ComparisonOperator' => 'EQUALS'
                )
            ];
        }


        $iterator = $this->connection->getIterator('Scan', array(
            'TableName' => $tableName,
            //'ScanFilter' => $scanFilter,
            //'Select' => 'string',
            //'AttributesToGet' => $columns
        ), ['limit' => $limit]);

        return $iterator;
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array(array($this->connection, $method), $parameters);
    }

}
