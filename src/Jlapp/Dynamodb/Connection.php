<?php namespace Jlapp\Dynament;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Sdk;

class Connection extends \Illuminate\Database\Connection {

    /**
     * The MongoDB database handler.
     *
     * @var DynamoDB
     */
    protected $db;

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
     * @return void
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        // Build the connection string
        /*$sdk = $this->getSdk($config);*/

        // You can pass options directly to the MongoClient constructor
        $options = array_get($config, 'options', []);

        // Create the connection
        $this->connection = $this->createConnection($config, $options);

        // Select database
        $this->db = $this->connection->{$config['database']};

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
        return new Collection($this, $this->db->selectCollection($name));
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
     * Get the DynamoDB database object.
     *
     * @return  DynamoDB
     */
    public function getDynamoDB()
    {
        return $this->db;
    }

    /**
     * return DynamoDbClient object.
     *
     * @return DynamoDbClient
     */
    /*public function getMongoClient()
    {
        return $this->connection;
    }*/

    /**
     * Create a new DynamoDbClient connection.
     *
     * @param  string  $dsn
     * @param  array   $config
     * @param  array   $options
     * @return DynamoDbClient
     */
    protected function createConnection(array $config, array $options)
    {
        // Add credentials as options, this makes sure the connection will not fail if
        // the username or password contains strange characters.
        if ( ! empty($config['username']))
        {
            $options['username'] = $config['username'];
        }

        if ( ! empty($config['password']))
        {
            $options['password'] = $config['password'];
        }

        /*// By default driver options is an empty array.
        $driverOptions = array();

        if (isset($config['driver_options']) && is_array($config['driver_options']))
        {
            $driverOptions = $config['driver_options'];
        }*/

        return new DynamoDbClient([
            'profile' => $config['profile'],
            'region'  => $config['region'],
            'version'  => $config['version'],
        ]);
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
        // First we will create the basic DSN setup as well as the port if it is in
        // in the configuration options. This will give us the basic DSN we will
        // need to establish the MongoClient and return them back for use.
        extract($config);

        $sdk = new Sdk([
            'profile'   => $config['profile'],
            'region'    => $config['region'],
            'version'   => $config['version'],
            'DynamoDb'  => [
                'region'    => $config['DynamoDb']['region']
            ]
        ]);

        return $sdk->createDynamoDb();
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

    /**
     * Dynamically pass methods to the connection.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array(array($this->db, $method), $parameters);
    }

}
