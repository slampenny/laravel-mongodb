<?php namespace Jlapp\DynamoDb;

use Exception;
use MongoCollection;

class Collection {

    /**
     * The connection instance.
     *
     * @var Connection
     */
    protected $connection;
    protected $tableName;

    /**
     * Constructor.
     */
    public function __construct(Connection $connection, $tableName)
    {
        $this->connection = $connection;
        $this->tableName = $tableName;
    }

    /**
     * Handle dynamic method calls.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        $start = microtime(true);

        $result = call_user_func_array([$this->connection, $method], array_merge([$this->tableName], $parameters));

        if ($this->connection->logging())
        {
            // Once we have run the query we will calculate the time that it took to run and
            // then log the query, bindings, and execution time so we will report them on
            // the event that the developer needs them. We'll log time in milliseconds.
            $time = $this->connection->getElapsedTime($start);

            $query = [];

            // Convert the query paramters to a json string.
            foreach ($parameters as $parameter)
            {
                try
                {
                    $query[] = json_encode($parameter);
                }
                catch (Exception $e)
                {
                    $query[] = '{...}';
                }
            }

            $queryString = $this->$tableName . '.' . $method . '(' . implode(',', $query) . ')';

            $this->connection->logQuery($queryString, [], $time);
        }

        return $result;
    }

}
