<?php

/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace MongoZend\Db\Adapter\Driver\Mongo;

use Zend\Db\Adapter\Driver\ConnectionInterface;
use Zend\Db\Adapter\Exception;
use Zend\Db\Adapter\Profiler;

class Connection implements ConnectionInterface, Profiler\ProfilerAwareInterface
{
    /**
     * @var Mongo
     */
    protected $driver = null;

    /**
     * @var Profiler\ProfilerInterface
     */
    protected $profiler = null;

    /**
     * Connection parameters
     *
     * @var array
     */
    protected $connectionParameters = array();

    /**
     * @var \MongoClient
     */
    protected $resource = null;

    /**
     * @var \MongoDB
     */
    protected $db = null;

    /**
     * In transaction
     *
     * @var bool
     */
    protected $inTransaction = false;

    /**
     * Last Generated Values
     *
     * @var array
     */
    protected $_lastGeneratedValues = null;

    /**
     * Constructor
     *
     * @param  array|\MongoClient|null            $connectionParameters
     * @throws Exception\InvalidArgumentException
     */
    public function __construct($connectionParameters = null)
    {
        if (is_array($connectionParameters)) {
            $this->setConnectionParameters($connectionParameters);
        } elseif ($connectionParameters instanceof \MongoClient) {
            $this->setResource($connectionParameters);
        } elseif (null !== $connectionParameters) {
            throw new Exception\InvalidArgumentException('$connection must be an array of parameters, a MongoClient object or null');
        }
    }

    /**
     * @param  \Mongo     $driver
     * @return Connection
     */
    public function setDriver(Mongo $driver)
    {
        $this->driver = $driver;

        return $this;
    }

    /**
     * @param  Profiler\ProfilerInterface $profiler
     * @return Connection
     */
    public function setProfiler(Profiler\ProfilerInterface $profiler)
    {
        $this->profiler = $profiler;

        return $this;
    }

    /**
     * @return null|Profiler\ProfilerInterface
     */
    public function getProfiler()
    {
        return $this->profiler;
    }

    /**
     * Set connection parameters
     *
     * @param  array      $connectionParameters
     * @return Connection
     */
    public function setConnectionParameters(array $connectionParameters)
    {
        $this->connectionParameters = $connectionParameters;
        if ($this->isConnected()) {
            $this->disconnect();
        }
        $this->connect();

        return $this;
    }

    /**
     * Get connection parameters
     *
     * @return array
     */
    public function getConnectionParameters()
    {
        return $this->connectionParameters;
    }

    /**
     * Get current schema
     *
     * @return string
     */
    public function getCurrentSchema()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->connectionParameters[ 'dbname' ];
    }

    /**
     * Set resource
     *
     * @param  \MongoClient $resource
     * @return Connection
     */
    public function setResource(\MongoClient $resource)
    {
        $this->resource = $resource;

        return $this;
    }

    /**
     * Get resource
     *
     * @return \MongoClient
     */
    public function getResource()
    {
        $this->connect();

        return $this->resource;
    }

    /**
     * Get db
     *
     * @return \MongoClient
     */
    public function getDB()
    {
        $this->connect();

        return $this->db;
    }

    /**
     * Connect
     *
     * @throws Exception\RuntimeException
     * @return Connection
     */
    public function connect()
    {
        if ($this->resource instanceof \MongoClient) {
            return $this;
        }

        // localize
        $p = $this->connectionParameters;

        // given a list of key names, test for existence in $p
        $findParameterValue = function (array $names) use ($p) {
            foreach ($names as $name) {
                if (isset($p[ $name ])) {
                    return $p[ $name ];
                }
            }

            return;
        };

        $options = $findParameterValue(array( 'options' ));
        if (!$options) {
            $options = array();
        }
        $dsn      = $findParameterValue(array( 'dsn' ));
        $database = $findParameterValue(array( 'database', 'dbname', 'db', 'schema' ));
        $username = $findParameterValue(array( 'username', 'user' ));
        $password = $findParameterValue(array( 'password', 'passwd', 'pw' ));
        if ($username) {
            $options[ 'username' ] = $username;
        }
        if ($password) {
            $options[ 'password' ] = $password;
        }
        if ($database) {
            $options[ 'db' ] = $database;
        }
        $this->connectionParameters[ 'dbname' ] = $database;

//        $port   = (isset( $p[ 'port' ] )) ? (int) $p[ 'port' ] : null;
//        $socket = (isset( $p[ 'socket' ] )) ? $p[ 'socket' ] : null;
        # :FIXME: fill dsn string with all options values

        try {
            $this->resource = new \MongoClient($dsn, $options);
            $this->db       = $this->resource->selectDB($database);
        } catch (\Exception $ex) {
            throw new Exception\RuntimeException(
            'Connection error', null, new Exception\ErrorException($ex->getMessage(), $ex->getCode())
            );
        }

        return $this;
    }

    /**
     * Is connected
     *
     * @return bool
     */
    public function isConnected()
    {
        return ($this->db instanceof \MongoDB);
    }

    /**
     * Disconnect
     *
     * @return void
     */
    public function disconnect()
    {
        unset($this->resource);
        unset($this->db);
        $this->resource = null;
        $this->db       = null;
    }

    /**
     * Begin transaction
     *
     * @return void
     */
    public function beginTransaction()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->inTransaction = true;
    }

    /**
     * Commit
     *
     * @return void
     */
    public function commit()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->inTransaction = false;
    }

    /**
     * Rollback
     *
     * @throws Exception\RuntimeException
     * @return Connection
     */
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new Exception\RuntimeException('Must be connected before you can rollback.');
        }

        if (!$this->inTransaction) {
            throw new Exception\RuntimeException('Must call commit() before you can rollback.');
        }

        return $this;
    }

    /**
     * Execute
     *
     * @param  string                          $sql
     * @throws Exception\InvalidQueryException
     * @return Result
     */
    public function execute($sql)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }
        try {
            if ($this->profiler) {
                $this->profiler->profilerStart($sql);
            }

            $resultResource = $this->db->execute($sql);

            if ($this->profiler) {
                $this->profiler->profilerFinish($sql);
            }

            // if the returnValue is something other than a mysqli_result, bypass wrapping it
            if (!$resultResource['ok']) {
                throw new Exception\InvalidQueryException($resultResource['errmsg']);
            }
        } catch (\Exception $e) {
            throw new Exception\InvalidQueryException($ex->getMessage(), $ex->getCode());
        }

        $resultPrototype = $this->driver->createResult(($resultResource === true) ? $this->resource : $resultResource);

        return $resultResource['retval'];
    }

    /**
     * Get last generated id
     *
     * @param  null $name Ignored
     * @return int
     */
    public function getLastGeneratedValue($name = null)
    {
        if ($name !== null) {
            if (!empty($this->_lastGeneratedValues[ $name ])) {
                return $this->_lastGeneratedValues[ $name ];
            }
        } else {
            return reset($this->_lastGeneratedValues);
        }

        return 0;
    }

    /**
     * Set last generated id
     *
     * @param  string     $name
     * @param  \MongoId   $value
     * @return Connection
     */
    public function storeLastGeneratedValue($name, $value = null)
    {
        $this->_lastGeneratedValues[ $name ] = $value;

        return $this;
    }
}
