<?php

namespace MongoZend\Db\Adapter;

use Zend\Db\Exception;
use Zend\Db\Adapter\Driver;
use Zend\Db\Exception\InvalidArgumentException;
use Zend\Db\Adapter\Driver\Mysqli\Mysqli;
use Zend\Db\Adapter\Driver\Sqlsrv\Sqlsrv;
use Zend\Db\Adapter\Driver\Oci8\Oci8;
use Zend\Db\Adapter\Driver\Pgsql\Pgsql;
use Zend\Db\Adapter\Driver\IbmDb2\IbmDb2;
use Zend\Db\Adapter\Driver\Pdo\Pdo;
use Zend\Db\Adapter\Driver\DriverInterface;
use Zend\Db\Adapter\Platform\Mysql;
use Zend\Db\Adapter\Platform\Oracle;
use Zend\Db\Adapter\Platform\Postgresql;
use Zend\Db\Adapter\Platform\Sql92;
use Zend\Db\Adapter\Platform\Sqlite;
use Zend\Db\Adapter\Platform\SqlServer;
use Zend\Db\Adapter\Platform\IbmDb2 as PlatformIbmDb2;
use MongoZend\Db\Adapter\Driver\Mongo\Mongo;

use Zend\Db\Adapter\Adapter as ZendAdapter;

class Adapter extends ZendAdapter
{

    /**
     * @param  array $parameters
     *
     * @return DriverInterface
     * @throws InvalidArgumentException
     */
    protected function createDriver($parameters)
    {
        if ( !isset($parameters['driver'])) {
            throw new InvalidArgumentException(__FUNCTION__
                . ' expects a "driver" key to be present inside the parameters');
        }

        if ($parameters['driver'] instanceof DriverInterface) {
            return $parameters['driver'];
        }

        if ( !is_string($parameters['driver'])) {
            throw new InvalidArgumentException(__FUNCTION__
                . ' expects a "driver" to be a string or instance of DriverInterface');
        }

        $options = [];
        if (isset($parameters['options'])) {
            $options = (array)$parameters['options'];
            unset($parameters['options']);
        }

        $driverName = strtolower($parameters['driver']);
        switch ($driverName) {
            case 'mysqli':
                $driver = new Mysqli($parameters,
                    null, null, $options);
                break;
            case 'sqlsrv':
                $driver
                    = new Sqlsrv($parameters);
                break;
            case 'oci8':
                $driver = new Oci8($parameters);
                break;
            case 'pgsql':
                $driver = new Pgsql($parameters);
                break;
            case 'ibmdb2':
                $driver
                    = new IbmDb2($parameters);
                break;
            case 'mongo':
                $driver
                    = new Mongo($parameters);
                break;
            case 'pdo':
            default:
                if ($driverName == 'pdo' || strpos($driverName, 'pdo') === 0) {
                    $driver = new Pdo($parameters);
                }
        }

        if ( !isset($driver) || !$driver instanceof DriverInterface) {
            throw new InvalidArgumentException('DriverInterface expected', null,
                null);
        }

        return $driver;
    }

    /**
     * @param DriverInterface $parameters
     * @return Platform\PlatformInterface
     */
    protected function createPlatform($parameters)
    {
        if (isset($parameters['platform'])) {
            $platformName = $parameters['platform'];
        } elseif ($this->driver instanceof Driver\DriverInterface) {
            $platformName = $this->driver->getDatabasePlatformName(Driver\DriverInterface::NAME_FORMAT_CAMELCASE);
        } else {
            throw new Exception\InvalidArgumentException('A platform could not be determined from the provided configuration');
        }

        // currently only supported by the IbmDb2 & Oracle concrete implementations
        $options = (isset($parameters['platform_options'])) ? $parameters['platform_options'] : [];

        switch ($platformName) {
            case 'Mysql':
                // mysqli or pdo_mysql driver
                $driver = ($this->driver instanceof Driver\Mysqli\Mysqli || $this->driver instanceof Driver\Pdo\Pdo) ? $this->driver : null;
                return new Mysql($driver);
            case 'SqlServer':
                // PDO is only supported driver for quoting values in this platform
                return new SqlServer(($this->driver instanceof Driver\Pdo\Pdo) ? $this->driver : null);
            case 'Oracle':
                // oracle does not accept a driver as an option, no driver specific quoting available
                return new Oracle($options);
            case 'Sqlite':
                // PDO is only supported driver for quoting values in this platform
                return new Sqlite(($this->driver instanceof Driver\Pdo\Pdo) ? $this->driver : null);
            case 'Postgresql':
                // pgsql or pdo postgres driver
                $driver = ($this->driver instanceof Driver\Pgsql\Pgsql || $this->driver instanceof Driver\Pdo\Pdo) ? $this->driver : null;
                return new Postgresql($driver);
            case 'IbmDb2':
                // ibm_db2 driver escaping does not need an action connection
                return new PlatformIbmDb2($options);
            case 'Mongo':
                // ibm_db2 driver escaping does not need an action connection
                return new Platform\Mongo($options);
            default:
                return new Sql92();
        }
    }
}
