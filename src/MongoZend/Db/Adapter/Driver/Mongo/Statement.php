<?php

/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace MongoZend\Db\Adapter\Driver\Mongo;

use Zend\Db\Adapter\Driver\StatementInterface;
use Zend\Db\Adapter\Exception;
use Zend\Db\Adapter\ParameterContainer;
use Zend\Db\Adapter\Profiler;
use Zend\Db\Sql\AbstractSql;

class Statement implements StatementInterface, Profiler\ProfilerAwareInterface
{
    /**
     * @var \MongoClient
     */
    protected $mongoclient = null;

    /**
     * @var Mongo
     */
    protected $driver = null;

    /**
     * @var Profiler\ProfilerInterface
     */
    protected $profiler = null;

    /**
     * @var string
     */
    protected $sql = '';

    /**
     * Parameter container
     *
     * @var ParameterContainer
     */
    protected $parameterContainer = null;

    /**
     * @var \MongoCollection
     */
    protected $resource = null;

    /**
     * Is prepared
     *
     * @var bool
     */
    protected $isPrepared = false;

    /**
     * @var bool
     */
    protected $bufferResults = false;

    /**
     * @param bool $bufferResults
     */
    public function __construct($bufferResults = false)
    {
        $this->bufferResults = (bool) $bufferResults;
    }

    /**
     * Set driver
     *
     * @param  Mongo     $driver
     * @return Statement
     */
    public function setDriver(Mongo $driver)
    {
        $this->driver = $driver;

        return $this;
    }

    /**
     * Get driver
     *
     * @return Mongo
     */
    public function getDriver()
    {
        return $this->driver;
    }

    /**
     * @param  Profiler\ProfilerInterface $profiler
     * @return Statement
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
     * Initialize
     *
     * @param  \MongoClient $mysqli
     * @return Statement
     */
    public function initialize(\MongoClient $mongoclient)
    {
        $this->mongoclient = $mongoclient;

        return $this;
    }

    /**
     * Set sql
     *
     * @param  MongoZend\Db\NoSql\Select $sql
     * @return Statement
     */
    public function setSql($sql)
    {
        $this->sql = $sql;

        return $this;
    }

    /**
     * Set Parameter container
     *
     * @param  ParameterContainer $parameterContainer
     * @return Statement
     */
    public function setParameterContainer(ParameterContainer $parameterContainer)
    {
        $this->parameterContainer = $parameterContainer;

        return $this;
    }

    /**
     * Get resource
     *
     * @return mixed
     */
    public function getResource()
    {
        return $this->resource;
    }

    /**
     * Set resource
     *
     * @param  \MongoCollection $mongoStatement
     * @return Statement
     */
    public function setResource(\MongoCollection $mongoStatement)
    {
        $this->resource   = $mongoStatement;
        $this->isPrepared = true;

        return $this;
    }

    /**
     * Get sql
     *
     * @return MongoZend\Db\NoSql\Select
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * Get parameter count
     *
     * @return ParameterContainer
     */
    public function getParameterContainer()
    {
        return $this->parameterContainer;
    }

    /**
     * Is prepared
     *
     * @return bool
     */
    public function isPrepared()
    {
        return $this->isPrepared;
    }

    /**
     * Prepare
     *
     * @param  string                          $sql
     * @throws Exception\InvalidQueryException
     * @throws Exception\RuntimeException
     * @return Statement
     */
    public function prepare($sql = null)
    {
        if ($this->isPrepared) {
            throw new Exception\RuntimeException('This statement has already been prepared');
        }

        $sql = ($sql instanceof AbstractSql) ? $sql : $this->sql;

        $this->resource = $this->getDriver()->getConnection()->getDB()->selectCollection($sql->getRawState('table'));
        $this->getDriver()->getConnection()->getDB()->lastError();

        if (!$this->resource instanceof \MongoCollection) {
            throw new Exception\ErrorException($this->getDriver()->getConnection()->getDB()->lastError());
        }

        $this->isPrepared = true;

        return $this;
    }

    /**
     * Execute
     *
     * @param  ParameterContainer|array   $parameters
     * @throws Exception\RuntimeException
     * @return mixed
     */
    public function execute($parameters = null)
    {
        if (!$this->isPrepared) {
            $this->prepare();
        }

        /** START Standard ParameterContainer Merging Block */
        if (!$this->parameterContainer instanceof ParameterContainer) {
            if ($parameters instanceof ParameterContainer) {
                $this->parameterContainer = $parameters;
                $parameters                 = null;
            } else {
                $this->parameterContainer = new ParameterContainer();
            }
        }

        if (is_array($parameters)) {
            $this->parameterContainer->setFromArray($parameters);
        }

        if ($this->parameterContainer->count() > 0) {
            $this->bindParametersFromContainer();
        }
        /** END Standard ParameterContainer Merging Block */
        if ($this->profiler) {
            $this->profiler->profilerStart($this);
        }

        if ($this->bufferResults === true) {
            //            $this -> resource -> store_result();
//            $this -> isPrepared = false;
            $buffered = true;
        } else {
            $buffered = false;
        }

        if ($this->sql instanceof \MongoZend\Db\NoSql\Select) {
            $_order = $this->sql->getOrder();
//            , $this -> sql->getColumns()
            $return = $this->resource->find($this->sql->getWhere());
            if (!empty($_order)) {
                foreach ($_order as $_k => $_v) {
                    if (strtolower($_v) == 'desc' || $_v < 0) {
                        $_order[ $_k ] = -1;
                    } else {
                        $_order[ $_k ] = 1;
                    }
                }
                $return = $return->sort($_order);
            }
            if ($this->sql->getLimit() !== null) {
                $return = $return->limit($this->sql->getLimit());
            }
            if ($this->sql->getOffset() !== null) {
                $return = $return->skip($this->sql->getOffset());
            }
        } elseif ($this->sql instanceof \MongoZend\Db\NoSql\Insert) {
            $_data    = $this->sql->getValues();
            $return   = $this->resource->insert($_data);
            $buffered = $this->sql->getRawState()[ 'table' ];
            $this->driver->getConnection()->storeLastGeneratedValue($buffered, $_data[ '_id' ]);
//          $return = $this -> resource -> update( [ '_id' => $_data[ '_id' ] ], [ '$set' => array( "id" => (string) $_data[ '_id' ] ) ] );
        } elseif ($this->sql instanceof \MongoZend\Db\NoSql\Update) {
            $_data  = $this->sql->getSet();
            $_where = $this->sql->getWhere();
            $return   = $this->resource->update($_where, $_data);
            $buffered = $this->sql->getRawState()[ 'table' ];
            $this->driver->getConnection()->storeLastGeneratedValue($buffered, $_data[ '_id' ]);
        } elseif ($this->sql instanceof \MongoZend\Db\NoSql\Delete) {
            $_where = $this->sql->getWhere();
            $return   = $this->resource->remove($_where);
            $buffered = $this->sql->getRawState()[ 'table' ];

            $this->driver->getConnection()->storeLastGeneratedValue($buffered, null);
        } else {
            $return = $this->resource->execute();
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($return === false) {
            throw new Exception\RuntimeException($this->getDriver()->getConnection()->getDB()->lastError());
        }

        $result = $this->driver->createResult($return, $buffered);

        return $result;
    }

    /**
     * Bind parameters from container
     *
     * @return void
     */
    protected function bindParametersFromContainer()
    {
        $parameters = $this->parameterContainer->getNamedArray();
        $type       = '';
        $args       = array();

        foreach ($parameters as $name => &$value) {
            if ($this->parameterContainer->offsetHasErrata($name)) {
                switch ($this->parameterContainer->offsetGetErrata($name)) {
                    case ParameterContainer::TYPE_DOUBLE:
                        $type .= 'd';
                        break;
                    case ParameterContainer::TYPE_NULL:
                        $value = null; // as per @see http://www.php.net/manual/en/mysqli-stmt.bind-param.php#96148
                    case ParameterContainer::TYPE_INTEGER:
                        $type .= 'i';
                        break;
                    case ParameterContainer::TYPE_STRING:
                    default:
                        $type .= 's';
                        break;
                }
            } else {
                $type .= 's';
            }
            $args[] = &$value;
        }

        if ($args) {
            array_unshift($args, $type);
//            call_user_func_array( array( $this -> resource, 'bind_param' ), $args );
        }
    }
}
