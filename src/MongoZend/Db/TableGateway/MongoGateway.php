<?php

namespace MongoZend\Db\TableGateway;

use MongoZend\Paginator\Adapter\MongoCursor;
use Zend\Db\Adapter\AdapterInterface;
use Zend\Db\Adapter\Profiler\ProfilerInterface;
use Zend\Db\Exception\InvalidArgumentException;
use Zend\Db\ResultSet\ResultSet;
use Zend\Db\ResultSet\ResultSetInterface;
use Zend\Db\Sql\TableIdentifier;
use Zend\Db\TableGateway\Exception\RuntimeException;
use Zend\Db\TableGateway\Feature\FeatureSet;
use Zend\Db\TableGateway\Feature\AbstractFeature;
use Zend\Paginator\Paginator;
use Zend\Db\TableGateway\TableGatewayInterface;

class MongoGateway implements TableGatewayInterface
{

    /**
     * @var bool
     */
    protected $isInitialized = false;

    /**
     * @var null|AdapterInterface
     */
    protected $adapter = null;

    /**
     * @var null|string
     */
    protected $table = null;

    /**
     * @var \MongoCollection
     */
    protected $collection = null;

    /**
     * @var array
     */
    protected $columns = [ ];

    /**
     * @var FeatureSet
     */
    protected $featureSet = null;

    /**
     * @var ResultSetInterface
     */
    protected $resultSetPrototype = null;

    /**
     * @var ProfilerInterface
     */
    protected $profiler = null;

    /**
     *
     * @var \MongoId
     */
    protected $lastInsertValue = null;

    /**
     * @param string|TableIdentifier                       $table
     * @param AdapterInterface                             $adapter
     * @param AbstractFeature|FeatureSet|AbstractFeature[] $features
     * @param ResultSetInterface                           $resultSetPrototype
     * @param Sql                                          $sql
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        $table,
        AdapterInterface $adapter,
        $features = null,
        ResultSetInterface $resultSetPrototype = null,
        Sql $sql = null
    ) {
        // table
        if (!( is_string( $table ) || $table instanceof TableIdentifier )) {
            throw new InvalidArgumentException( 'Table name must be a string or an instance of Zend\Db\Sql\TableIdentifier' );
        }
        $this->table = $table;

        // adapter
        $this->adapter = $adapter;

        // process features
        if ($features !== null) {
            if ($features instanceof AbstractFeature) {
                $features = [ $features ];
            }
            if (is_array( $features )) {
                $this->featureSet = new FeatureSet( $features );
            } elseif ($features instanceof FeatureSet) {
                $this->featureSet = $features;
            } else {
                throw new InvalidArgumentException(
                    'TableGateway expects $feature to be an instance of an AbstractFeature or a FeatureSet, or an array of AbstractFeatures'
                );
            }
        } else {
            $this->featureSet = new FeatureSet();
        }

        // result prototype
        $this->resultSetPrototype = ( $resultSetPrototype ) ?: new ResultSet();

        $this->initialize();
    }

    /**
     * @return bool
     */
    public function isInitialized()
    {
        return $this->isInitialized;
    }

    /**
     * Initialize
     *
     * @throws RuntimeException
     * @return null
     */
    public function initialize()
    {
        if ($this->isInitialized()) {
            return;
        }

        if (!$this->featureSet instanceof FeatureSet) {
            $this->featureSet = new FeatureSet();
        }

        // :FIXME: add featureSet compat
//        $this -> featureSet -> setTableGateway( $this );
//        $this -> featureSet -> apply( 'preInitialize', array() );

        if (!$this->adapter instanceof AdapterInterface) {
            throw new RuntimeException( 'This table does not have an Adapter setup' );
        }

        if (!is_string( $this->table )
            && !$this->table instanceof TableIdentifier
        ) {
            throw new RuntimeException( 'This table object does not have a valid table set.' );
        }

        if (!$this->resultSetPrototype instanceof ResultSetInterface) {
            $this->resultSetPrototype = new ResultSet();
        }

        $this->collection = $this->adapter->getDriver()->getConnection()
                                          ->getDB()
                                          ->selectCollection( $this->getTable() );
//        $this -> featureSet -> apply( 'postInitialize', array() );

        $this->isInitialized = true;
    }

    /**
     * Get table name
     *
     * @return string
     */
    public function getTable()
    {
        return $this->table instanceof TableIdentifier
            ? $this->table->getTable() : $this->table;
    }

    /**
     * Get adapter
     *
     * @return AdapterInterface
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * @return array
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * @return FeatureSet
     */
    public function getFeatureSet()
    {
        return $this->featureSet;
    }

    /**
     * Get select result prototype
     *
     * @return ResultSet
     */
    public function getResultSetPrototype()
    {
        return $this->resultSetPrototype;
    }

    /**
     * @param ProfilerInterface $profiler
     *
     * @return Statement
     */
    public function setProfiler( ProfilerInterface $profiler )
    {
        $this->profiler = $profiler;

        return $this;
    }

    /**
     * @return null|ProfilerInterface
     */
    public function getProfiler()
    {
        return $this->profiler;
    }

    protected function convertId( $ids )
    {
        if (is_array( $ids )) {
            foreach ($ids as $_k => $_v) {
                $ids[ $_k ] = $this->convertId( $_v );
            }
        } elseif (!$ids instanceof \MongoId
                  && preg_match( '/^[0123456789abcdefABCDEF]{24}$/', $ids )
        ) {
            $ids = new \MongoId( $ids );
        }

        return $ids;
    }

    protected function _convertIds( $fields = [ ] )
    {
        if ($fields == null) {
            $fields = [ ];
        }
        foreach ($fields as $_key => $_value) {
            $_value          = $this->convertId( $_value );
            $fields[ $_key ] = $_value;
        }

        return $fields;
    }

    protected function _where( $where = [ ] )
    {
        return $this->_mongoWhere( $this->_convertIds( $where ) );
    }

    protected function _mongoWhere( $where = [ ] )
    {
        $_where = [ ];
        foreach ($where as $_key => $_value) {
            if ($_key{0} == '$' || is_numeric( $_key )) {
                if (is_array( $_value )) {
                    $_value = $this->_mongoWhere( $_value );
                }
            } elseif ($_key{0} == '-' && is_array( $_value )) {
                $_value = [ '$nin' => $_value ];
                $_key   = substr( $_key, 1 );
            } elseif ($_key{0} == '-' && !is_array( $_value )) {
                $_value = [ '$ne' => $_value ];
                $_key   = substr( $_key, 1 );
            } elseif (is_array( $_value ) && count( $_value )) {
                $_1key = array_keys( $_value )[ 0 ];
                if ($_1key{0} == '$') {
                    $_value = $this->_mongoWhere( $_value );
                } else {
                    $_value = [ '$in' => $_value ];
                }
            }
            $_where[ $_key ] = $_value;
        }

        return $_where;
    }

    protected function _orders( $orders = [ ] )
    {
        if ($orders == null) {
            $orders = [ ];
        }
        foreach ($orders as $_k => $_v) {
            if (strtolower( $_v ) == 'desc' || $_v < 0) {
                $orders[ $_k ] = -1;
            } else {
                $orders[ $_k ] = 1;
            }
        }

        return $orders;
    }

    /**
     * Select
     *
     * @param Where|\Closure|string|array $where
     *
     * @return ResultSet
     * @throws RuntimeException
     */
    public function select( $where = null )
    {
        if ($this->profiler) {
            $this->profiler->profilerStart( $this );
        }

        // apply preSelect features
        // $this -> featureSet -> apply( 'preSelect', array( $select ) );

        $return = $this->collection->find( $this->_where( $where ) );

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($return === false) {
            throw new RuntimeException( $this->getDriver()->getConnection()
                                             ->getDB()->lastError() );
        }

        $result = $this->adapter->getDriver()
                                ->createResult( $return, $this->getTable() );

        $resultSet = clone $this->resultSetPrototype;
        $resultSet->initialize( $result );

        // apply postSelect features
        // $this -> featureSet -> apply( 'postSelect', array( $result, $resultSet ) );

        return $resultSet;
    }

    /**
     * Insert
     *
     * @param array $set
     *
     * @return array
     */
    public function insert( $set )
    {
        $return                = $this->collection->insert( $set );
        $this->lastInsertValue = $set[ '_id' ];

        return $return;
    }

    /**
     * Update
     *
     * @param array $set
     * @param array $where
     *
     * @return array
     */
    public function update( $set, $where = null )
    {
        $return                =
            $this->collection->update( $this->_where( $where ),
                [ '$set' => $this->_convertids( $set ) ],
                [ 'multiple' => true ] );
        $this->lastInsertValue = 0;

        return $return;
    }

    /**
     * Delete
     *
     * @param Where|\Closure|string|array $where
     *
     * @return int
     */
    public function delete( $where )
    {
        $return                =
            $this->collection->remove( $this->_where( $where ) );
        $this->lastInsertValue = 0;

        return $return[ 'ok' ];
    }

    /**
     * Drop
     *
     * @return int
     */
    public function drop()
    {
        $return                = $this->collection->drop();
        $this->lastInsertValue = 0;

        return $return[ 'ok' ];
    }

    /**
     * @return \Zend\Db\ResultSet\ResultSet
     */
    public function fetchAll()
    {
        return $this->select();
    }

    /**
     * @param string $id
     *
     * @return DataModelInterface
     * @throws \Exception|RuntimeException
     */
    public function get( $id )
    {
        if ($this->profiler) {
            $this->profiler->profilerStart( $this );
        }

        // apply preSelect features
//        $this -> featureSet -> apply( 'preSelect', array( $select ) );

        try {
            //          $where = [ '_id' => $this -> convertid( $id ) ];
            $return =
                $this->collection->findOne( $this->_where( [ '_id' => $id ] ) );
        } catch ( \Exception $ex ) {
            throw new \Exception( 'wrong mongo id object.' .
                                  $ex->getMessage() );
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($return === false) {
            throw new RuntimeException( $this->getDriver()->getConnection()
                                             ->getDB()->lastError() );
        }

        if ($return === null) {
            return;
        }

        $model  = clone $this->resultSetPrototype->getArrayObjectPrototype();
        $result = $model->exchangeArray( $return );

        return $result;
    }

    /**
     * @param array $where
     *
     * @return DataModelInterface
     * @throws RuntimeException
     */
    public function findOne( array $where )
    {
        if ($this->profiler) {
            $this->profiler->profilerStart( $this );
        }

        // apply preSelect features
        // $this -> featureSet -> apply( 'preSelect', array( $select ) );

        $return = $this->collection->findOne( $this->_where( $where ) );

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($return === false) {
            throw new RuntimeException( $this->getDriver()->getConnection()
                                             ->getDB()->lastError() );
        }

        if ($return === null) {
            return;
        }

        $model  = clone $this->resultSetPrototype->getArrayObjectPrototype();
        $result = $model->exchangeArray( $return );

        return $result;
    }

    /**
     * @param array    $fields
     * @param array    $orders
     * @param null|int $limit
     * @param null|int $offset
     *
     * @return ResultSet|ResultSetInterface
     * @throws RuntimeException
     * @throws \Exception
     */
    public function find(
        $fields = [ ],
        $orders = [ ],
        $limit = null,
        $offset = null
    ) {
        if (!is_array( $fields ) || !is_array( $orders )) {
            throw new \Exception( "Wrong input type of parameters !" );
        }

        if ($this->profiler) {
            $this->profiler->profilerStart( $this );
        }

        // apply preSelect features
//        $this -> featureSet -> apply( 'preSelect', array( $select ) );

        $return = $this->collection->find( $this->_where( $fields ) );

        foreach ($orders as $_k => $_v) {
            if (strtolower( $_v ) == 'desc' || $_v < 0) {
                $orders[ $_k ] = -1;
            } else {
                $orders[ $_k ] = 1;
            }
        }

        if (!empty( $orders )) {
            $return->sort( $this->_orders( $orders ) );
        }

        if ($limit !== null) {
            $return->limit( $limit );
        }

        if ($offset !== null) {
            $return->skip( $offset );
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }

        if ($return === false) {
            throw new RuntimeException( $this->getDriver()->getConnection()
                                             ->getDB()->lastError() );
        }
        $result    = $this->adapter->getDriver()
                                   ->createResult( $return, $this->getTable() );
        $resultSet = clone $this->resultSetPrototype;
        $resultSet->initialize( $result );

        // apply postSelect features
//        $this -> featureSet -> apply( 'postSelect', array( $result, $resultSet ) );

        return $resultSet;
    }

    /**
     * @param array $model
     *
     * @return array|bool|int
     * @throws \Exception
     */
    public function save( $model )
    {
        $data = $this->_convertids( $model );
        $_id  = $model['_id'];
        if (empty( $_id )) {
            $insertResult = $this->insert( $data );

            $result       = empty( $insertResult[ 'ok' ] ) ? 0
                : $insertResult[ 'ok' ];
            if ($result) {
                $model['_id'] = $data[ '_id' ];
            }
        } else {
            if ($this->get( $_id )) {
                $result =
                    $this->update( $data, $this->_where( [ '_id' => $_id ] ) );
            } else {
                throw new \Exception( 'Model id does not exist' );
            }
        }

        return $result;
    }

    /**
     * @param array $fields
     * @param array $where
     * @param array $orders
     *
     * @return Paginator
     */
    public function getPages(
        $fields = [ ],
        $where = [ ],
        $orders = [ ]
    )
    {
        $cursor = $this->collection->find( $this->_where( $where ), $fields );
        if (!empty( $orders )) {
            $cursor->sort( $this->_orders( $orders ) );
        }

        $adapter = new MongoCursor( $cursor, $this->getResultSetPrototype() );
        $pager   = new Paginator( $adapter );

        return $pager;
    }

    /**
     * @return \MongoId
     */
    public function getLastInsertId()
    {
        return $this->lastInsertValue;
    }

    /**
     * @return mixed
     */
    public function model()
    {
        return clone $this->resultSetPrototype->getArrayObjectPrototype();
    }

    /**
     * @param array $index
     */
    public function ensureIndex( $index )
    {
        $this->dropIndexes();
        $this->collection->ensureIndex( $index, [
            'name' => $this->table . 'TextIndex',
        ] );
    }

    /**
     * @param array $indexes
     */
    public function createIndexes( $indexes )
    {
        $this->dropIndexes();
        foreach ($indexes as $name => $index) {
            $this->createIndex( $index, [ 'name' => $name ] );
        }
    }

    /**
     * @param array $index
     */
    public function createIndex( $index, $options = [ ] )
    {
        if (!count( $options )) {
            $options = [
                'name' => $this->table . 'TextIndex',
            ];
        }

        $this->collection->ensureIndex( $index, $options );
    }

    /**
     * @param $name
     *
     * @return array
     */
    public function dropIndex( $name )
    {
        return $this->collection->deleteIndex( $name );
    }

    /**
     * @return array
     */
    public function dropIndexes()
    {
        return $this->collection->deleteIndexes();
    }

}
