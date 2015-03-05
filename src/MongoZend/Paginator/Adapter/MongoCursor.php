<?php

namespace MongoZend\Paginator\Adapter;

use Zend\Db\ResultSet\ResultSet;
use Zend\Db\ResultSet\ResultSetInterface;
use Zend\Paginator\Adapter\AdapterInterface;
use MongoZend\Db\Adapter\Driver\Mongo\Result;

// , ResultInterface
class MongoCursor implements AdapterInterface
{
    /**
     * Database query
     *
     * @var \MongoCursor
     */
    protected $cursor = null;

    /**
     * @var ResultSet
     */
    protected $resultSetPrototype = null;

    /**
     * Total item count
     *
     * @var int
     */
    protected $rowCount = null;

    /**
     * Constructor.
     *
     * @param  \MongoCursor                       $cursor             The select query
     * @param  null|ResultSetInterface            $resultSetPrototype
     * @throws Exception\InvalidArgumentException
     */
    public function __construct(\MongoCursor $cursor, ResultSetInterface $resultSetPrototype = null)
    {
        $this->cursor             = $cursor;
        $this->resultSetPrototype = ($resultSetPrototype) ?: new ResultSet();
    }

    /**
     * @param  \MongoCursor $resource
     * @param  mixed        $context
     * @return Result
     */
    public function createResult($resource)
    {
        $result = new Result();
        if ($resource instanceof \MongoCursor) {
            $rowCount  = $resource->count($foundOnly = true);
        } elseif (is_array($resource) && isset($resource[ 'n' ])) {
            $rowCount = (int) $resource[ 'n' ];
//            $resource = $this -> connection -> getDB() -> selectCollection( $context ) -> find( ['_id' => $this -> connection -> getLastGeneratedValue( $context ) ] );
        } else {
            $rowCount = (int) $resource;
//            $resource = $this -> connection -> getDB() -> selectCollection( $context ) -> find( ['_id' => $this -> connection -> getLastGeneratedValue( $context ) ] );
        }

        $result->initialize($resource, 0, $rowCount);

        return $result;
    }

    /**
     * Returns an array of items for a page.
     *
     * @param  int   $offset           Page offset
     * @param  int   $itemCountPerPage Number of items per page
     * @return array
     */
    public function getItems($offset, $itemCountPerPage)
    {
        //      $cursor = clone $this -> cursor;
        $this->cursor->skip($offset);
        $this->cursor->limit($itemCountPerPage);

        $resultSet = clone $this->resultSetPrototype;
        $resultSet->initialize($this->createResult($this->cursor));

        return $resultSet;
    }

    /**
     * Returns the total number of rows in the result set.
     *
     * @return int
     */
    public function count()
    {
        if ($this->rowCount !== null) {
            return $this->rowCount;
        }

        $this->rowCount = $this->cursor->count($foundOnly        = true);

        return $this->rowCount;
    }
}
