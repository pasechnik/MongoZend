<?php

/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace MongoZend\Paginator\Adapter;

use MongoZend\Db\Adapter\Adapter;
use MongoZend\Db\NoSql\NoSql;
use MongoZend\Db\NoSql\Select;
use Zend\Db\ResultSet\ResultSet;
use Zend\Db\ResultSet\ResultSetInterface;

class DbSelect implements \Zend\Paginator\Adapter\AdapterInterface
{
    /**
     * @var NoSql
     */
    protected $sql = null;

    /**
     * Database query
     *
     * @var Select
     */
    protected $select = null;

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
     * @param  Select                             $select             The select query
     * @param  Adapter|NoSql                      $adapterOrSqlObject DB adapter or NoSql object
     * @param  null|ResultSetInterface            $resultSetPrototype
     * @throws Exception\InvalidArgumentException
     */
    public function __construct(Select $select, $adapterOrSqlObject, ResultSetInterface $resultSetPrototype = null)
    {
        $this->select = $select;

        if ($adapterOrSqlObject instanceof Adapter) {
            $adapterOrSqlObject = new NoSql($adapterOrSqlObject, null, new \MongoZend\Db\NoSql\Platform\Mongo\Mongo());
        }

        if (!$adapterOrSqlObject instanceof NoSql) {
            throw new Exception\InvalidArgumentException(
            '$adapterOrSqlObject must be an instance of MongoZend\Db\Adapter\Adapter or MongoZend\Db\NoSql\NoSql'
            );
        }

        $this->sql                = $adapterOrSqlObject;
        $this->resultSetPrototype = ($resultSetPrototype) ?: new ResultSet();
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
        $select = clone $this->select;
        $select->offset($offset);
        $select->limit($itemCountPerPage);

        $statement = $this->sql->prepareStatementForSqlObject($select);
        $result    = $statement->execute();

        $resultSet = clone $this->resultSetPrototype;
        $resultSet->initialize($result);

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

        $select = clone $this->select;
        $select->reset(Select::LIMIT);
        $select->reset(Select::OFFSET);
        $select->reset(Select::ORDER);

        $statement        = $this->sql->prepareStatementForSqlObject($select);
        $result           = $statement->execute();
        $this->rowCount = $result->count();

        return $this->rowCount;
    }
}
