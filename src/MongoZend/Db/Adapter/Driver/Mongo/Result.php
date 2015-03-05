<?php

/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace MongoZend\Db\Adapter\Driver\Mongo;

use Iterator;
use Zend\Db\Adapter\Driver\ResultInterface;
use Zend\Db\Adapter\Exception;

class Result implements Iterator, ResultInterface
{
    const STATEMENT_MODE_SCROLLABLE = 'scrollable';
    const STATEMENT_MODE_FORWARD    = 'forward';

    /**
     *
     * @var string
     */
    protected $statementMode = self::STATEMENT_MODE_SCROLLABLE;

    /**
     * @var \MongoCursor
     */
    protected $resource = null;

    /**
     * @var array Result options
     */
    protected $options;

    /**
     * Is the current complete?
     * @var bool
     */
    protected $currentComplete = false;

    /**
     * Track current item in recordset
     * @var mixed
     */
    protected $currentData = null;

    /**
     * Current position of scrollable statement
     * @var int
     */
    protected $position = -1;

    /**
     * @var mixed
     */
    protected $generatedValue = null;

    /**
     * @var null
     */
    protected $rowCount = null;

    /**
     * @var null
     */
    protected $fieldCount = null;

    /**
     * Initialize
     *
     * @param  \MongoCursor $resource
     * @param               $generatedValue
     * @param  int          $rowCount
     * @return Result
     */
    public function initialize($resource, $generatedValue, $rowCount = null)
    {
        $this->resource       = $resource;
        $this->generatedValue = $generatedValue;
        $this->rowCount       = $rowCount;
        if ($resource instanceof \Iterator) {
            $this->resource->rewind();
        }

        return $this;
    }

    /**
     * @return null
     */
    public function buffer()
    {
        return;
    }

    /**
     * @return bool|null
     */
    public function isBuffered()
    {
        return false;
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
     * Get the data
     * @return array
     */
    public function current()
    {
        if ($this->currentComplete) {
            return $this->currentData;
        }

        $this->currentData = $this->resource->current();

        return $this->currentData;
    }

    /**
     * Next
     *
     * @return mixed
     */
    public function next()
    {
        $this->resource->next();
        $this->currentData     = $this->resource->current();
        $this->currentComplete = true;
        $this->position++;

        return $this->currentData;
    }

    /**
     * Key
     *
     * @return mixed
     */
    public function key()
    {
        return $this->position;
    }

    /**
     * @throws Exception\RuntimeException
     * @return void
     */
    public function rewind()
    {
        //        if ( $this -> statementMode == self::STATEMENT_MODE_FORWARD && $this -> position > 0 )
//        {
//            throw new Exception\RuntimeException(
//            'This result is a forward only result set, calling rewind() after moving forward is not supported'
//            );
//        }
        $this->resource->rewind();
        $this->currentData     = $this->resource->current();
        $this->currentComplete = true;
        $this->position        = 0;
    }

    /**
     * Valid
     *
     * @return bool
     */
    public function valid()
    {
        return ($this->currentData !== null);
    }

    /**
     * Count
     *
     * @return int
     */
    public function count()
    {
        if (is_int($this->rowCount)) {
            return $this->rowCount;
        }
        if ($this->rowCount instanceof \Closure) {
            $this->rowCount = (int) call_user_func($this->rowCount);
        } else {
            $this->rowCount = (int) $this->resource->count($foundOnly        = true);
        }

        return $this->rowCount;
    }

    /**
     * @return integer
     */
    public function setFieldCount($fieldcount)
    {
        $this->fieldCount = $fieldcount;

        return $this;
    }

    /**
     * @return int
     */
    public function getFieldCount()
    {
        return $this->fieldCount;
    }

    /**
     * Is query result
     *
     * @return bool
     */
    public function isQueryResult()
    {
        return ($this->resource instanceof \MongoCursor);
    }

    /**
     * Get affected rows
     *
     * @return int
     */
    public function getAffectedRows()
    {
        if ($this->resource instanceof \MongoCursor) {
            return $this->resource->count();
        }

        return $this->resource;
    }

    /**
     * @return mixed|null
     */
    public function getGeneratedValue()
    {
        return $this->generatedValue;
    }

    public function skip($offset)
    {
        if ($offset > 0 && $this->resource instanceof \MongoCursor) {
            return $this->resource->skip($offset);
        }

        return $this->resource;
    }

    public function limit($itemCountPerPage)
    {
        if ($this->resource instanceof \MongoCursor) {
            return $this->resource->limit($itemCountPerPage);
        }

        return $this->resource;
    }
}
