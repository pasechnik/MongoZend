<?php
/**
 * Created by PhpStorm.
 * User: vlad
 * Date: 05.03.15
 * Time: 11:42
 */
namespace Album\Model;

use Zend\InputFilter\InputFilter;
use Zend\InputFilter\InputFilterAwareInterface;
use Zend\InputFilter\InputFilterInterface;

class Album implements InputFilterAwareInterface
{

    public $_id;
    public $artist;
    public $title;

    protected $inputFilter = null;

    public function exchangeArray($data)
    {
        $this->_id     = ( !empty($data['_id'])) ? ''.$data['_id'] : null;
        $this->artist = ( !empty($data['artist'])) ? $data['artist'] : null;
        $this->title  = ( !empty($data['title'])) ? $data['title'] : null;
    }

    // Add the following method:
    public function getArrayCopy()
    {
        $a = get_object_vars($this);
        return get_object_vars($this);
    }

    // Add content to these methods:
    public function setInputFilter(InputFilterInterface $inputFilter)
    {
        throw new \Exception("Not used");
    }

    public function getInputFilter()
    {
        if ( !$this->inputFilter) {
            $inputFilter = new InputFilter();

            $inputFilter->add([
                'name'     => '_id',
                'required' => true,
                'filters'  => [
                    ['name' => 'StripTags'],
                    ['name' => 'StringTrim'],
                ],
            ]);

            $inputFilter->add([
                'name'       => 'artist',
                'required'   => true,
                'filters'    => [
                    ['name' => 'StripTags'],
                    ['name' => 'StringTrim'],
                ],
                'validators' => [
                    [
                        'name'    => 'StringLength',
                        'options' => [
                            'encoding' => 'UTF-8',
                            'min'      => 1,
                            'max'      => 100,
                        ],
                    ],
                ],
            ]);

            $inputFilter->add([
                'name'       => 'title',
                'required'   => true,
                'filters'    => [
                    ['name' => 'StripTags'],
                    ['name' => 'StringTrim'],
                ],
                'validators' => [
                    [
                        'name'    => 'StringLength',
                        'options' => [
                            'encoding' => 'UTF-8',
                            'min'      => 1,
                            'max'      => 100,
                        ],
                    ],
                ],
            ]);

            $this->inputFilter = $inputFilter;
        }

        return $this->inputFilter;
    }
}
