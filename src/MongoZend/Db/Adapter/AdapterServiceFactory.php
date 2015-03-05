<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2015 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace MongoZend\Db\Adapter;

use Zend\Db\Adapter\AdapterServiceFactory as ZendAdapterServiceFactory;
use Zend\ServiceManager\ServiceLocatorInterface;

class AdapterServiceFactory extends ZendAdapterServiceFactory
{
    /**
     * Create db adapter service
     *
     * @param ServiceLocatorInterface $serviceLocator
     * @return Adapter
     */
    public function createService(ServiceLocatorInterface $serviceLocator)
    {
        $config = $serviceLocator->get('Config');
        return new Adapter($config['mongozend_db']);
    }

}
