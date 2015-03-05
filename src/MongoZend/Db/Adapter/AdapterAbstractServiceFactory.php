<?php

namespace MongoZend\Db\Adapter;

use Zend\ServiceManager\ServiceLocatorInterface;
use Zend\Db\Adapter\AdapterAbstractServiceFactory as ZendAdapterAbstractServiceFactory;

class AdapterAbstractServiceFactory extends ZendAdapterAbstractServiceFactory
{

    /**
     * Create a DB adapter
     *
     * @param  ServiceLocatorInterface $services
     * @param  string                  $name
     * @param  string                  $requestedName
     *
     * @return Adapter
     */
    public function createServiceWithName(
        ServiceLocatorInterface $services,
        $name,
        $requestedName
    ) {
        $config = $this->getConfig($services);

        return new Adapter($config[$requestedName]);
    }

    /**
     * Get db configuration, if any
     *
     * @param  ServiceLocatorInterface $services
     * @return array
     */
    protected function getConfig(ServiceLocatorInterface $services)
    {
        if ($this->config !== null) {
            return $this->config;
        }

        if (!$services->has('Config')) {
            $this->config = array();
            return $this->config;
        }

        $config = $services->get('Config');
        if (!isset($config['mongozend_db'])
            || !is_array($config['mongozend_db'])
        ) {
            $this->config = array();
            return $this->config;
        }

        $config = $config['mongozend_db'];
        if (!isset($config['adapters'])
            || !is_array($config['adapters'])
        ) {
            $this->config = array();
            return $this->config;
        }

        $this->config = $config['adapters'];
        return $this->config;
    }

}
