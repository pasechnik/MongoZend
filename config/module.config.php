<?php
return [
    'mongozend_db'      => [
        'driver'         => 'Mongo',
        'dsn'            => 'mongodb://127.0.0.1:27017',
        'dbname'         => 'album',
        'driver_options' => ['connect' => true],
        'adapters'       => [
            'wepo_mongo' => [
                'driver'         => 'Mongo',
                'gateway'        => 'MongoLite',
                'dsn'            => 'mongodb://127.0.0.1:27017',
                'dbname'         => 'wepo_company',
                'driver_options' => ['connect' => true],
            ],
//            'wepo_mysql' => [
//                'driver'         => 'Pdo',
//                'dsn'            => 'mysql:dbname=album;host=localhost',
//                'driver_options' => [
//                    PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES \'UTF8\''
//                ]
//            ],
        ]
    ],
    'service_manager' => [
        'abstract_factories' => [
            'MongoZend\Db\Adapter\Adapter' => 'MongoZend\Db\Adapter\AdapterAbstractServiceFactory',
        ],
        'factories'          => [
            'MongoZend\Db\Adapter\Adapter' => 'MongoZend\Db\Adapter\AdapterServiceFactory',
        ],
    ],
];
