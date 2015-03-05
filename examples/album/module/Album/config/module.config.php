<?php
/**
 * Created by PhpStorm.
 * User: vlad
 * Date: 05.03.15
 * Time: 11:34
 */
return [
    'controllers'  => [
        'invokables' => [
            'Album' => 'Album\Controller\AlbumController',
        ],
    ],
    'router'       => [
        'routes' => [
            'album' => [
                'type'    => 'segment',
                'options' => [
                    'route'       => '/album[/:action][/:id]',
                    'constraints' => [
                        'action' => '[a-zA-Z][a-zA-Z0-9_-]*',
                        'id'     => '[a-zA-Z0-9]+',
                    ],
                    'defaults'    => [
                        'controller' => 'Album',
                        'action'     => 'index',
                    ],
                ],
            ],
        ],
    ],
    'view_manager' => [
        'template_path_stack' => [
            'album' => __DIR__ . '/../view',
        ],
    ],
];
