<?php

namespace go1\util_index;

use go1\util\consume\ConsumeController;
use go1\util_index\controller\InstallController;
use go1\util_index\task\TaskRepository;
use Pimple\Container;
use Pimple\ServiceProviderInterface;
use Silex\Api\BootableProviderInterface;
use Silex\Application;

class IndexServiceProvider implements ServiceProviderInterface, BootableProviderInterface
{
    public function register(Container $c)
    {
        $c['ctrl.install'] = function (Container $c) {
            return new InstallController($c['dbs']['default'], $c['dbs']['go1_write'], $c['go1.client.es'], $c['go1.client.mq']);
        };

        $c['ctrl.consumer'] = function (Container $c) {
            return new ConsumeController($c['consumers'], $c['logger'], $c['access_checker']);
        };

        $c['history.repository'] = function (Container $c) {
            return new HistoryRepository($c['dbs']['default']);
        };

        $c['consumer.lo.arguments'] = function (Container $c) {
            return [
                $c['go1.client.es'],
                $c['history.repository'],
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['accounts_name'],
                $c['formatter.lo'],
                $c['formatter.user'],
                $c['waitForCompletion'],
                $c['repository.es'],
                $c['go1.client.mq'],
                $c['portal_checker'],
            ];
        };

        $c['task.repository'] = function (Container $c) {
            return new TaskRepository(
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['go1.client.mq'],
                $c['go1.client.es'],
                $c,
                $c['logger']
            );
        };

        $c['repository.es'] = function (Container $c) {
            return new ElasticSearchRepository(
                $c['go1.client.es'],
                $c['waitForCompletion'],
                $c['go1.client.mq'],
                $c['history.repository']
            );
        };

        $c['middleware.consume'] = function (Container $c) {
            return new ConsumeMiddleware(
                $c['dbs']['go1_write'],
                $c['repository.es'],
                $c['logger'],
                $c['portal_checker'],
                $c['go1.client.es']
            );
        };
    }

    public function boot(Application $app)
    {
        $app->post('/install', 'ctrl.install:post');
    }
}
