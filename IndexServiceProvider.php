<?php

namespace go1\util_index;

use go1\util\contract\ServiceConsumeController;
use go1\util_index\controller\InstallController;
use go1\util_index\task\TaskRepository;
use Pimple\Container;
use Pimple\ServiceProviderInterface;
use Silex\Api\BootableProviderInterface;
use Silex\Application;
use Symfony\Component\HttpFoundation\Request;

class IndexServiceProvider implements ServiceProviderInterface, BootableProviderInterface
{
    public function register(Container $c)
    {
        $c['ctrl.install'] = function (Container $c) {
            return new InstallController($c['dbs']['default'], $c['dbs']['go1_write'], $c['go1.client.es'], $c['go1.client.mq']);
        };

        $c['ctrl.consumer'] = function (Container $c) {
            return new ServiceConsumeController($c['consumers'], $c['logger']);
        };

        $c['history.repository'] = function (Container $c) {
            return new HistoryRepository($c['dbs']['default']);
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
        $app->get('/consume', 'ctrl.consumer:getConsumersInfo');
        $app->post('/consume', 'ctrl.consumer:post')
            ->before(function (Request $req) use ($app) {
                return $app['middleware.consume']($req);
            });
    }
}
