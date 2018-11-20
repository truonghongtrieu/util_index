<?php

namespace go1\util_index\core;

use go1\util\location\LocationRepository;
use Pimple\Container;
use Pimple\ServiceProviderInterface;
use Silex\Api\BootableProviderInterface;
use Silex\Application;

class IndexCoreServiceProvider implements ServiceProviderInterface, BootableProviderInterface
{
    public function register(Container $c)
    {
        $c['location.repository'] = function (Container $c) {
            return new LocationRepository($c['dbs']['go1_write'], $c['go1.client.mq']);
        };

        $c['formatter.eck_data'] = function (Container $c) {
            return new AccountFieldFormatter($c['dbs']['go1'], $c['dbs']['eck']);
        };

        $c['formatter.user'] = function (Container $c) {
            return new UserFormatter(
                $c['dbs']['go1'],
                $c['dbs']['social'] ?? null,
                $c['dbs']['eck'] ?? null,
                $c['accounts_name'],
                $c['formatter.eck_data']
            );
        };

        $c['formatter.lo'] = function (Container $c) {
            return new LoFormatter(
                $c['dbs']['go1'],
                $c['dbs']['social'] ?? null,
                $c['dbs']['award'] ?? null,
                $c['dbs']['vote'] ?? null,
                $c['dbs']['quiz'] ?? null,
                $c['dbs']['policy'] ?? null,
                $c['dbs']['collection'] ?? null,
                $c['accounts_name'],
                $c['formatter.user'],
                $c['formatter.event'] ?? null,
                $c['portal_checker'],
                $c['location.repository']
            );
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

        $c['ctrl.task'] = function (Container $c) {
            return new TaskController(
                $c['env'],
                $c['dbs']['go1_write'],
                $c['dbs']['default'],
                $c['task.repository'],
                $c['access_checker'],
                $c,
                $c['client'],
                $c['go1.client.es'],
                $c['go1.client.mq']
            );
        };
    }

    public function boot(Application $app)
    {
        $app->get('/reindex/handlers', 'ctrl.handlers:get');
        $app->post('/task', 'ctrl.task:post');
        $app->get('/task/{id}/verify', 'ctrl.task:verify');
        $app->post('/task/{id}/execute', 'ctrl.task:execute');
        $app->delete('/task/{id}', 'ctrl.task:delete');
    }
}
