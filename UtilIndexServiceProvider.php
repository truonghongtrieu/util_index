<?php

namespace go1\util_index;

use go1\util\consume\ConsumeController;
use go1\util\location\LocationRepository;
use go1\util_index\controller\InstallController;
use go1\util_index\controller\ReindexHandlersController;
use go1\util_index\controller\StatsController;
use go1\util_index\controller\TaskController;
use go1\util_index\task\TaskRepository;
use go1\util_index\worker\InstallConsumer;
use go1\util_index\worker\TaskConsumer;
use Pimple\Container;
use Pimple\ServiceProviderInterface;
use Silex\Api\BootableProviderInterface;
use Silex\Application;
use Symfony\Component\HttpFoundation\Request;

class UtilIndexServiceProvider implements ServiceProviderInterface, BootableProviderInterface
{
    const INDEX_INSTALL_PORTAL = 'worker.index.install-portal';

    public function register(Container $c)
    {
        $c['ctrl.install'] = function (Container $c) {
            return new InstallController(
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['go1.client.es'],
                $c['go1.client.mq']
            );
        };

        $c['ctrl.consumer'] = function (Container $c) {
            return new ConsumeController($c['consumers'], $c['logger'], $c['access_checker']);
        };

        $c['history.repository'] = function (Container $c) {
            return new HistoryRepository($c['dbs']['default']);
        };

        $c['location.repository'] = function (Container $c) {
            return new LocationRepository($c['dbs']['go1_write'], $c['go1.client.mq']);
        };

        $c['consumers'] = function (Container $c) {
            return [
                $c['consumer.portal'],
                $c['consumer.configuration'],
                $c['consumer.user'],
                $c['consumer.lo'],
                $c['consumer.lo_collection'],
                $c['consumer.lo_share'],
                $c['consumer.lo_content_sharing'],
                $c['consumer.lo.location'],
                $c['consumer.lo_customisation'],
                $c['consumer.lo_policy'],
                $c['consumer.plan'],
                $c['consumer.plan.enrolment-virtual'],
                $c['consumer.enrolment'],
                $c['consumer.enrolment.virtual'],
                $c['consumer.module-enrolment-progress'],
                $c['consumer.enrolment_revision'],
                $c['consumer.account_enrolment'],
                $c['consumer.assessor'],
                $c['consumer.author'],
                $c['consumer.eck-data'],
                $c['consumer.task'],
                $c['consumer.manual_record'],
                $c['consumer.group'],
                $c['consumer.group.item'],
                $c['consumer.payment_transaction'],
                $c['consumer.quiz_user_answer'],
                $c['consumer.coupon'],
                $c['consumer.credit'],
                $c['consumer.event'],
                $c['consumer.event_session'],
                $c['consumer.event_attendance'],
                $c['consumer.manager'],
                $c['consumer.award'],
                $c['consumer.award.item'],
                $c['consumer.award.item-manual'],
                $c['consumer.award.item.enrolment'],
                $c['consumer.award.enrolment'],
                $c['consumer.award.enrolment_revision'],
                $c['consumer.award.manual_enrolment'],
                $c['consumer.award.assessor'],
                $c['consumer.award.account_enrolment'],
                $c['consumer.suggestion.category'],
                $c['consumer.install'],
                $c['consumer.remove_redundant_data'],
                $c['consumer.contract'],
                $c['consumer.metric'],
                $c['consumer.lazy'],
                $c['consumer.quiz.progress'],
            ];
        };

        $c['consumer.task'] = function (Container $c) {
            return new TaskConsumer(
                $c['dbs']['default'],
                $c,
                $c['go1.client.mq'],
                $c['task.repository'],
                $c['history.repository'],
                $c['logger']
            );
        };

        $c['consumer.install'] = function (Container $c) {
            return new InstallConsumer($c['dbs']['go1_write'], $c['repository.es'], $c['portal_checker']);
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

        $c['repository.es'] = function (Container $c) {
            return new ElasticSearchRepository($c['go1.client.es'], $c['waitForCompletion'], $c['go1.client.mq'], $c['history.repository']);
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

        $c['consumer.remove_redundant_data'] = function (Container $c) {
            return new ReindexDataCleanupConsumer(
                $c['dbs']['go1_write'],
                $c['go1.client.es'],
                $c['history.repository'],
                $c['logger'],
                $c['waitForCompletion']
            );
        };

        $c['ctrl.stats'] = function (Container $c) {
            return new StatsController($c, $c['task.repository'], $c['access_checker']);
        };

        $c['ctrl.handlers'] = function (Container $c) {
            return new ReindexHandlersController($c);
        };
    }

    public function boot(Application $app)
    {
        $app->post('/install', 'ctrl.install:post');
        $app->post('/consume', 'ctrl.consumer:post')
            ->before(function (Request $req) use ($app) { return $app['middleware.consume']($req); });
        $app->get('/stats', 'ctrl.stats:get');
        $app->get('/verify/{portalId}', 'ctrl.portal:verify');

        # ---------------------
        # Re-index
        # ---------------------
        $app->get('/reindex/handlers', 'ctrl.handlers:get');
        $app->post('/task', 'ctrl.task:post');
        $app->get('/task/{id}/verify', 'ctrl.task:verify');
        $app->post('/task/{id}/execute', 'ctrl.task:execute');
        $app->delete('/task/{id}', 'ctrl.task:delete');
    }
}
