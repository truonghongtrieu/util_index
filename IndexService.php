<?php

namespace go1\util_index;

use go1\app\App as GO1;
use go1\clients\UtilCoreClientServiceProvider;
use go1\util\Service;
use go1\util\UtilCoreServiceProvider;
use go1\util\UtilServiceProvider;
use Symfony\Component\HttpFoundation\JsonResponse;

class IndexService extends GO1
{
    const NAME    = 'index';
    const VERSION = Service::VERSION;

    const WORKER_TASK_PROCESS  = 'worker.index.task.process';
    const WORKER_TASK_BULK     = 'worker.index.task.bulk';
    const WORKER_MESSAGE_RETRY = 'worker.index.message.retry';

    public function __construct($values = [])
    {
        parent::__construct($values);

        $serviceProviders = $values['serviceProviders'] ?? [];
        unset($values['serviceProviders']);

        $this
            ->register(new UtilCoreServiceProvider)
            ->register(new UtilCoreClientServiceProvider)
            ->register(new UtilServiceProvider)
            ->register(new UtilIndexServiceProvider)
            ->get('/', function () {
                return new JsonResponse(['service' => static::NAME, 'version' => static::VERSION, 'time' => time()]);
            });

        foreach ($serviceProviders as $serviceProvider) {
            $this->register($serviceProvider);
        }
    }
}
