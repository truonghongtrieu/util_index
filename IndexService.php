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
    const NAME    = SERVICE_NAME;
    const VERSION = Service::VERSION;

    // routingKeys
    const WORKER_TASK_PROCESS = REINDEX_TASK_PROCESS;
    const REINDEX_START       = REINDEX_ROUTING_KEY;
    const REINDEX_CLEANUP     = REINDEX_CLEANUP;

    public function __construct($values = [])
    {
        $serviceProviders = $values['serviceProviders'] ?? [];
        unset($values['serviceProviders']);

        parent::__construct($values);

        $this
            ->register(new UtilCoreServiceProvider)
            ->register(new UtilCoreClientServiceProvider)
            ->register(new UtilServiceProvider)
            ->register(new IndexServiceProvider)
            ->get('/', function () {
                return new JsonResponse(['service' => static::NAME, 'version' => static::VERSION, 'time' => time()]);
            });

        foreach ($serviceProviders as $serviceProvider) {
            $this->register($serviceProvider);
        }
    }

    public static function needMaster(): bool
    {
        $need = true;

        if ('POST' == ($_SERVER['REQUEST_METHOD'] ?? null)) {
            if (!empty($_SERVER['REQUEST_URI']) && strpos($_SERVER['REQUEST_URI'], '/consume') === 0) {
                $input = file_get_contents('php://input');
                $data = $input ? json_decode($input, true) : [];
                $routingKey = $data['routingKey'] ?? '';
                if (is_string($routingKey)) {
                    if (in_array($routingKey, [self::WORKER_TASK_PROCESS])) {
                        $need = false;
                    }
                }
            }
        }

        return $need;
    }
}
