<?php

namespace go1\util_index;

class IndexService
{
    // routingKeys
    const WORKER_TASK_PROCESS = REINDEX_TASK_PROCESS;
    const REINDEX_START       = REINDEX_ROUTING_KEY;
    const REINDEX_CLEANUP     = REINDEX_CLEANUP;

    public static function needMaster(): bool
    {
        if ('POST' == ($_SERVER['REQUEST_METHOD'] ?? null)) {
            if (!empty($_SERVER['REQUEST_URI']) && strpos($_SERVER['REQUEST_URI'], '/consume') === 0) {
                $input = file_get_contents('php://input');
                $data = $input ? json_decode($input, true) : [];
                $routingKey = $data['routingKey'] ?? '';
                if (is_string($routingKey)) {
                    if (in_array($routingKey, [self::WORKER_TASK_PROCESS])) {
                        return false;
                    }
                }
            }
        }

        return true;
    }
}
