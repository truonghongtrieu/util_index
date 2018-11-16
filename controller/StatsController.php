<?php

namespace go1\index\controller;

use go1\util\AccessChecker;
use go1\util\Error;
use go1\util_index\task\Task;
use go1\util_index\task\TaskRepository;
use Pimple\Container;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class StatsController
{
    private $container;
    private $taskRepository;
    private $accessChecker;

    public function __construct(
        Container $container,
        TaskRepository $taskRepository,
        AccessChecker $accessChecker
    )
    {
        $this->container = $container;
        $this->taskRepository = $taskRepository;
        $this->accessChecker = $accessChecker;
    }

    public function get(Request $req)
    {
        if (!$this->accessChecker->isAccountsAdmin($req)) {
            return Error::simpleErrorJsonResponse('Internal resource', 403);
        }

        $task = Task::create((object) ['data' => ['handlers' => TaskRepository::HANDLERS]]);

        return new JsonResponse($this->taskRepository->stats($task));
    }
}
