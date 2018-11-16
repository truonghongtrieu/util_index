<?php

namespace go1\util_index\worker;

use Doctrine\DBAL\Connection;
use Exception;
use go1\clients\MqClient;
use go1\util\contract\ConsumerInterface;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexService;
use go1\util_index\task\TaskRepository;
use Pimple\Container;
use Psr\Log\LoggerInterface;
use stdClass;

class TaskConsumer implements ConsumerInterface
{
    private $db;
    private $container;
    private $mqClient;
    private $repository;
    private $history;
    private $logger;

    public function __construct(
        Connection $db,
        Container $container,
        MqClient $mqClient,
        TaskRepository $repository,
        HistoryRepository $history,
        LoggerInterface $logger
    ) {
        $this->db = $db;
        $this->container = $container;
        $this->mqClient = $mqClient;
        $this->repository = $repository;
        $this->history = $history;
        $this->logger = $logger;
    }

    public function aware(string $event): bool
    {
        return in_array($event, [IndexService::WORKER_TASK_PROCESS, IndexService::WORKER_TASK_BULK]);
    }

    public function consume(string $routingKey, stdClass $data, stdClass $context = null): bool
    {
        switch ($routingKey) {

            case IndexService::WORKER_TASK_PROCESS:
                $this->process($data);
                break;

            case IndexService::WORKER_TASK_BULK:
                $this->onBulkComplete($data, $context);
                break;
        }

        return true;
    }

    private function process(stdClass &$data)
    {
        if ($task = $this->repository->load($data->id)) {
            try {
                $handler = $this->repository->getHandler($task->currentHandler);
                if ($handler) {
                    $limit = isset($handler::$limit) ? $handler::$limit : $task->limit;

                    $task->offset = $data->currentOffset ?? 0;
                    $task->offset = $task->offset * $limit;
                    $task->offsetId = $data->currentIdFromOffset ?? 0;

                    $task->currentOffset = $data->offset ?? 0;
                    $task->currentIdFromOffset = $data->currentIdFromOffset ?? 0;
                    $task->limit = $limit;
                    $handler->handle($task);
                }
            } catch (Exception $e) {
                $this->history->write('task_process', $task->id, 500, ['message' => $e->getMessage(), 'data' => $data]);
            }
        }
    }

    private function onBulkComplete(stdClass &$data, stdClass $context)
    {
        if ($task = $this->repository->load($context->id)) {
            try {
                $task->failureItems += $data->failures ?? 0;
                $this->repository->verify($task);
            } catch (Exception $e) {
                $this->history->write('task_process', $task->id, 500, ['message' => $e->getMessage(), 'data' => $data]);
            }
        }
    }
}
