<?php

namespace go1\util_index\core\consumer;

use Exception;
use go1\util\contract\ServiceConsumerInterface;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexService;
use go1\util_index\task\TaskRepository;
use stdClass;

class TaskConsumer implements ServiceConsumerInterface
{
    private $repository;
    private $history;

    public function __construct(TaskRepository $repository, HistoryRepository $history)
    {
        $this->repository = $repository;
        $this->history = $history;
    }

    public function aware(): array
    {
        return [
            IndexService::WORKER_TASK_PROCESS => 'TODO: description',
            IndexService::WORKER_TASK_BULK    => 'TODO: description',
        ];
    }

    public function consume(string $routingKey, stdClass $data, stdClass $context = null)
    {
        switch ($routingKey) {
            case IndexService::WORKER_TASK_PROCESS:
                $this->process($data);
                break;

            case IndexService::WORKER_TASK_BULK:
                $this->onBulkComplete($data, $context);
                break;
        }
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
