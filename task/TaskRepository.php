<?php

namespace go1\util_index\task;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Exception;
use go1\clients\MqClient;
use go1\core\internal\index\MetricReindex;
use go1\core\learning_record\commerce\index\CouponReindex;
use go1\core\learning_record\commerce\index\CreditReindex;
use go1\core\learning_record\commerce\index\PaymentTransactionReindex;
use go1\core\learning_record\enrolment\index\EnrolmentReindex;
use go1\core\learning_record\enrolment\index\EnrolmentRevisionReindex;
use go1\core\learning_record\enrolment\index\EnrolmentShareReindex;
use go1\core\learning_record\enrolment\index\EnrolmentVirtualReindex;
use go1\core\learning_record\enrolment\index\PlanReindex;
use go1\core\lo\award\index\AwardAccountEnrolmentReindex;
use go1\core\lo\award\index\AwardEnrolmentReindex;
use go1\core\lo\award\index\AwardEnrolmentRevisionReindex;
use go1\core\lo\award\index\AwardItemEnrolmentReindex;
use go1\core\lo\award\index\AwardItemManualReindex;
use go1\core\lo\award\index\AwardItemReindex;
use go1\core\lo\award\index\AwardManualEnrolmentReindex;
use go1\core\lo\award\index\AwardReindex;
use go1\core\lo\award\index\SuggestionCategoryReindex;
use go1\core\lo\event_li\index\EventAttendanceReindex;
use go1\core\lo\event_li\index\EventReindex;
use go1\core\lo\event_li\index\EventSessionReindex;
use go1\core\lo\index\LoContentSharingReindex;
use go1\core\lo\index\LoGroupReindex;
use go1\core\lo\index\LoLocationReindex;
use go1\core\lo\index\LoPolicyReindex;
use go1\core\lo\index\LoReindex;
use go1\core\lo\index\LoShareReindex;
use go1\core\lo\quiz_li\index\QuizUserAnswerReindex;
use go1\core\lo\social_li\index\GroupReindex;
use go1\index\domain\reindex\handler\AccountEnrolmentReindex;
use go1\index\domain\reindex\handler\AccountReindex;
use go1\index\domain\reindex\handler\ContractReindex;
use go1\index\domain\reindex\handler\EckDataReindex;
use go1\index\domain\reindex\handler\EckMetadataReindex;
use go1\index\domain\reindex\handler\ManualRecordReindex;
use go1\index\domain\reindex\handler\PortalConfigReindex;
use go1\index\domain\reindex\handler\PortalReindex;
use go1\index\domain\reindex\handler\UserReindex;
use go1\index\products\IndexService;
use go1\util\DB;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use go1\util_index\ReindexInterface;
use ONGR\ElasticsearchDSL\BuilderInterface;
use Pimple\Container;
use Psr\Log\LoggerInterface;

class TaskRepository
{
    const SEARCH_SLOW_TIME_WARN = '5s';
    const SEARCH_SLOW_TIME_INFO = '2s';
    const INDEX_SLOW_TIME_WARN  = '10s';
    const INDEX_SLOW_TIME_INFO  = '2s';

    const HANDLERS = [
        UserReindex::NAME,
        AccountReindex::NAME,
        PortalReindex::NAME,
        PortalConfigReindex::NAME,
        LoReindex::NAME,
        LoShareReindex::NAME,
        LoContentSharingReindex::NAME,
        LoGroupReindex::NAME,
        LoLocationReindex::NAME,
        EnrolmentReindex::NAME,
        EnrolmentShareReindex::NAME,
        EnrolmentVirtualReindex::NAME,
        EnrolmentRevisionReindex::NAME,
        AccountEnrolmentReindex::NAME,
        GroupReindex::NAME,
        EckDataReindex::NAME,
        PaymentTransactionReindex::NAME,
        QuizUserAnswerReindex::NAME,
        EckMetadataReindex::NAME,
        ManualRecordReindex::NAME,
        CouponReindex::NAME,
        CreditReindex::NAME,
        EventReindex::NAME,
        EventSessionReindex::NAME,
        EventAttendanceReindex::NAME,
        AwardReindex::NAME,
        AwardItemReindex::NAME,
        AwardItemManualReindex::NAME,
        AwardItemEnrolmentReindex::NAME,
        AwardEnrolmentReindex::NAME,
        AwardEnrolmentRevisionReindex::NAME,
        AwardManualEnrolmentReindex::NAME,
        AwardAccountEnrolmentReindex::NAME,
        SuggestionCategoryReindex::NAME,
        PlanReindex::NAME,
        ContractReindex::NAME,
        MetricReindex::NAME,
        LoPolicyReindex::NAME,
    ];

    public  $db;
    public  $go1;
    public  $mqClient;
    public  $client;
    private $container;
    private $logger;

    public function __construct(
        Connection $db,
        Connection $go1,
        MqClient $mqClient,
        Client $client,
        Container $container,
        LoggerInterface $logger
    )
    {
        $this->db = $db;
        $this->go1 = $go1;
        $this->mqClient = $mqClient;
        $this->client = $client;
        $this->container = $container;
        $this->logger = $logger;
    }

    public function create(Task $task)
    {
        $this->db->insert('index_task', $task->jsonSerialize());
        $task->id = $this->db->lastInsertId('index_task');

        return $task->id;
    }

    public function update(Task $task)
    {
        $task->updated = time();
        $this->db->update('index_task', $task->jsonSerialize(), ['id' => $task->id]);

        return true;
    }

    public function delete(Task $task)
    {
        return DB::transactional($this->db, function () use ($task) {
            $this->db->delete('index_task', ['id' => $task->id]);

            return true;
        });
    }

    public function load(int $id)
    {
        $data = $this->db
            ->executeQuery('SELECT * FROM index_task WHERE id = ?', [$id])
            ->fetch(DB::OBJ);

        if ($data) {
            $task = Task::create($data);
            $task->instance && $task->instance = PortalHelper::load($this->go1, $task->instance);

            return $task;
        }

        return false;
    }

    private function getAlias(Task $task)
    {
        try {
            $alias = $this->client->indices()->getAlias(['name' => $task->aliasName]);
            $alias = $alias ? array_keys($alias) : [];

            return $alias[0] ?? null;
        } catch (Exception $e) {
        }

        return null;
    }

    public function finish(Task $task)
    {
        $this->client->indices()->refresh(['index' => Schema::INDEX]);
        if ($task->alias && ($task->aliasName != $task->index)) {
            if (Schema::INDEX == $task->aliasName) {
                $activeIndex = $this->getAlias($task) ?: Schema::INDEX;
                foreach ($this->client->indices()->getAliases() as $index => $aliases) {
                    if ($index === $activeIndex) {
                        foreach ($aliases['aliases'] as $name => $conf) {
                            $params['body']['actions'][]['add'] = $conf + ['index' => $task->index, 'alias' => $name];
                        }
                    }
                }
            }

            $params['body']['actions'][]['add'] = [
                'index' => $task->index,
                'alias' => $task->aliasName,
            ];
            $params['body']['actions'][]['remove_index'] = [
                'index' => $activeIndex ?? $task->aliasName,
            ];
            $this->client->indices()->updateAliases($params);
            $this->client->indices()->putSettings([
                'index' => $task->index,
                'body'  => [
                    'settings' => [
                        'number_of_replicas'                          => 1,
                        'index.search.slowlog.threshold.query.warn'   => self::SEARCH_SLOW_TIME_WARN,
                        'index.search.slowlog.threshold.query.info'   => self::SEARCH_SLOW_TIME_INFO,
                        'index.indexing.slowlog.threshold.index.warn' => self::INDEX_SLOW_TIME_WARN,
                        'index.indexing.slowlog.threshold.index.info' => self::INDEX_SLOW_TIME_INFO,
                    ],
                ],
            ]);
        }

        $task->status = Task::FINISHED;
        $task->percent = 100;
        $this->update($task);

        $task->removeRedundant && $this->removeRedundant($task);
    }

    public function removeRedundant(Task $task)
    {
        foreach ($task->handlers as $name) {
            $handler = $this->getHandler($name);
            if (method_exists($handler, 'removeRedundant')) {
                /** @var $query BuilderInterface */
                $query = $handler->removeRedundant($task);
                if ($query->toArray()) {
                    $this->client->deleteByQuery([
                        'index'               => Schema::INDEX,
                        'body'                => ['query' => $query->toArray()],
                        'refresh'             => true,
                        'wait_for_completion' => true,
                    ]);
                }
            }
        }
    }

    public function execute(Task $task)
    {
        $settings = [
            'settings' => [
                'number_of_shards'                 => 2,
                'number_of_replicas'               => 0,
                'index.mapping.total_fields.limit' => 5000,
            ],
        ];

        if (!$this->client->indices()->exists(['index' => $task->index])) {
            $this->client->indices()->create([
                'index' => $task->index,
                'body'  => Schema::BODY + $settings,
            ]);
        }

        if (!$this->client->indices()->exists(['index' => $task->aliasName])) {
            $this->client->indices()->create([
                'index' => $task->aliasName,
                'body'  => Schema::BODY + $settings,
            ]);
        }

        $task->stats = $this->stats($task);
        foreach ($task->stats as $taskName => $num) {
            $handler = $this->getHandler($taskName);
            $limit = isset($handler::$limit) ? $handler::$limit : $task->limit;
            $task->stats[$taskName] = ceil($num / $limit);
        }

        $task->totalItems = array_sum($task->stats);
        $task->status = Task::IN_PROGRESS;
        $this->update($task);

        $this->verify($task);
    }

    private function generateItems(Task $task)
    {
        $handler = $this->getHandler($task->currentHandler);
        $items = [];
        for ($i = 0; $i < $task->maxNumItems; ++$i) {
            if ($task->currentOffset < $task->stats[$task->currentHandler]) {
                $idFromOffset = 0;
                if ($task->currentOffset > 0) {
                    $idFromOffset = method_exists($handler, 'offsetToId') ? $handler->offsetToId($task, $task->currentIdFromOffset) : 0;
                }

                $items[] = [
                    'routingKey' => IndexService::WORKER_TASK_PROCESS,
                    'body'       => $body = [
                        'handler'             => $task->currentHandler,
                        'id'                  => $task->id,
                        'currentOffset'       => $task->currentOffset,
                        'currentIdFromOffset' => $idFromOffset,
                    ],
                ];
                $task->currentIdFromOffset = $idFromOffset;
            }
            ++$task->currentOffset;
        }
        $this->update($task);
        $this->mqClient->queue($items, IndexService::WORKER_TASK_BULK, ['id' => $task->id]);
    }

    private function handlerIsCompleted(Task $task, string $handler)
    {
        return (0 == $task->stats[$handler]) || ($task->currentOffset >= $task->stats[$handler]);
    }

    private function calculatePercent(Task $task)
    {
        return ($task->processedItems / $task->totalItems) * 100;
    }

    public function verify(Task $task)
    {
        if ($this->handlerIsCompleted($task, $task->currentHandler)) {
            # Find a next handler.
            $task->processedItems += $task->stats[$task->currentHandler];
            $task->currentHandler = $task->nextHandler();
            while ($task->currentHandler && (0 == $task->stats[$task->currentHandler])) {
                $task->currentHandler = $task->nextHandler();
            }

            if (!$task->currentHandler) {
                return $this->finish($task);
            }

            $task->currentOffset = 0;
            $task->currentOffset = 0;
            $task->currentIdFromOffset = 0;
            $task->percent = $this->calculatePercent($task);
            $this->update($task);
        }

        $this->generateItems($task);
    }

    /**
     * @param string $name
     * @return ReindexInterface|null
     */
    public function getHandler(string $name)
    {
        $handlerName = "reindex.handler.$name";
        if ($this->container->offsetExists($handlerName)) {
            return $this->container[$handlerName];
        }

        return null;
    }

    public function stats(Task $task)
    {
        $stats = [];
        foreach ($task->handlers as $name) {
            if ($handler = $this->getHandler($name)) {
                $stats[$name] = (int) $handler->count($task);
            }
        }

        return $stats;
    }

    public function hash(Task $task)
    {
        $hash = [];
        foreach ($task->handlers as $name) {
            if ($handler = $this->getHandler($name)) {
                $hash[$name] = $handler->hash($task);
            }
        }

        return $hash;
    }
}
