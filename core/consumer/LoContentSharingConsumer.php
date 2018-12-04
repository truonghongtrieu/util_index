<?php

namespace go1\util_index\core\consumer;

use Aws\ElasticsearchService\Exception\ElasticsearchServiceException;
use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\core\lo\index\LearningObjectIndexServiceProvider;
use go1\util\es\Schema;
use go1\util\group\GroupHelper;
use go1\util\group\GroupItemStatus;
use go1\util\group\GroupItemTypes;
use go1\util\lo\LoHelper;
use go1\util\portal\PortalChecker;
use go1\util\queue\Queue;
use go1\util_index\core\LoFormatter;
use go1\util_index\core\UserFormatter;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\HistoryRepository;
use Ramsey\Uuid\Uuid;
use stdClass;

class LoContentSharingConsumer extends LoShareConsumer
{
    public static $limit = 200;

    protected $social;

    public function __construct(
        Client $client,
        HistoryRepository $history,
        Connection $db,
        Connection $go1,
        string $accountsName,
        LoFormatter $formatter,
        UserFormatter $userFormatter,
        bool $waitForCompletion,
        ElasticSearchRepository $repository,
        MqClient $queue,
        PortalChecker $portalChecker,
        Connection $social
    ) {
        parent::__construct(
            $client,
            $history,
            $db,
            $go1,
            $accountsName,
            $formatter,
            $userFormatter,
            $waitForCompletion,
            $repository,
            $queue,
            $portalChecker
        );

        $this->social = $social;
    }

    public function aware(): array
    {
        return [
            Queue::GROUP_ITEM_CREATE                                 => 'TODO: description',
            Queue::GROUP_ITEM_UPDATE                                 => 'TODO: description',
            Queue::GROUP_ITEM_DELETE                                 => 'TODO: description',
            Queue::LO_UPDATE                                         => 'TODO: description',
            LearningObjectIndexServiceProvider::BULK_CONTENT_SHARING => 'TODO: description',
        ];
    }

    public function consume(string $routingKey, stdClass $body, stdClass $context = null)
    {
        switch ($routingKey) {
            case Queue::GROUP_ITEM_CREATE:
                list($group, $lo) = $this->groupData($body);
                $lo && $this->isActiveItem($body) && $this->onItemCreate($body, $lo, $group);
                break;

            case Queue::GROUP_ITEM_UPDATE:
                list($group, $lo) = $this->groupData($body);
                $lo && $this->onItemUpdate($body, $lo, $group);
                break;

            case Queue::GROUP_ITEM_DELETE:
                list(, $lo) = $this->groupData($body);
                $lo && $this->onItemDelete($body, $lo);
                break;

            case LearningObjectIndexServiceProvider::BULK_CONTENT_SHARING:
                $this->onBulk($body->items, $body->indexName);
                break;
        }
    }

    private function isActiveItem(stdClass $item)
    {
        return GroupItemStatus::ACTIVE == $item->status;
    }

    private function groupData(stdClass $body)
    {
        if ($group = GroupHelper::load($this->social, $body->group_id)) {
            if (GroupHelper::isContentSharing($group)) {
                if (GroupItemTypes::LO == GroupHelper::hostTypeFromGroupTitle($group->title)) {
                    if ($loId = GroupHelper::hostIdFromGroupTitle($group->title)) {
                        if ($lo = LoHelper::load($this->go1, $loId)) {
                            return [$group, $lo];
                        }
                    }
                }
            }
        }

        return [null, null];
    }

    public static function id(int $loId, int $portalId)
    {
        return "shared:$loId:$portalId";
    }

    public static function isPassiveContentSharing(stdClass $group)
    {
        return (strpos($group->title, 'marketplace') !== false);
    }

    protected function format(stdClass $lo, array $context = [])
    {
        $lo = parent::format($lo);
        $lo['metadata']['shared'] = 1;

        if (!empty($context['group'])) {
            $lo['metadata']['shared_passive'] = (int) self::isPassiveContentSharing($context['group']);
        }

        return $lo;
    }

    private function onItemCreate(stdClass $groupItem, stdClass $lo, stdClass $group)
    {
        if (GroupItemTypes::PORTAL == $groupItem->entity_type) {
            try {
                $portalId = $groupItem->entity_id;
                $formatted = $this->format($lo, ['group' => $group]);
                $formatted = parent::custom($formatted, $portalId);

                $this->repository->create([
                    'type' => Schema::O_LO,
                    'id'   => $this->id($lo->id, $portalId),
                    'body' => $formatted,
                ], [Schema::portalIndex($portalId)]);
            } catch (ElasticsearchServiceException $e) {
                $this->history->write(Schema::O_LO, $lo->id, $e->getCode(), $e->getMessage());
            }
        }
    }

    private function onItemUpdate(stdClass $groupItem, stdClass $lo, stdClass $group)
    {
        $original = $groupItem->original ?? false;
        if ($original) {
            // Remove learning object if group item is blocked
            if ($this->isActiveItem($original) && !$this->isActiveItem($groupItem)) {
                return $this->onItemDelete($groupItem, $lo);
            }

            // Add learning object if group item is opened
            if ($this->isActiveItem($groupItem) && !$this->isActiveItem($original)) {
                return $this->onItemCreate($groupItem, $lo, $group);
            }
        }
    }

    private function onItemDelete(stdClass $groupItem, stdClass $lo)
    {
        if (GroupItemTypes::PORTAL == $groupItem->entity_type) {
            $portalId = $groupItem->entity_id;
            $this->client->delete([
                'index' => Schema::portalIndex($portalId),
                'type'  => Schema::O_LO,
                'id'    => $this->id($lo->id, $portalId),
            ]);
        }
    }

    protected function onBulk(array $groupItems, string $indexName)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion, 'client' => ['headers' => ['uuid' => Uuid::uuid4()->toString()]]];
        foreach ($groupItems as $groupItem) {
            list($group, $lo) = $this->groupData($groupItem);

            if ($lo) {
                $params['body'][] = [
                    'index' => [
                        '_index'   => $indexName,
                        '_type'    => Schema::O_LO,
                        '_id'      => $this->id($lo->id, $portalId = $groupItem->entity_id),
                        '_routing' => $portalId,
                    ],
                ];
                $lo->routing = $portalId;
                $formatted = $this->format($lo, ['group' => $group]);
                $params['body'][] = parent::custom($formatted, $portalId);
            }
        }

        if ($params['body']) {
            $response = $this->client->bulk($params);
            $this->history->bulkLog($response);
        }
    }
}
