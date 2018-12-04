<?php

namespace go1\util_index\core\consumer;

use go1\core\lo\index\LearningObjectIndexServiceProvider;
use go1\util\DB;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\lo\TagTypes;
use go1\util\queue\Queue;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use Ramsey\Uuid\Uuid;
use stdClass;

class LoShareConsumer extends LoConsumer
{
    public static $limit = 200;

    public function aware(): array
    {
        return [
            Queue::LO_UPDATE                                   => 'TODO: description',
            Queue::LO_GROUP_CREATE                             => 'TODO: description',
            Queue::LO_GROUP_DELETE                             => 'TODO: description',
            Queue::CUSTOM_TAG_PUSH                             => 'TODO: description',
            Queue::CUSTOM_TAG_CREATE                           => 'TODO: description',
            Queue::CUSTOM_TAG_DELETE                           => 'TODO: description',
            LearningObjectIndexServiceProvider::INDEX_SHARE_LO => 'TODO: description',
            LearningObjectIndexServiceProvider::BULK_LO_SHARE  => 'TODO: description',
        ];
    }

    public function consume(string $routingKey, stdClass $lo, stdClass $context = null)
    {
        switch ($routingKey) {
            case Queue::LO_UPDATE:
                $loIds = [$lo->id];
                $numOfPortal = $loIds ? $this->numOfPortal($loIds) : 0;
                if ($numOfPortal > 0) {
                    for ($offset = 0; $offset < $numOfPortal; $offset += static::$limit) {
                        $this->queue->queue(
                            [
                                'originalKey' => $routingKey,
                                'lo'          => $lo,
                                'offset'      => $offset,
                                'loIds'       => $loIds,
                            ],
                            LearningObjectIndexServiceProvider::INDEX_SHARE_LO
                        );
                    }
                }
                break;

            case Queue::LO_GROUP_CREATE:
                $this->onLoGroupCreate($lo->lo_id, $lo->instance_id);
                break;

            case Queue::LO_GROUP_DELETE:
                $this->onLoGroupDelete($lo->lo_id, $lo->instance_id);
                break;

            case Queue::CUSTOM_TAG_PUSH:
            case Queue::CUSTOM_TAG_CREATE:
            case Queue::CUSTOM_TAG_DELETE:
                $this->onTagUpdate($lo, $routingKey);
                break;

            case LearningObjectIndexServiceProvider::INDEX_SHARE_LO:
                $this->onIndexShare($lo);
                break;

            case LearningObjectIndexServiceProvider::BULK_LO_SHARE:
                $this->onBulk($lo->los, $lo->indexName);
                break;
        }
    }

    private function onTagUpdate(stdClass $customTag, string $routingKey)
    {
        $loId = $customTag->lo_id;
        $portalId = $customTag->instance_id;
        $isLoExist = $this->client->exists(
            [
                'index'   => Schema::INDEX,
                'id'      => self::id($portalId, $loId),
                'routing' => $portalId,
                'type'    => Schema::O_LO,
            ]
        );

        if (!$isLoExist) {
            return;
        }

        if ($lo = LoHelper::load($this->go1, $loId)) {
            $params = ['body' => [], 'refresh' => true];
            $params['body'][] = [
                'update' => array_filter([
                    '_routing' => $portalId,
                    '_index'   => Schema::portalIndex($portalId),
                    '_type'    => Schema::O_LO,
                    '_id'      => self::id($portalId, $lo->id),
                ]),
            ];
            $formatted = $this->formatter->format($lo);
            $customFormatted = $this->custom($formatted, $portalId);
            $params['body'][] = [
                'doc'           => $customFormatted,
                'doc_as_upsert' => true,
            ];

            $this->addLoTagParams($params['body'], $portalId, $customFormatted['tags']);

            $shouldAddTag = ((Queue::CUSTOM_TAG_CREATE == $routingKey) && $customTag->status);
            if ($shouldAddTag) {
                $addTags = $this->formatter->processTags([$customTag->tag]);
                $this->addBulkTagSuggestionParams($params['body'], $addTags, [], $portalId, Schema::portalIndex($portalId), self::id($portalId, $lo->id));
            }

            $shouldRemoveTag = ((Queue::CUSTOM_TAG_CREATE == $routingKey && !$customTag->status)) || (Queue::CUSTOM_TAG_DELETE == $routingKey);
            if ($shouldRemoveTag) {
                $removeTags = $this->formatter->processTags([$customTag->tag]);
                $this->addBulkTagSuggestionParams($params['body'], [], $removeTags, $portalId, Schema::portalIndex($portalId), self::id($portalId, $lo->id));
            }

            if ($params['body']) {
                $response = $this->client->bulk($params);
                $this->history->bulkLog($response);
            }
        }
    }

    private function numOfPortal(array $loIds): int
    {
        $sql = 'SELECT count(instance_id) FROM gc_lo_group WHERE lo_id IN (?)';

        return $this->go1->executeQuery($sql, [$loIds], [DB::INTEGERS])->fetchColumn();
    }

    private function onIndexShare(stdClass $data)
    {
        $lo = $data->lo;

        $q = $this->go1->createQueryBuilder();
        $q = $q
            ->select('instance_id')
            ->from('gc_lo_group')
            ->andWhere('lo_id IN (?)')
            ->setParameters([$data->loIds], [DB::INTEGERS])
            ->setFirstResult($data->offset)
            ->setMaxResults(static::$limit)
            ->execute();

        $portalIds = [];
        while ($portalId = $q->fetchColumn()) {
            $portalIds[] = $portalId;
        }

        switch ($data->originalKey) {

            case Queue::LO_CREATE:
                $this->create($lo, $portalIds);
                break;

            case Queue::LO_UPDATE:
                $this->update($lo, $portalIds);
                break;

            case Queue::LO_DELETE:
                $this->delete($lo, $portalIds);
                break;
        }

        return true;
    }

    private function onLoGroupCreate(int $loId, int $portalId)
    {
        if ($lo = LoHelper::load($this->go1, $loId)) {
            $this->create($lo, [$portalId]);
        }
    }

    private function onLoGroupDelete(int $loId, int $portalId)
    {
        if ($lo = LoHelper::load($this->go1, $loId)) {
            $this->delete($lo, [$portalId]);
        }
    }

    public static function id(int $portalId, int $loId)
    {
        return implode(':', [$portalId, $loId]);
    }

    protected function custom(array $formatted, int $portalId)
    {
        $formatted['tags'] = $this->getCustomTags($formatted['id'], $portalId, $formatted['tags']);
        $formatted['metadata']['instance_id'] = $portalId;
        $formatted['metadata']['customized'] = 0;
        $formatted['metadata']['membership'] = $portalId;

        $date = LoHelper::getCustomisation($this->go1, $formatted['id'], $portalId);
        if ($date) {
            $formatted['published'] = (int) ($date['published'] ?? $formatted['published']);
            $formatted['private'] = (int) $date['private'] ?? $formatted['private'];
            $formatted['metadata']['customized'] = 1;
        }

        return $formatted;
    }

    protected function getCustomTags(int $loId, int $portalId, array $loTags = [])
    {
        $customTags = $loTags;
        $q = 'SELECT tag, status FROM gc_lo_tag WHERE instance_id = ? AND lo_id = ?';
        $q = $this->go1->executeQuery($q, [$portalId, $loId]);
        while ($tag = $q->fetch(DB::OBJ)) {
            $customTag = html_entity_decode($tag->tag);
            if ($tag->status) {
                if (!in_array($customTag, $loTags)) {
                    $customTags[] = $customTag;
                }
            } // Remove if tag is marked disabled in portal (custom tag status = 0)
            else {
                $removeIndex = array_search($customTag, $loTags);
                if (false !== $removeIndex) {
                    unset($customTags[$removeIndex]);
                }
            }
        }

        return array_values($customTags);
    }

    private function create(stdClass $lo, array $portalIds)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion];
        foreach ($portalIds as $portalId) {
            $params['body'][] = [
                'index' => [
                    '_routing' => $portalId,
                    '_index'   => Schema::portalIndex($portalId),
                    '_type'    => Schema::O_LO,
                    '_id'      => self::id($portalId, $lo->id),
                ],
            ];

            $formatted = $this->formatter->format($lo);
            $customFormatted = $this->custom($formatted, $portalId);
            $params['body'][] = $customFormatted;

            $this->addBulkTagSuggestionParams($params['body'], $customFormatted['tags'], [], $portalId, Schema::portalIndex($portalId), self::id($portalId, $lo->id));
            $this->addLoTagParams($params['body'], $portalId, $customFormatted['tags']);
        }

        if ($params['body']) {
            $response = $this->client->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function update(stdClass $lo, array $portalIds)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion];
        foreach ($portalIds as $portalId) {
            $params['body'][] = [
                'update' => array_filter([
                    '_routing' => $portalId,
                    '_index'   => Schema::portalIndex($portalId),
                    '_type'    => Schema::O_LO,
                    '_id'      => self::id($portalId, $lo->id),
                ]),
            ];

            $formatted = $this->formatter->format($lo);
            $customFormatted = $this->custom($formatted, $portalId);
            $params['body'][] = [
                'doc'           => $customFormatted,
                'doc_as_upsert' => true,
            ];

            $originalTags = $this->formatter->processTags($lo->original->tags ?? []);
            $this->addBulkTagSuggestionParams($params['body'], $customFormatted['tags'], $originalTags, $portalId, Schema::portalIndex($portalId), self::id($portalId, $lo->id));
            $this->addLoTagParams($params['body'], $portalId, $customFormatted['tags'], $originalTags);
        }

        if ($params['body']) {
            $response = $this->client->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function delete(stdClass $lo, array $portalIds)
    {
        $indices = [];
        foreach ($portalIds as $portalId) {
            $indices[] = Schema::portalIndex($portalId);
        }

        $this->client->deleteByQuery([
            'index'               => implode(',', $indices),
            'type'                => Schema::O_LO,
            'body'                => [
                'query' => (new TermQuery('id', $lo->id))->toArray(),
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ]);

        if ($lo->tags) {
            $params = ['body' => [], 'refresh' => $this->waitForCompletion];
            $currentTags = $this->formatter->processTags($lo->tags);
            foreach ($portalIds as $portalId) {
                $customTags = $this->getCustomTags($lo->id, $portalId, $currentTags);
                $this->addBulkTagSuggestionParams($params['body'], [], $customTags, $portalId, Schema::portalIndex($portalId), self::id($portalId, $lo->id));
                $this->addLoTagParams($params['body'], $portalId, [], $customTags);
            }

            if ($params['body']) {
                $response = $this->client->bulk($params);
                $this->history->bulkLog($response);
            }
        }
    }

    protected function onBulk(array $loGroups, string $indexName)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion, 'client' => ['headers' => ['uuid' => Uuid::uuid4()->toString()]]];
        foreach ($loGroups as $loGroup) {
            $esLoId = self::id($loGroup->instance_id, $loGroup->lo_id);
            $params['body'][] = [
                'index' => [
                    '_index'   => $indexName,
                    '_type'    => Schema::O_LO,
                    '_id'      => $esLoId,
                    '_routing' => $loGroup->instance_id,
                ],
            ];
            $lo = LoHelper::load($this->go1, $loGroup->lo_id, $loGroup->instance_id);
            $lo->routing = $loGroup->instance_id;
            $formatted = $this->formatter->format($lo);
            $formatted = $this->custom($formatted, $loGroup->instance_id);
            $params['body'][] = $formatted;

            if ($formatted['tags']) {
                $this->addBulkTagSuggestionParams($params['body'], $formatted['tags'], [], $loGroup->instance_id, $indexName, $esLoId);
            }

            if ($formatted['tags']) {
                $this->bulkIndexLoTags($params['body'], $formatted['tags'], $lo, $indexName, TagTypes::PREMIUM);
            }
        }

        if ($params['body']) {
            $response = $this->client->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function addLoTagParams(array &$paramBody, int $portalId, $newTags, $currentTags = [])
    {
        $addTags = array_diff($newTags, $currentTags);
        foreach ($addTags as $addTag) {
            $paramBody[] = [
                'index' => [
                    '_index' => Schema::portalIndex($portalId),
                    '_type'  => Schema::O_LO_TAG,
                    '_id'    => $addTag . ':' . $portalId,
                ],
            ];
            $paramBody[] = [
                'title'    => $addTag,
                'type'     => TagTypes::PREMIUM,
                'metadata' => [
                    'instance_id' => $portalId,
                ],
            ];
        }

        $removeTags = array_diff($currentTags, $newTags);
        foreach ($removeTags as $removeTag) {
            $paramBody[] = [
                'delete' => [
                    '_index' => Schema::portalIndex($portalId),
                    '_type'  => Schema::O_LO_TAG,
                    '_id'    => $removeTag . ':' . $portalId,
                ],
            ];
        }
    }
}
