<?php

namespace go1\util_index\core\consumer;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use go1\clients\MqClient;
use go1\util\contract\ServiceConsumerInterface;
use go1\util\es\dsl\TermsAggregation;
use go1\util\es\Schema;
use go1\util\lo\LoTypes;
use go1\util\lo\TagTypes;
use go1\util\portal\PortalChecker;
use go1\util_index\core\LoFormatter;
use go1\util_index\core\UserFormatter;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\HistoryRepository;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\IdsQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermsQuery;
use ONGR\ElasticsearchDSL\Search;
use stdClass;

abstract class LearningObjectBaseConsumer implements ServiceConsumerInterface
{
    protected $client;
    protected $history;
    protected $db;
    protected $go1;
    protected $accountsName;
    protected $formatter;
    protected $userFormatter;
    protected $waitForCompletion;
    protected $repository;
    protected $queue;
    protected $portalChecker;

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
        PortalChecker $portalChecker
    )
    {
        $this->client = $client;
        $this->history = $history;
        $this->db = $db;
        $this->go1 = $go1;
        $this->accountsName = $accountsName;
        $this->formatter = $formatter;
        $this->userFormatter = $userFormatter;
        $this->waitForCompletion = $waitForCompletion;
        $this->repository = $repository;
        $this->queue = $queue;
        $this->portalChecker = $portalChecker;
    }

    protected function updateTagSuggestion(stdClass $lo, array $newTagTitles, array $currentTagTitles = [], $isLo = true)
    {
        try {
            $params['body'] = [];
            $esLoId = $isLo ? $lo->id : sprintf('%s:%s', LoTypes::AWARD, $lo->id);
            $this->addBulkTagSuggestionParams($params['body'], $newTagTitles, $currentTagTitles, $lo->instance_id, Schema::portalIndex($lo->instance_id), $esLoId);

            if ($params['body']) {
                $this->client->bulk(['refresh' => $this->waitForCompletion] + $params);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_SUGGESTION_TAG, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function addBulkTagSuggestionParams(array &$paramBody, array $newTagTitles, array $currentTagTitles, int $portalId, string $indexName, $esLoId)
    {
        $addTagTitles = array_diff($newTagTitles, $currentTagTitles);
        foreach ($addTagTitles as $addTagTitle) {
            $paramBody[] = [
                'index' => [
                    '_routing' => $portalId,
                    '_index'   => $indexName,
                    '_type'    => Schema::O_SUGGESTION_TAG,
                    '_id'      => self::getSuggestionTagId($addTagTitle, $portalId),
                ],
            ];
            $paramBody[] = $this->formatSuggestionTag($addTagTitle, $portalId);
        }

        $removeTagTitles = array_diff($currentTagTitles, $newTagTitles);
        $removeTagSearch = $this->client->search([
            'index' => $indexName,
            'type'  => Schema::O_LO,
            'body'  => (new Search)
                ->addQuery(new TermQuery('_id', $esLoId), BoolQuery::MUST_NOT)
                ->addQuery(new TermsQuery('tags', array_values($removeTagTitles)))
                ->addAggregation(new TermsAggregation('tags', 'tags', null, count($removeTagTitles)))
                ->setSize(0)
                ->toArray(),
        ]);
        $foundTagTitles = array_column($removeTagSearch['aggregations']['tags']['buckets'], 'key');
        $removeTagTitles = array_diff($removeTagTitles, $foundTagTitles);

        foreach ($removeTagTitles as $removeTag) {
            $paramBody[] = [
                'delete' => [
                    '_routing' => $portalId,
                    '_index'   => $indexName,
                    '_type'    => Schema::O_SUGGESTION_TAG,
                    '_id'      => self::getSuggestionTagId($removeTag, $portalId),
                ],
            ];
        }
    }

    public static function getSuggestionTagId($title, $portalId)
    {
        return md5("$title:$portalId");
    }

    protected function formatSuggestionTag($tagTitle, int $portalId)
    {
        return [
            'tag'      => [
                'input'    => $tagTitle,
                'weight'   => 1,
                'contexts' => ['instance_id' => $portalId],
            ],
            'metadata' => [
                'instance_id' => $portalId,
            ],
        ];
    }

    protected function createLoTags(stdClass $lo, array $newTags, $type = TagTypes::LOCAL)
    {
        try {
            $params['body'] = [];
            foreach ($newTags as $addTag) {
                $params['body'][] = [
                    'index' => [
                        '_routing' => $lo->instance_id,
                        '_index'   => Schema::portalIndex($lo->instance_id),
                        '_type'    => Schema::O_LO_TAG,
                        '_id'      => $addTag . ':' . $lo->instance_id,
                    ],
                ];

                $params['body'][] = [
                    'title'    => $addTag,
                    'type'     => $type,
                    'metadata' => ['instance_id' => $lo->instance_id],
                ];
            }

            if ($params['body']) {
                return $this->client->bulk(['refresh' => $this->waitForCompletion] + $params);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO_TAG, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function updateLoTags(stdClass $lo, array $newTags, array $currentTags = [], $type = TagTypes::LOCAL, $isLo = true)
    {
        $addTags = array_diff($newTags, $currentTags);
        $this->createLoTags($lo, $addTags, $type);

        $removeTags = array_diff($currentTags, $newTags);
        $this->deleteLoTags($lo, $removeTags, $isLo);
    }

    protected function deleteLoTags(stdClass $lo, array $removeTags, $isLo = true)
    {
        try {
            if (!$removeTags) {
                return;
            }

            $esLoId = $isLo ? $lo->id : sprintf('%s:%s', LoTypes::AWARD, $lo->id);
            $portalId = $lo->instance_id;
            $removeTags = array_values($removeTags);

            foreach ($removeTags as $removeTag) {
                $removeTagSearch[] = ['index' => Schema::portalIndex($portalId), 'type' => Schema::O_LO];
                $removeTagSearch[] = (new Search)
                    ->addQuery(new TermQuery('id', $esLoId), BoolQuery::MUST_NOT)
                    ->addQuery(new TermQuery('tags', $removeTag))
                    ->addQuery(new TermQuery('instance_id', $portalId))
                    ->toArray();
            }

            $hasTags = $this->client->msearch(['body' => $removeTagSearch]);
            foreach ($hasTags['responses'] as $key => $hasTag) {
                if ($hasTag['hits']['total'] == 0) {
                    $removeTagIds[] = $removeTags[$key] . ':' . $portalId;
                }
            }

            !empty($removeTagIds) && $this->client->deleteByQuery([
                'index'   => Schema::portalIndex($portalId),
                'type'    => Schema::O_LO_TAG,
                'body'    => ['query' => (new IdsQuery($removeTagIds))->toArray()],
                'refresh' => $this->waitForCompletion,
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO_TAG, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function bulkIndexLoTags(array &$body, array $tags, stdClass $lo, string $indexName, $type = TagTypes::LOCAL)
    {
        foreach ($tags as $tag) {
            $body[]['index'] = [
                '_routing' => $lo->instance_id,
                '_index'   => $indexName,
                '_type'    => Schema::O_LO_TAG,
                '_id'      => $tag . ':' . $lo->instance_id,
            ];

            $body[] = [
                'title'    => $tag,
                'type'     => $type,
                'metadata' => ['instance_id' => $lo->instance_id],
            ];
        }
    }
}
