<?php

namespace go1\util_index\consumer;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use go1\clients\MqClient;
use go1\core\customer\user\index\UserFormatter;
use go1\core\lo\index\LoFormatter;
use go1\util\contract\ConsumerInterface;
use go1\util\es\dsl\TermsAggregation;
use go1\util\es\Schema;
use go1\util\lo\LoTypes;
use go1\util\portal\PortalChecker;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\HistoryRepository;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermsQuery;
use ONGR\ElasticsearchDSL\Search;
use stdClass;

abstract class LearningObjectBaseConsumer implements ConsumerInterface
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
}
