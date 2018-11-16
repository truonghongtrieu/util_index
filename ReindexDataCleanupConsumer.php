<?php

namespace go1\util_index;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Exception;
use go1\util\contract\ConsumerInterface;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\RangeQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermsQuery;
use Psr\Log\LoggerInterface;
use stdClass;

class ReindexDataCleanupConsumer implements ConsumerInterface
{
    private $go1;
    private $client;
    private $history;
    private $logger;
    private $waitForCompletion;

    public function __construct(
        Connection $go1,
        Client $client,
        HistoryRepository $history,
        LoggerInterface $logger,
        bool $waitForCompletion
    )
    {
        $this->go1 = $go1;
        $this->client = $client;
        $this->history = $history;
        $this->logger = $logger;
        $this->waitForCompletion = $waitForCompletion;
    }

    public function aware(string $event): bool
    {
        return in_array($event, [ReindexServiceProvider::INDEX_REMOVE_REDUNDANT_DATA]);
    }

    public function consume(string $routingKey, stdClass $data, stdClass $context = null): bool
    {
        switch ($routingKey) {
            case ReindexServiceProvider::INDEX_REMOVE_REDUNDANT_DATA:
                $this->delete($data);
                break;
        }

        return true;
    }

    private function delete(stdClass &$data)
    {
        if (!$portal = PortalHelper::load($this->go1, $data->portalId)) {
            return null;
        }

        if (!$fromDate = $data->fromDate ?? 0) {
            return null;
        }

        try {
            $query = new BoolQuery();
            $query->add(new RangeQuery('metadata.updated_at', [RangeQuery::LT => $fromDate]));
            $query->add(new TermsQuery('_type', [Schema::O_LO, Schema::O_ENROLMENT, Schema::O_ACCOUNT, Schema::O_EVENT]));

            $this->client->deleteByQuery([
                'index'               => Schema::portalIndex($portal->id),
                'refresh'             => $this->waitForCompletion,
                'wait_for_completion' => $this->waitForCompletion,
                'body'                => [
                    'query' => $query->toArray(),
                ],
            ]);
        } catch (Exception $e) {
            $this->history->write('delete.legacy.data', $data->portalId, $e->getCode(), json_encode($data));
        }
    }
}
