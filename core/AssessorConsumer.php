<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use go1\clients\MqClient;
use go1\util\contract\ConsumerInterface;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\lo\LoTypes;
use go1\util\queue\Queue;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexService;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\IdsQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use stdClass;

class AssessorConsumer implements ConsumerInterface
{
    protected $client;
    protected $history;
    protected $db;
    protected $go1;
    protected $award;
    protected $enrolmentFormatter;
    protected $waitForCompletion;
    protected $queue;

    public function __construct(
        Client $client,
        HistoryRepository $repository,
        Connection $db,
        Connection $go1,
        Connection $award,
        EnrolmentFormatter $enrolmentFormatter,
        bool $waitForCompletion,
        MqClient $queue
    )
    {
        $this->client = $client;
        $this->history = $repository;
        $this->db = $db;
        $this->go1 = $go1;
        $this->award = $award;
        $this->enrolmentFormatter = $enrolmentFormatter;
        $this->waitForCompletion = $waitForCompletion;
        $this->queue = $queue;
    }

    public function aware(string $event): bool
    {
        return in_array($event, [
            Queue::LO_SAVE_ASSESSORS,
            Queue::ENROLMENT_SAVE_ASSESSORS,
            IndexService::WORKER_MESSAGE_RETRY,
        ]);
    }

    public function consume(string $routingKey, stdClass $body, stdClass $context = null): bool
    {
        switch ($routingKey) {
            case Queue::LO_SAVE_ASSESSORS:
                $this->onCourse($routingKey, $body);
                break;

            case Queue::ENROLMENT_SAVE_ASSESSORS:
                $this->onEnrolment($routingKey, $body);
                break;

            case IndexService::WORKER_MESSAGE_RETRY:
                $this->onMessageRetry($body);
                break;
        }

        return true;
    }

    protected function onMessageRetry(stdClass $data)
    {
        if ($data->body->numOfRetry < 3) {
            $this->consume($data->routingKey, $data->body);
        }
    }

    protected function handleConflict($response, $routingKey, $body)
    {
        $numOfConflict = $response['version_conflicts'] ?? 0;
        if ($numOfConflict > 0) {
            $body->numOfRetry = ($body->numOfRetry ?? 0) + 1;
            $this->queue->queue([
                'routingKey' => $routingKey,
                'body'       => $body,
            ], IndexService::WORKER_MESSAGE_RETRY);
        }
    }

    private function onCourse(string $routingKey, stdClass $body)
    {
        $courseId = $body->id;
        $assessors = LoHelper::assessorIds($this->go1, $courseId);
        try {
            $this->client->updateByQuery([
                'index'               => Schema::INDEX,
                'type'                => Schema::O_LO,
                'body'                => [
                    'query'  => (new IdsQuery([$courseId]))->toArray(),
                    'script' => [
                        'inline' => implode(";", [
                            "ctx._source.assessor = params.assessor",
                            "ctx._source.assessors = params.assessors",
                        ]),
                        'params' => [
                            'assessor'  => $this->enrolmentFormatter->assessor($assessors),
                            'assessors' => $assessors,
                        ],
                    ],
                ],
                'wait_for_completion' => $this->waitForCompletion,
                'conflicts'           => 'proceed',
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $courseId, $e->getCode(), $e->getMessage());
        }

        $assessors = (count($assessors) <= 1) ? $assessors : [];
        $query = new BoolQuery();
        $query->add(new TermQuery('metadata.has_assessor', 0), BoolQuery::MUST);
        $query->add(new TermQuery('metadata.course_id', $courseId), BoolQuery::MUST);

        $response = $this->client->updateByQuery([
            'index'               => Schema::INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => $query->toArray(),
                'script' => [
                    'inline' => "ctx._source.assessors = params.assessors;",
                    'params' => ['assessors' => $assessors],
                ],
            ],
            'refresh'             => true,
            # When a course changed assessors
            # There will be 2 messages published(ro.create then ro.delete).
            # We need wait until each query completed to avoid conflicts.
            'wait_for_completion' => true,
            'conflicts'           => 'proceed',
        ]);

        $this->handleConflict($response, $routingKey, $body);
    }

    private function onEnrolment(string $routingKey, $body)
    {
        $enrolmentId = $body->id;
        if ($enrolment = EnrolmentHelper::load($this->go1, $enrolmentId)) {
            $assessors = EnrolmentHelper::assessorIds($this->go1, $enrolmentId);
            $hasAssessor = $assessors ? 1 : 0;

            if (empty($assessors) && ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) && ($lo->type == LoTypes::COURSE)) {
                $courseAssessors = LoHelper::assessorIds($this->go1, $lo->id);
                (count($courseAssessors) == 1) && $assessors = $courseAssessors;
            }

            $query = new BoolQuery();
            $query->add(new TermQuery('metadata.course_enrolment_id', $enrolment->id), BoolQuery::MUST);
            $response = $this->client->updateByQuery([
                'index'               => Schema::INDEX,
                'type'                => Schema::O_ENROLMENT,
                'body'                => [
                    'query'  => $query->toArray(),
                    'script' => [
                        'inline' => implode(";", [
                            "ctx._source.assessor = params.assessor",
                            "ctx._source.assessors = params.assessors",
                            "ctx._source.metadata.has_assessor = params.has_assessor",
                        ]),
                        'params' => [
                            'assessor'     => $this->enrolmentFormatter->assessor($assessors),
                            'assessors'    => $assessors,
                            'has_assessor' => $hasAssessor,
                        ],
                    ],
                ],
                'refresh'             => true,
                'wait_for_completion' => true,
                'conflicts'           => 'proceed',
            ]);

            $this->handleConflict($response, $routingKey, $body);
        }
    }
}
