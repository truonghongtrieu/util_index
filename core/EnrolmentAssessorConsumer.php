<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\util\contract\ConsumerInterface;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\lo\LoTypes;
use go1\util\queue\Queue;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use stdClass;

class EnrolmentAssessorConsumer implements ConsumerInterface
{
    const RETRY_ROUTING_KEY = 'enrolment-index.message.retry';

    private $client;
    private $go1;
    private $fEnrolment;
    private $queue;

    public function __construct(Client $client, Connection $go1, EnrolmentFormatter $fEnrolment, MqClient $queue)
    {
        $this->client = $client;
        $this->go1 = $go1;
        $this->fEnrolment = $fEnrolment;
        $this->queue = $queue;
    }

    public function aware(string $event): bool
    {
        return in_array($event, [Queue::ENROLMENT_SAVE_ASSESSORS, self::RETRY_ROUTING_KEY]);
    }

    public function consume(string $routingKey, stdClass $msg, stdClass $context = null): bool
    {
        switch ($routingKey) {
            case Queue::ENROLMENT_SAVE_ASSESSORS:
                $this->onEnrolmentAssessorUpdated($routingKey, $msg);
                break;

            case self::RETRY_ROUTING_KEY:
                if ($msg->body->numOfRetry < 3) {
                    $this->consume($msg->routingKey, $msg->body);
                }
                break;
        }

        return true;
    }

    private function onEnrolmentAssessorUpdated(string $routingKey, stdClass $body)
    {
        $enrolmentId = $body->id;
        if (!$enrolment = EnrolmentHelper::load($this->go1, $enrolmentId)) {
            return null;
        }

        $assessors = EnrolmentHelper::assessorIds($this->go1, $enrolmentId);
        if (!$assessors) {
            if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                if ($lo->type == LoTypes::COURSE) {
                    $courseAssessors = LoHelper::assessorIds($this->go1, $lo->id);
                    if (count($courseAssessors) == 1) {
                        $assessors = $courseAssessors;
                    }
                }
            }
        }

        $response = $this->client->updateByQuery([
            'index'               => Schema::INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => call_user_func(
                    function () use (&$enrolment) {
                        $query = new BoolQuery;
                        $query->add(new TermQuery('metadata.course_enrolment_id', $enrolment->id), BoolQuery::MUST);

                        return $query->toArray();
                    }
                ),
                'script' => [
                    'inline' => implode(";", [
                        "ctx._source.assessor = params.assessor",
                        "ctx._source.assessors = params.assessors",
                        "ctx._source.metadata.has_assessor = params.has_assessor",
                    ]),
                    'params' => [
                        'assessor'     => $this->fEnrolment->assessor($assessors),
                        'assessors'    => $assessors,
                        'has_assessor' => $assessors ? 1 : 0,
                    ],
                ],
            ],
            'refresh'             => true,
            'wait_for_completion' => true,
            'conflicts'           => 'proceed',
        ]);

        $this->handleConflict($response, $routingKey, $body);
    }

    protected function handleConflict(array $response, string $routingKey, stdClass $body)
    {
        $numOfConflict = $response['version_conflicts'] ?? 0;
        if ($numOfConflict > 0) {
            $body->numOfRetry = ($body->numOfRetry ?? 0) + 1;
            $this->queue->queue(['routingKey' => $routingKey, 'body' => $body], self::RETRY_ROUTING_KEY);
        }
    }
}
