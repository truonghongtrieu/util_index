<?php

namespace go1\util_index\core\consumer;

use Elasticsearch\Common\Exceptions\ElasticsearchException;
use Exception;
use go1\core\lo\index\LearningObjectIndexServiceProvider;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\queue\Queue;
use go1\util\vote\VoteTypes;
use go1\util_index\IndexHelper;
use ONGR\ElasticsearchDSL\Query\Joining\NestedQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\IdsQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use Ramsey\Uuid\Uuid;
use stdClass;

class LoConsumer extends LearningObjectBaseConsumer
{
    public function aware(string $event): bool
    {
        return in_array($event, [
            Queue::PORTAL_UPDATE,
            Queue::USER_UPDATE, Queue::USER_DELETE,
            Queue::LO_CREATE, Queue::LO_UPDATE, Queue::LO_DELETE,
            Queue::ENROLMENT_CREATE, Queue::ENROLMENT_DELETE,
            Queue::VOTE_CREATE, Queue::VOTE_UPDATE, Queue::VOTE_DELETE,
        ]);
    }

    public function consume(string $routingKey, stdClass $lo, stdClass $context = null): bool
    {
        switch ($routingKey) {
            case Queue::LO_CREATE:
                $this->onCreate($lo);
                break;

            case Queue::LO_UPDATE:
                $this->onUpdate($lo);
                break;

            case Queue::LO_DELETE:
                $this->onDelete($lo);
                break;

            case Queue::ENROLMENT_CREATE:
            case Queue::ENROLMENT_DELETE:
                $enrolment = &$lo;

                // Don't need process indexing job if portal is disabled.
                if (!EnrolmentHelper::isEmbeddedPortalActive($enrolment)) {
                    break;
                }

                $this->updateTotalEnrolment($enrolment->lo_id);
                break;

            case Queue::USER_UPDATE:
                $this->onUserUpdate($lo);
                break;

            case Queue::USER_DELETE:
                $this->onUserDelete($lo);
                break;

            case Queue::PORTAL_UPDATE:
                $portal = clone $lo;
                $this->onPortalUpdate($portal);
                break;

            case Queue::VOTE_CREATE:
            case Queue::VOTE_UPDATE:
            case Queue::VOTE_DELETE:
                $vote = $lo;
                if ($vote->entity_type === VoteTypes::ENTITY_TYPE_LO) {
                    $lo = LoHelper::load($this->go1, $vote->entity_id);
                    $this->onUpdate($lo);
                }
                break;

            case LearningObjectIndexServiceProvider::BULK_LO:
                $this->onBulk($lo->los, $lo->indexName);
                break;

            default:
                trigger_error('Invalid routing key: ' . $routingKey);
                break;
        }

        return true;
    }

    protected function format(stdClass $lo)
    {
        $formatted = $this->formatter->format($lo);

        $groupIds = $this->formatter->groupIds($lo->id, false);
        if ($groupIds) {
            $formatted['group_ids'] = $groupIds;
        }

        return $formatted;
    }

    protected function onCreate(stdClass $lo, $indices = null)
    {
        try {
            if (!LoHelper::isEmbeddedPortalActive($lo)) {
                return null;
            }

            $this->repository->create([
                'type' => Schema::O_LO,
                'id'   => $lo->id,
                'body' => $formattedLo = $this->format($lo),
            ], $indices ?? [Schema::portalIndex($lo->instance_id)]);

            if ($formattedLo['tags']) {
                $this->updateTagSuggestion($lo, $formattedLo['tags']);
                $this->createLoTags($lo, $formattedLo['tags']);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function onUpdate(stdClass $lo, $indices = null)
    {
        try {
            $this->repository->update([
                'type' => Schema::O_LO,
                'id'   => $lo->id,
                'body' => [
                    'doc'           => $formattedLo = $this->format($lo),
                    'doc_as_upsert' => true,
                ],
            ], $indices ?? [Schema::portalIndex($lo->instance_id)]);

            $originalTags = $this->formatter->processTags($lo->original->tags ?? []);
            $this->updateTagSuggestion($lo, $formattedLo['tags'], $originalTags);
            $this->updateLoTags($lo, $formattedLo['tags'], $originalTags);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function onDelete(stdClass $lo, $indices = null)
    {
        try {
            $this->repository->delete([
                'type' => Schema::O_LO,
                'id'   => $lo->id,
            ], $indices ?? [Schema::portalIndex($lo->instance_id)]);

            if ($lo->tags) {
                $tags = $this->formatter->processTags($lo->tags);
                $this->updateTagSuggestion($lo, [], $tags);
                $this->deleteLoTags($lo, $tags);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $lo->id, $e->getCode(), $e->getMessage());
        }
    }

    protected function onBulk(array $los, string $indexName)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion, 'client' => ['headers' => ['uuid' => Uuid::uuid4()->toString()]]];
        foreach ($los as $lo) {
            try {
                $formattedLo = $this->format($lo);
                $params['body'][]['index'] = [
                    '_index'   => $indexName,
                    '_type'    => Schema::O_LO,
                    '_id'      => $lo->id,
                    '_routing' => $lo->routing,
                ];
                $params['body'][] = $formattedLo;

                if ($formattedLo['tags']) {
                    $this->addBulkTagSuggestionParams($params['body'], $formattedLo['tags'], [], $lo->instance_id, $indexName, $lo->id);
                    $this->bulkIndexLoTags($params['body'], $formattedLo['tags'], $lo, $indexName);
                }
            } catch (Exception $e) {
                $this->history->write(Schema::O_LO, $lo->id, 400, $e->getMessage());
            }
        }

        if ($params['body']) {
            $response = $this->client->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function onUserUpdate(stdClass $user)
    {
        if (($this->accountsName == $user->instance) && IndexHelper::userIsChanged($user)) {
            try {
                $formattedUser = $this->userFormatter->format($user, true);
                foreach ($formattedUser as $k => $v) {
                    $lines[] = "ctx._source.authors[i].$k = params.$k";
                }
                $updateCode = implode(";", $lines);
                $this->repository->updateByQuery(Schema::INDEX, Schema::O_LO, [
                    'query'  => (new NestedQuery('authors', new TermQuery('authors.id', $user->id)))->toArray(),
                    'script' => [
                        'inline' => "for (int i=0;i<ctx._source.authors.size();i++) { if (ctx._source.authors[i].id == params.user_id) {{$updateCode}} } ",
                        'params' => ['user_id' => (int) $user->id] + $formattedUser,
                    ],
                ]);
            } catch (ElasticsearchException $e) {
                $this->history->write(Schema::O_USER, $user->id, $e->getCode(), $e->getMessage());
            }
        }
    }

    private function onUserDelete(stdClass $user)
    {
        if ($this->accountsName == $user->instance) {
            try {
                $this->repository->updateByQuery(Schema::INDEX, Schema::O_LO, [
                    'query'  => (new NestedQuery('authors', new TermQuery('authors.id', $user->id)))->toArray(),
                    'script' => [
                        'inline' => "for (int i=0;i<ctx._source.authors.size();i++) { if (ctx._source.authors[i].id == params.user_id) {ctx._source.authors.remove(i);} } ",
                        'params' => [
                            'user_id' => (int) $user->id,
                        ],
                    ],
                ]);
            } catch (ElasticsearchException $e) {
                $this->history->write(Schema::O_USER, $user->id, $e->getCode(), $e->getMessage());
            }
        }
    }

    private function updateTotalEnrolment($loId)
    {
        try {
            $this->repository->updateByQuery(Schema::INDEX, Schema::O_LO, [
                'query'  => (new IdsQuery([$loId]))->toArray(),
                'script' => [
                    'inline' => "ctx._source.totalEnrolment = params.totalEnrolment",
                    'params' => [
                        'totalEnrolment' => LoHelper::countEnrolment($this->go1, $loId),
                    ],
                ],
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $loId, $e->getCode(), $e->getMessage());
        }
    }

    private function onPortalUpdate(stdClass $portal)
    {
        $original = $portal->original ?? null;
        if ($original) {
            $oldSiteName = $this->portalChecker->getSiteName($original);
            $newSiteName = $this->portalChecker->getSiteName($portal);

            if ($oldSiteName != $newSiteName) {
                $this->repository->updateByQuery(Schema::INDEX, Schema::O_LO, [
                    'query'  => (new TermQuery('instance_id', $portal->id))->toArray(),
                    'script' => [
                        'inline' => "ctx._source.portal_name = params.portal_name",
                        'params' => [
                            'portal_name' => $newSiteName,
                        ],
                    ],
                ]);
            }
        }
    }
}
