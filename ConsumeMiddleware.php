<?php

namespace go1\util_index;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\util\Error;
use go1\util\portal\PortalChecker;
use go1\util\portal\PortalHelper;
use go1\util\queue\Queue;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Request;

class ConsumeMiddleware
{
    private $go1;
    private $repository;
    private $logger;
    private $portalChecker;
    private $client;

    public function __construct(
        Connection $go1,
        ElasticSearchRepository $repository,
        LoggerInterface $logger,
        PortalChecker $portalChecker,
        Client $client)
    {
        $this->go1 = $go1;
        $this->repository = $repository;
        $this->logger = $logger;
        $this->portalChecker = $portalChecker;
        $this->client = $client;
    }

    public function __invoke(Request $req)
    {
        $routingKey = $req->get('routingKey');
        # Drop prefix if routingKey contains re-index prefix.
        if (Queue::REINDEX_PREFIX == substr($routingKey, 0, 12)) {
            $parsedRoutingKey = explode(Queue::REINDEX_PREFIX, $routingKey)[1];
            $req->request->set('routingKey', $parsedRoutingKey);
        }

        $body = $req->get('body');
        $body = is_scalar($body) ? json_decode($body) : json_decode(json_encode($body));
        if (!$body) {
            return null;
        }

        $portalId = $body->taken_instance_id ?? $body->instance_id ?? $body->instance ?? null;
        if (0 === $portalId) {
            $this->logger->error(sprintf('index.consume skip bad message %s: %s', $routingKey, json_encode($body)));

            return Error::simpleErrorJsonResponse(null, 204);
        }

        if ($portalId && ($portal = PortalHelper::load($this->go1, $portalId))) {
            if ($this->portalChecker->isLegacy($portal)) {
                $this->logger->alert(sprintf('index.consume skip legacy message %s: %s', $routingKey, json_encode($body)));

                return Error::simpleErrorJsonResponse(null, 204);
            }

            # @TODO Disable to improve performance
            # if (!$this->client->indices()->exists(['index' => Schema::portalIndex($portal->id)])) {
            #     $this->repository->installPortalIndex($portal->id);
            # }
        }

        return null;
    }
}
