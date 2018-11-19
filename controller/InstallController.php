<?php

namespace go1\util_index\controller;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\util\DB;
use go1\util\es\Schema as Index;
use go1\util\portal\PortalStatuses;
use go1\util_index\IndexSchema;
use go1\util_index\IndexServiceProvider;

class InstallController
{
    private $db;
    private $go1;
    private $client;
    private $mqClient;

    public function __construct(
        Connection $db,
        Connection $go1,
        Client $client,
        MqClient $mqClient
    )
    {
        $this->db = $db;
        $this->go1 = $go1;
        $this->client = $client;
        $this->mqClient = $mqClient;
    }

    public function post()
    {
        if (!$this->client->indices()->exists(['index' => Index::INDEX])) {
            $this->client->indices()->create(Index::SCHEMA);
        }

        if (!$this->client->indices()->exists(['index' => Index::MARKETPLACE_INDEX])) {
            $this->client->indices()->create([
                'index' => Index::MARKETPLACE_INDEX,
                'body'  => Index::BODY,
            ]);
        }

        $q = $this->go1->createQueryBuilder();
        $q = $q
            ->select('id')
            ->from('gc_instance')
            ->where('status = :status')
            ->andWhere('id > 0')
            ->setParameter('status', PortalStatuses::ENABLED)
            ->execute();

        while ($portalId = $q->fetchColumn()) {
            $this->mqClient->queue(['portalId' => $portalId], IndexServiceProvider::INDEX_INSTALL_PORTAL);
        }

        return DB::install($this->db, [
            function (Schema $schema) {
                IndexSchema::install($schema);
            },
        ]);
    }
}
