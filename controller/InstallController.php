<?php

namespace go1\util_index\controller;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\internal\index\deprecated\DeprecatedInternalServiceProvider;
use go1\util\DB;
use go1\util\es\Schema as Index;
use go1\util\portal\PortalStatuses;
use go1\util_index\IndexSchema;

class InstallController
{
    private $db;
    private $go1;
    private $es;
    private $queue;

    public function __construct(Connection $db, Connection $go1, Client $es, MqClient $queue)
    {
        $this->db = $db;
        $this->go1 = $go1;
        $this->es = $es;
        $this->queue = $queue;
    }

    public function post()
    {
        if (!$this->es->indices()->exists(['index' => Index::INDEX])) {
            $this->es->indices()->create(Index::SCHEMA);
        }

        if (!$this->es->indices()->exists(['index' => Index::MARKETPLACE_INDEX])) {
            $this->es->indices()->create([
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
            $this->queue->queue(['portalId' => $portalId], DeprecatedInternalServiceProvider::INDEX_INSTALL_PORTAL);
        }

        return DB::install($this->db, [
            function (Schema $schema) {
                IndexSchema::install($schema);
            },
        ]);
    }
}
