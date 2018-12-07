<?php

namespace go1\util_index\tests;

use go1\core\customer\portal\index\tests\PortalReIndexTest;
use go1\internal\index\deprecated\DeprecatedReindexCleanupConsumer;
use go1\util\es\mock\EsEnrolmentMockTrait;
use go1\util\es\mock\EsEventMockTrait;
use go1\util\es\mock\EsLoMockTrait;
use go1\util\es\mock\EsUserMockTrait;
use go1\util\es\Schema;
use go1\util_index\IndexService;

class ReindexDataCleanupConsumerTest extends PortalReIndexTest
{
    use EsLoMockTrait;
    use EsEnrolmentMockTrait;
    use EsUserMockTrait;
    use EsEventMockTrait;

    public function testDelete()
    {
        $app = parent::test();

        /** @var DeprecatedReindexCleanupConsumer $consumer */
        $consumer = $app['consumer.remove_redundant_data'];
        $consumer->consume(IndexService::REINDEX_CLEANUP, (object) [
            'portalId' => $this->portalId,
            'fromDate' => $time = time() + 2,
        ]);

        $client = $this->client($app);
        $base = ['routing' => $this->portalId, 'instance_id' => $this->portalId];
        $this->createEsLo($client, ['id' => 200, 'updated_at' => $time + 3] + $base);
        $this->createEsEnrolment($client, ['id' => 300, 'updated_at' => $time + 3] + $base);
        $this->createEsUser($client, ['id' => 400, 'type' => Schema::O_ACCOUNT, 'updated_at' => $time + 3] + $base);
        $this->createEsEvent($client, ['id' => 500, 'updated_at' => $time + 3, 'parent' => ['id' => 1]] + $base);

        $response = $client->search(['index' => Schema::portalIndex($this->portalId), '_source' => false]);
        $this->assertEquals(4, $response['hits']['total']);
    }

    public function testDeleteAll()
    {
        $app = parent::test();

        /** @var DeprecatedReindexCleanupConsumer $consumer */
        $consumer = $app['consumer.remove_redundant_data'];
        $consumer->consume(IndexService::REINDEX_CLEANUP, (object) ['portalId' => $this->portalId, 'fromDate' => time() + 1]);

        $client = $this->client($app);
        $response = $client->search(['index' => Schema::portalIndex($this->portalId), '_source' => false]);
        $this->assertEquals(0, $response['hits']['total']);
    }
}
