<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\util\es\Schema;

class InstallTest extends IndexServiceTestCase
{
    public function test()
    {
        /** @var Connection $db */
        /** @var Client $client */
        $app = $this->getApp();
        $db = $app['dbs']['default'];
        $client = $app['go1.client.es'];

        $this->assertTrue($db->getSchemaManager()->tablesExist(['index_history', 'index_kv']));
        $this->assertEquals($client->indices()->getMapping(['index' => Schema::INDEX]), [Schema::INDEX => ['mappings' => Schema::MAPPING]]);
    }
}
