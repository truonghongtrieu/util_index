<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Exception;
use go1\app\DomainService;
use go1\util\es\Schema;
use go1\util_index\task\TaskRepository;
use PHPUnit\Framework\TestCase;

trait IndexServiceTestHelperTrait
{
    protected function indices()
    {
        return [Schema::INDEX];
    }

    protected function assertMissing404Exception(callable $function)
    {
        /** @var TestCase $this */
        try {
            call_user_func($function);
            $this->fail('No exception thrown');
        } catch (Missing404Exception $e) {
            $this->assertTrue($e instanceof Exception);
        }
    }

    protected function assertDocField($value, array $hit, $field = 'id')
    {
        /** @var TestCase $this */
        $this->assertEquals($value, $hit['_source'][$field]);

        return $this;
    }

    protected function client(DomainService $app): Client
    {
        return $app['go1.client.es'];
    }

    protected function db(DomainService $app): Connection
    {
        return $app['dbs']['default'];
    }

    protected function taskRepository(DomainService $app): TaskRepository
    {
        return $app['task.repository'];
    }
}
