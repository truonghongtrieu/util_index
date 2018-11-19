<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema as DBSchema;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use go1\clients\EventClient;
use go1\clients\LoClient;
use go1\clients\MqClient;
use go1\core\lo\event_li\index\tests\EventSchema;
use go1\util\DB;
use go1\util\es\mock\EsInstallTrait;
use go1\util\es\Schema;
use go1\util\schema\AssignmentSchema;
use go1\util\schema\AwardSchema;
use go1\util\schema\CollectionSchema;
use go1\util\schema\ContractSchema;
use go1\util\schema\CouponSchema;
use go1\util\schema\CreditSchema;
use go1\util\schema\EckSchema;
use go1\util\schema\EnrolmentSchema;
use go1\util\schema\InstallTrait;
use go1\util\schema\MailSchema;
use go1\util\schema\MetricSchema;
use go1\util\schema\PaymentSchema;
use go1\util\schema\PolicySchema;
use go1\util\schema\QuizSchema;
use go1\util\user\UserHelper;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexSchema;
use go1\util_index\IndexService;
use go1\util_index\task\TaskRepository;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

abstract class IndexServiceTestCase extends TestCase
{
    use InstallTrait;
    use EsInstallTrait;

    protected $mockMqClientThenConsume = false;
    protected $isUnitTestCase          = false;
    protected $messages;
    protected $logs;

    protected function client(IndexService $app): Client
    {
        return $app['go1.client.es'];
    }

    protected function getApp(): IndexService
    {
        /** @var IndexService $app */
        $app = require __DIR__ . '/../public/index.php';
        $app['waitForCompletion'] = true;

        $app['dbs'] = $app->extend('dbs', function () {
            return [
                'default'          => $db = DriverManager::getConnection(['url' => 'sqlite://sqlite::memory:']),
                'go1'              => $db,
                'assignment'       => $db,
                'social'           => $db,
                'mail'             => $db,
                'portal'           => $db,
                'eck'              => $db,
                'payment'          => $db,
                'quiz'             => $db,
                'collection'       => $db,
                'enrolment'        => $db,
                'credit'           => $db,
                'award'            => $db,
                'vote'             => $db,
                'contract'         => $db,
                'policy'           => $db,
                'staff'            => $db,
                'event'            => $db,
                'go1_write'        => $db,
                'assignment_write' => $db,
                'social_write'     => $db,
                'mail_write'       => $db,
                'portal_write'     => $db,
                'eck_write'        => $db,
                'payment_write'    => $db,
                'quiz_write'       => $db,
                'enrolment_write'  => $db,
                'credit_write'     => $db,
                'award_write'      => $db,
                'vote_write'       => $db,
                'contract_write'   => $db,
            ];
        });

        $app->extend('history.repository', function () {
            $history = $this->getMockBuilder(HistoryRepository::class)->disableOriginalConstructor()->setMethods(['write', 'bulkLog'])->getMock();

            $history
                ->expects($this->any())
                ->method('write')
                ->willReturnCallback(function ($type, $id, $status, $data = null, $timestamp = null) {
                    $this->logs[] = [
                        'message' => !$data ? null : (is_string($data) ? $data : json_encode($data)),
                        'status'  => $status,
                    ];
                });

            $history
                ->expects($this->any())
                ->method('bulkLog')
                ->willReturnCallback(function (array $response) {
                    if ($response['errors']) {
                        foreach ($response['items'] as $item) {
                            foreach ($item as $action => $data) {
                                if (isset($data['error'])) {
                                    $this->logs[] = [
                                        'message' => !$data['error'] ? null : (is_string($data['error']) ? $data['error'] : json_encode($data['error'])),
                                        'status'  => $data['status'],
                                    ];
                                }
                            }
                        }
                    }
                });

            return $history;
        });

        $this->mockMqClientThenConsume
            ? $this->mockMqClientToDoConsume($app)
            : $this->mockMqClient($app);
        $this->appInstall($app);

        return $app;
    }

    protected function mockMqClient(IndexService $app)
    {
        $app->extend('go1.client.mq', function () {
            $mqClient = $this->getMockBuilder(MqClient::class)->disableOriginalConstructor()->setMethods(['queue', 'publish'])->getMock();

            $mqClient
                ->expects($this->any())
                ->method('publish')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            $mqClient
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            return $mqClient;
        });
    }

    protected function mockMqClientToDoConsume(IndexService $app)
    {
        $app->extend('go1.client.mq', function () use ($app) {
            $mock = $this
                ->getMockBuilder(MqClient::class)
                ->disableOriginalConstructor()
                ->setMethods(['queue'])
                ->getMock();

            $mock
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(
                    function ($body, $routingKey, $context) use ($app) {
                        if (IndexService::WORKER_TASK_BULK == $routingKey) {
                            foreach ($body as $msg) {
                                $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                                $req->request->replace(['routingKey' => $msg['routingKey'], 'body' => (object) $msg['body']]);
                                $res = $app->handle($req);
                                $this->assertEquals(204, $res->getStatusCode());
                            }

                            $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                            $req->request->replace(['routingKey' => $routingKey, 'body' => (object) [], 'context' => $context]);
                            $res = $app->handle($req);
                            $this->assertEquals(204, $res->getStatusCode());

                            return true;
                        }

                        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                        $req->request->replace(['routingKey' => $routingKey, 'body' => (object) $body]);
                        $res = $app->handle($req);
                        $this->assertEquals(204, $res->getStatusCode());

                        return true;
                    }
                );

            return $mock;
        });
    }

    protected function indices()
    {
        return [Schema::INDEX];
    }

    protected function appInstall(IndexService $app)
    {
        $app->extend('go1.client.lo', function () {
            $client = $this
                ->getMockBuilder(LoClient::class)
                ->setMethods(['eventAvailableSeat'])
                ->disableOriginalConstructor()
                ->getMock();

            $client
                ->expects($this->any())
                ->method('eventAvailableSeat')
                ->willReturn(12); # The logic is not important here.

            return $client;
        });

        $app->extend('go1.client.event', function () {
            $client = $this
                ->getMockBuilder(EventClient::class)
                ->setMethods(['getAvailableSeats'])
                ->disableOriginalConstructor()
                ->getMock();

            $client
                ->expects($this->any())
                ->method('getAvailableSeats')
                ->willReturn(12);

            return $client;
        });

        $this->installGo1Schema($app['dbs']['go1'], $coreOnly = false);

        if (!$this->isUnitTestCase) {
            $client = $this->client($app);

            $indices = $this->indices();
            foreach ($indices as $index) {
                if ($client->indices()->exists(['index' => $index])) {
                    $client->indices()->delete(['index' => $index]);
                }
            }
            $this->installEs($client, $indices);
        }

        DB::install($app['dbs']['assignment'], [
            function (DBSchema $schema) {
                AssignmentSchema::install($schema);
                MailSchema::install($schema);
                EckSchema::install($schema);
                PaymentSchema::install($schema);
                EnrolmentSchema::installManualRecord($schema);
                CouponSchema::install($schema);
                CreditSchema::install($schema);
                AwardSchema::install($schema);
                ContractSchema::install($schema);
                MetricSchema::install($schema);
                IndexSchema::install($schema);
                CollectionSchema::install($schema);
                PolicySchema::install($schema);
            },
        ]);

        DB::install($app['dbs']['quiz'], [
            function (DBSchema $schema) {
                QuizSchema::install($schema);
            },
        ]);

        DB::install($app['dbs']['event'], [
            function (DBSchema $schema) {
                EventSchema::install($schema);
            },
        ]);
    }

    protected function db(IndexService $app): Connection
    {
        return $app['dbs']['default'];
    }

    protected function taskRepository(IndexService $app): TaskRepository
    {
        return $app['task.repository'];
    }

    protected function assertMissing404Exception(callable $function)
    {
        try {
            call_user_func($function);
            $this->fail('No exception thrown');
        } catch (Missing404Exception $e) {
            $this->assertTrue($e instanceof \Exception);
        }
    }

    protected function assertDocField($value, array $hit, $field = 'id')
    {
        $this->assertEquals($value, $hit['_source'][$field]);

        return $this;
    }
}
