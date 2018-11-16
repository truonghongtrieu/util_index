<?php

namespace go1\util_index\tests;

use go1\index\domain\reindex\handler\UserReindex;
use go1\index\products\App;
use go1\util\eck\mock\EckMockTrait;
use go1\util\payment\mock\PaymentMockTrait;
use go1\util\schema\mock\AssignmentMockTrait;
use go1\util\schema\mock\EnrolmentMockTrait;
use go1\util\schema\mock\GroupMockTrait;
use go1\util\schema\mock\LoMockTrait;
use go1\util\schema\mock\PortalMockTrait;
use go1\util\schema\mock\QuizMockTrait;
use go1\util\schema\mock\UserMockTrait;
use go1\util\user\UserHelper;
use GuzzleHttp\Client;
use Symfony\Component\HttpFoundation\Request;

class TaskTest extends IndexTestCase
{
    use UserMockTrait;
    use PortalMockTrait;
    use LoMockTrait;
    use EnrolmentMockTrait;
    use AssignmentMockTrait;
    use GroupMockTrait;
    use EckMockTrait;
    use PaymentMockTrait;
    use QuizMockTrait;

    private $portalName     = 'a.mygo1.com';
    private $portalId;
    private $userAProfileId = 101;
    private $userBProfileId = 202;
    private $userIds;
    private $accountIds;
    private $taskId;

    public function appInstall(App $app)
    {
        parent::appInstall($app);

        ini_set('xdebug.max_nesting_level', 1000);
        $db = $app['dbs']['go1'];

        $this->createPortal($db, ['title' => $app['accounts_name']]);
        $this->portalId = $this->createPortal($db, ['title' => $this->portalName]);
        $this->createUser($db, ['mail' => 'user.0@a.mygo1.com', 'instance' => $app['accounts_name']]);
        $this->createUser($db, ['mail' => 'user.1@a.mygo1.com', 'instance' => $app['accounts_name']]);
        $this->accountIds[] = $this->createUser($db, ['mail' => 'userA@mail.com', 'profile_id' => $this->userAProfileId, 'instance' => $this->portalName]);
        $this->accountIds[] = $this->createUser($db, ['mail' => 'userB@mail.com', 'profile_id' => $this->userBProfileId, 'instance' => $this->portalName]);
        $this->userIds[] = $this->createUser($db, ['mail' => 'userA@mail.com', 'profile_id' => $this->userAProfileId, 'instance' => $app['accounts_name']]);
        $this->userIds[] = $this->createUser($db, ['mail' => 'userB@mail.com', 'profile_id' => $this->userBProfileId, 'instance' => $app['accounts_name']]);
    }

    public function testCreate()
    {
        $app = $this->getApp();
        $req = Request::create("/task?jwt=" . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace(['title' => 'foo', 'remove_redundant' => 1]);
        $res = $app->handle($req);
        $this->assertEquals(200, $res->getStatusCode());

        $this->taskId = json_decode($res->getContent())->id;
        $repo = $this->taskRepository($app);
        $task = $repo->load($this->taskId);
        $this->assertEquals(UserReindex::NAME, $task->currentHandler);
        $this->assertEquals(1, $task->removeRedundant);
        $msg = $this->messages[App::WORKER_TASK_BULK][0][0]['body'];
        $this->assertEquals($msg['handler'], $task->currentHandler);
        $this->assertEquals($msg['id'], $task->id);
        $this->assertEquals($msg['currentOffset'], 0);
        $this->assertEquals($msg['currentIdFromOffset'], 0);

        return $app;
    }

    public function testMessageDispatching()
    {
        $this->isUnitTestCase = true;
        $app = $this->getApp();

        $app->extend('client', function () {
            $http = $this
                ->getMockBuilder(Client::class)
                ->setMethods(['post'])
                ->getMock();

            $http
                ->expects($this->any())
                ->method('post')
                ->willReturnCallback(
                    function (string $url) {
                        $this->assertEquals($url, 'http://my-team.dev.go1.service/reindex?jwt=' . UserHelper::ROOT_JWT);
                    }
                );

            return $http;
        });

        $req = Request::create("/task?jwt=" . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'title'    => 'foo',
            'handlers' => ['#my-team'],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
    }

    public function testMessageDispatchingForCertainPortal()
    {
        $this->isUnitTestCase = true;
        $app = $this->getApp();

        $app->extend('client', function () {
            $http = $this
                ->getMockBuilder(Client::class)
                ->setMethods(['post'])
                ->getMock();

            $http
                ->expects($this->any())
                ->method('post')
                ->willReturnCallback(
                    function (string $url) {
                        $this->assertEquals($url, 'http://my-team.dev.go1.service/reindex?jwt=' . UserHelper::ROOT_JWT . '&portalId=' . $this->portalId);
                    }
                );

            return $http;
        });

        $req = Request::create("/task?jwt=" . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'title'    => 'foo',
            'instance' => $this->portalName,
            'handlers' => ['#my-team'],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
    }

    public function testHandleTask()
    {
        $repo = $this->taskRepository($app = $this->testCreate());
        $task = $repo->load($this->taskId);
        $this->assertEquals(UserReindex::NAME, $task->currentHandler);
        $this->messages = [];

        $req = Request::create("/consume?jwt=" . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'routingKey' => App::WORKER_TASK_PROCESS,
            'body'       => (object) [
                'id'                  => $this->taskId,
                'handler'             => $task->currentHandler,
                'currentOffset'       => 0,
                'currentIdFromOffset' => 0,
            ],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());

        $task = $repo->load($this->taskId);
        $this->assertEquals(UserReindex::NAME, $task->currentHandler);
        $this->assertEmpty($this->messages);
    }

    public function testVerify()
    {
        $this->taskRepository($app = $this->testCreate());
        $this->messages = [];
        $req = Request::create("/task/$this->taskId/verify?jwt=" . UserHelper::ROOT_JWT, 'GET');
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());

        $this->assertCount(1, $this->messages[App::WORKER_TASK_BULK]);
    }
}
