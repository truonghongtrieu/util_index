<?php

namespace go1\util_index\tests;

use go1\index\products\IndexService;
use go1\util\es\Schema;
use go1\util\schema\mock\PortalMockTrait;
use go1\util\schema\mock\UserMockTrait;
use go1\util\user\UserHelper;
use go1\util_index\task\Task;
use Symfony\Component\HttpFoundation\Request;

class ReindexBulkMessageTest extends IndexTestCase
{
    use UserMockTrait;
    use PortalMockTrait;

    protected $mockMqClientThenConsume = true;
    private   $portalId                = 100;
    private   $portalName              = 'z.go1.co';

    protected function appInstall(IndexService $app)
    {
        parent::appInstall($app);

        $go1 = $app['dbs']['go1'];
        $this->createPortal($go1, ['id' => $this->portalId, 'title' => $this->portalName]);

        $rangeSize = 2000;
        for ($offset = 0; $offset < 150; ++$offset) {
            $accountId = $offset + $rangeSize;
            $userId = $offset + $rangeSize + 1;
            $this->createUser($go1, ['id' => $accountId, 'mail' => "$accountId@mail.co", 'instance' => $this->portalName]);
            $this->createUser($go1, ['id' => $userId, 'mail' => "$accountId@mail.co", 'instance' => $app['accounts_name']]);
            $rangeSize += 1000;
        }
        $app['repository.es']->installPortalIndex($this->portalId);
    }

    public function test()
    {
        $app = $this->getApp();
        $req = Request::create('/task?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $indexName = sprintf('%s_%s', getenv('ES_INDEX') ?: 'go1_dev', time());
        $req->request->replace(['title' => 'reindex', 'index' => $indexName, 'max_num_items' => 2]);
        $res = $app->handle($req);

        $this->assertEquals(200, $res->getStatusCode());
        $taskId = json_decode($res->getContent())->id;
        $repo = $this->taskRepository($app);
        $task = $repo->load($taskId);

        $this->client($app)->indices()->refresh(['index' => Schema::INDEX]);
        $result = $this->client($app)->count(['index' => Schema::INDEX, 'type' => Schema::O_ACCOUNT]);
        $this->assertEquals(100, $task->percent);
        $this->assertEquals(Task::FINISHED, $task->status);
        $this->assertEquals(150, $result['count']);

        return $app;
    }
}
