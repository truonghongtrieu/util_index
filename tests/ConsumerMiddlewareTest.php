<?php

namespace go1\util_index\tests;

use go1\util\es\Schema;
use go1\util\queue\Queue;
use go1\util\schema\mock\PortalMockTrait;
use go1\util\user\UserHelper;
use go1\util_index\IndexService;
use Symfony\Component\HttpFoundation\Request;

class ConsumerMiddlewareTest extends IndexServiceTestCase
{
    use PortalMockTrait;

    private $portalId;
    private $legacyPortalId;

    protected function appInstall(IndexService $app)
    {
        parent::appInstall($app);

        $db = $app['dbs']['go1'];
        $this->legacyPortalId = $this->createPortal($db, ['title' => 'legacy.mygo1.com', 'version' => 'v2.10.0']);
        $this->portalId = $this->createPortal($db, ['title' => 'qa.mygo1.com']);
    }

    public function testSkipLegacy()
    {
        $app = $this->getApp();
        $client = $this->client($app);

        # Portal Id
        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
                'routingKey' => Queue::USER_CREATE,
                'body'       => (object) ['instance_id' => $this->legacyPortalId],
            ]
        );
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
        $this->assertFalse($client->indices()->exists(['index' => Schema::portalIndex($this->legacyPortalId)]));

        # Instance Name
        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'routingKey' => Queue::USER_CREATE,
            'body'       => (object) ['instance' => 'legacy.mygo1.com'],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
        $this->assertFalse($client->indices()->exists(['index' => Schema::portalIndex($this->legacyPortalId)]));

        # Portal ID is zero
        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'routingKey' => Queue::ENROLMENT_CREATE,
            'body'       => (object) ['taken_instance_id' => 0],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
        $this->assertFalse($client->indices()->exists(['index' => Schema::portalIndex($this->legacyPortalId)]));
    }

    public function test()
    {
        $app = $this->getApp();

        # Portal ID
        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'routingKey' => 'foo.key',
            'body'       => (object) ['instance_id' => $this->portalId],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());

        # @TODO
        # $client = $this->client($app);
        # $this->assertTrue($client->indices()->exists(['index' => Schema::portalIndex($this->portalId)]));

        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace([
            'routingKey' => 'foo.key',
            'body'       => (object) ['instance' => 'qa.mygo1.com'],
        ]);
        $res = $app->handle($req);
        $this->assertEquals(204, $res->getStatusCode());
        # @TODO
        # $this->assertTrue($client->indices()->exists(['index' => Schema::portalIndex($this->instanceId)]));
    }
}
