<?php

namespace go1\util_index\tests;

use Elasticsearch\Client;
use go1\core\learning_record\enrolment\index\EnrolmentReindex;
use go1\core\learning_record\enrolment\index\EnrolmentRevisionReindex;
use go1\core\lo\event_li\index\EventReindex;
use go1\core\lo\index\reindex\LoContentSharingReindex;
use go1\core\lo\index\reindex\LoReindex;
use go1\core\lo\index\reindex\LoShareReindex;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\es\mock\EsEnrolmentMockTrait;
use go1\util\es\mock\EsEventMockTrait;
use go1\util\es\mock\EsLoMockTrait;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use go1\util\schema\mock\PortalMockTrait;
use go1\util_index\core\LoShareConsumer;
use go1\util_index\IndexService;
use go1\util_index\task\Task;

class ReindexRemoveRedundantTest extends IndexServiceTestCase
{
    use EsLoMockTrait;
    use EsEnrolmentMockTrait;
    use PortalMockTrait;
    use EsEventMockTrait;

    private $portalId1 = 100;
    private $portalId2 = 200;
    private $portalBase1;
    private $portalBase2;

    protected function appInstall(IndexService $app)
    {
        parent::appInstall($app);

        $go1 = $app['dbs']['go1'];
        $this->createPortal($go1, ['id' => $this->portalId1, 'title' => '1.go1.co']);
        $this->createPortal($go1, ['id' => $this->portalId2, 'title' => '2.go1.co']);
        $this->installPortalIndex($this->client($app), $this->portalId1);
        $this->installPortalIndex($this->client($app), $this->portalId2);

        $this->portalBase1 = ['instance_id' => $this->portalId1, 'routing' => $this->portalId1];
        $this->portalBase2 = ['instance_id' => $this->portalId2, 'routing' => $this->portalId2];
    }

    private function deleteByQuery(Client $client, $query)
    {
        $client->deleteByQuery([
            'index'               => Schema::INDEX,
            'body'                => ['query' => $query->toArray()],
            'refresh'             => true,
            'wait_for_completion' => true,
        ]);
    }

    private function assertNumOfDoc(Client $client, $portalId, $type, $expectedNum)
    {
        $hit = $client->count(['index' => Schema::portalIndex($portalId), 'type' => $type]);
        $this->assertEquals($expectedNum, $hit['count']);

        return $this;
    }

    public function testLearningObject()
    {
        /** @var LoReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.lo'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $this->portalId1);
        $task->created = 1000;

        $this->createEsLo($client, ['id' => 10, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 11, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 12, 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 13, 'updated_at' => 1001] + $this->portalBase1);
        # shared course
        $this->createEsLo($client, ['id' => 14, 'updated_at' => 800] + ['instance_id' => $this->portalId2] + $this->portalBase1);

        $this->createEsLo($client, ['id' => 20] + $this->portalBase2);
        $this->createEsLo($client, ['id' => 21] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 5)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 3)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);
    }

    public function testLoContentSharing()
    {
        /** @var LoContentSharingReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.lo_content_sharing'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $this->portalId1);
        $task->created = 1000;

        $this->portalBase1['instance_id'] = $this->portalId2;
        $this->portalBase1['metadata']['shared'] = 1;

        $this->portalBase2['metadata']['shared_passive'] = 1;
        $this->portalBase2['instance_id'] = $this->portalId1;
        $this->createEsLo($client, ['id' => 10, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 11, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 12, 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsLo($client, ['id' => 13, 'updated_at' => 1001] + $this->portalBase1);

        $this->createEsLo($client, ['id' => 20] + $this->portalBase2);
        $this->createEsLo($client, ['id' => 21] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 4)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 2)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);
    }

    public function testLoShare()
    {
        /** @var LoShareReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.lo_share'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $this->portalId1);
        $task->created = 1000;

        $this->portalBase1['instance_id'] = $this->portalId2;
        $this->portalBase2['instance_id'] = $this->portalId1;
        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId1, 10), 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId1, 11), 'updated_at' => 900] + $this->portalBase1);
        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId1, 12), 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId1, 13), 'updated_at' => 1001] + $this->portalBase1);

        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId2, 20)] + $this->portalBase2);
        $this->createEsLo($client, ['id' => LoShareConsumer::id($this->portalId2, 21)] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 4)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_LO, 2)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_LO, 2);
    }

    public function testEnrolment()
    {
        /** @var EnrolmentReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.enrolment'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $this->portalId1);
        $task->created = 1000;

        $this->createEsEnrolment($client, ['id' => 10, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsEnrolment($client, ['id' => 11, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsEnrolment($client, ['id' => 12, 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsEnrolment($client, ['id' => 13, 'updated_at' => 1001] + $this->portalBase1);
        $this->createEsEnrolment($client, ['type' => EnrolmentTypes::TYPE_AWARD, 'id' => 14, 'updated_at' => 900] + $this->portalBase1);

        $this->createEsEnrolment($client, ['id' => 20] + $this->portalBase2);
        $this->createEsEnrolment($client, ['id' => 21] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_ENROLMENT, 5)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_ENROLMENT, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_ENROLMENT, 3)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_ENROLMENT, 2);
    }

    public function testEnrolmentRevision()
    {
        /** @var EnrolmentRevisionReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.enrolment_revision'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $this->portalId1);
        $task->created = 1000;

        $this->createEsRevisionEnrolment($client, ['id' => 10, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsRevisionEnrolment($client, ['id' => 11, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsRevisionEnrolment($client, ['id' => 12, 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsRevisionEnrolment($client, ['id' => 13, 'updated_at' => 1001] + $this->portalBase1);

        $this->createEsRevisionEnrolment($client, ['id' => 20] + $this->portalBase2);
        $this->createEsRevisionEnrolment($client, ['id' => 21] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_ENROLMENT_REVISION, 4)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_ENROLMENT_REVISION, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_ENROLMENT_REVISION, 2)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_ENROLMENT_REVISION, 2);
    }

    public function dataEvent()
    {
        return [[$this->portalId1, 2, 2], [null, 2, 0]];
    }

    /**
     * @dataProvider dataEvent
     */
    public function testEvent($portalId, $numberDoc1, $numberDoc2)
    {
        /** @var EventReindex $consumer */
        $app = $this->getApp();
        $go1 = $app['dbs']['go1'];
        $client = $this->client($app);
        $consumer = $app['reindex.handler.event'];

        $task = Task::create((object) []);
        $task->portal = PortalHelper::load($go1, $portalId);
        $task->created = 1000;

        $this->createEsEvent($client, ['id' => 10, 'lo_id' => 10, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsEvent($client, ['id' => 11, 'lo_id' => 11, 'updated_at' => 900] + $this->portalBase1);
        $this->createEsEvent($client, ['id' => 12, 'lo_id' => 12, 'updated_at' => 1000] + $this->portalBase1);
        $this->createEsEvent($client, ['id' => 13, 'lo_id' => 13, 'updated_at' => 1001] + $this->portalBase1);

        $this->createEsEvent($client, ['id' => 20, 'lo_id' => 14, 'updated_at' => 900] + $this->portalBase2);
        $this->createEsEvent($client, ['id' => 21, 'lo_id' => 15, 'updated_at' => 900] + $this->portalBase2);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_EVENT, 4)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_EVENT, 2);

        $query = $consumer->removeRedundant($task);
        $this->deleteByQuery($client, $query);

        $this
            ->assertNumOfDoc($client, $this->portalId1, Schema::O_EVENT, $numberDoc1)
            ->assertNumOfDoc($client, $this->portalId2, Schema::O_EVENT, $numberDoc2);
    }
}
