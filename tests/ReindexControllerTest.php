<?php

namespace go1\util_index\tests;

use DateTime as DefaultDateTime;
use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\core\customer\user\index\reindex\AccountReindex;
use go1\core\learning_record\enrolment\index\reindex\EnrolmentReindex;
use go1\core\lo\award\index\consumer\AwardEnrolmentConsumer;
use go1\core\lo\event_li\index\tests\mocks\EventEnrolmentMockTrait;
use go1\core\lo\index\reindex\LoReindex;
use go1\util\award\AwardEnrolmentStatuses;
use go1\util\award\AwardHelper;
use go1\util\award\AwardItemTypes;
use go1\util\Country;
use go1\util\credit\CreditStatuses;
use go1\util\eck\mock\EckMockTrait;
use go1\util\edge\EdgeTypes;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\es\Schema;
use go1\util\group\GroupItemTypes;
use go1\util\group\GroupTypes;
use go1\util\lo\LiTypes;
use go1\util\lo\LoStatuses;
use go1\util\lo\LoTypes;
use go1\util\lo\TagTypes;
use go1\util\payment\mock\PaymentMockTrait;
use go1\util\plan\PlanTypes;
use go1\util\schema\mock\AwardMockTrait;
use go1\util\schema\mock\ContractMockTrait;
use go1\util\schema\mock\CreditMockTrait;
use go1\util\schema\mock\EnrolmentMockTrait;
use go1\util\schema\mock\GroupMockTrait;
use go1\util\schema\mock\LoMockTrait;
use go1\util\schema\mock\MetricMockTrait;
use go1\util\schema\mock\PlanMockTrait;
use go1\util\schema\mock\PortalMockTrait;
use go1\util\schema\mock\QuizMockTrait;
use go1\util\schema\mock\UserMockTrait;
use go1\util\user\UserHelper;
use go1\util_index\core\consumer\LoContentSharingConsumer;
use go1\util_index\core\consumer\LoShareConsumer;
use go1\util_index\IndexService;
use go1\util_index\task\Task;
use Symfony\Component\HttpFoundation\Request;

class ReindexControllerTest extends IndexServiceTestCase
{
    use UserMockTrait;
    use PortalMockTrait;
    use LoMockTrait;
    use EnrolmentMockTrait;
    use GroupMockTrait;
    use EckMockTrait;
    use PaymentMockTrait;
    use QuizMockTrait;
    use CreditMockTrait;
    use AwardMockTrait;
    use PlanMockTrait;
    use ContractMockTrait;
    use MetricMockTrait;
    use EventEnrolmentMockTrait;

    protected $mockMqClientThenConsume = true;
    private   $portalName              = 'a.mygo1.com';
    private   $portalName2             = 'c.mygo1.com';
    private   $portalId;
    private   $portalId2;
    private   $portalB                 = 'b.mygo1.com';
    private   $portalIdB;
    private   $userAProfileId          = 101;
    private   $userBProfileId          = 202;
    private   $loId;
    private   $loId2;
    private   $enrolmentIds;
    private   $enrolmentRevisionIds;
    private   $transactionIds;
    private   $userIds;
    private   $accountIds;
    private   $groupId;
    private   $recordIds;
    private   $couponIds;
    private   $loGroupIds;
    private   $creditIds;
    private   $courseEventId;
    private   $courseArchiveEventId;
    private   $liEventId1;
    private   $liEventId2;
    private   $liEventId3;
    private   $liEventId4;
    private   $events;
    private   $eventSessions;
    private   $eventEnrolmentIds;
    private   $awardIds;
    private   $awardItemIds;
    private   $awardItemManualIds;
    private   $awardEnrolmentIds;
    private   $awardEnrolmentRevisionIds;
    private   $awardItemEnrolmentIds;
    private   $awardPlanIds;
    private   $planIds;
    private   $userIdWithoutEnrollments;
    private   $courseSharingId;
    private   $membershipEnrolmentId;
    private   $membershipPlanId;
    private   $awardPlanId;
    private   $contractIds;
    private   $metricIds;
    private   $quizUserAnswerIds;
    private   $quizESUserAnswers;
    private   $courseIdsB;

    private function createManualRecord(Connection $db, array $options = [])
    {
        $db->insert('enrolment_manual', [
            'instance_id' => $options['instance_id'] ?? 1,
            'entity_type' => $options['entity_type'] ?? 'lo',
            'entity_id'   => $options['entity_id'] ?? 1,
            'user_id'     => $options['user_id'] ?? 1,
            'verified'    => isset($options['verified']) ? (int) $options['verified'] : 0,
            'data'        => $options['data'] ?? json_encode([]),
            'created'     => $options['created'] ?? time(),
            'updated'     => $options['updated'] ?? time(),
        ]);

        return $db->lastInsertId('enrolment_manual');
    }

    private function createRevision(Connection $db, int $enrolmentId)
    {
        $enrolment = EnrolmentHelper::load($db, $enrolmentId);
        $db->insert('gc_enrolment_revision', [
            'enrolment_id'      => $enrolmentId,
            'start_date'        => '2017-02-21 04:10:00',
            'end_date'          => '2017-03-21 04:10:00',
            'status'            => 'completed',
            'result'            => 90,
            'pass'              => 1,
            'note'              => 'Foo',
            'profile_id'        => $enrolment->profile_id,
            'lo_id'             => $enrolment->lo_id,
            'parent_lo_id'      => $enrolment->parent_lo_id,
            'instance_id'       => $enrolment->instance_id,
            'taken_instance_id' => $enrolment->taken_instance_id,
        ]);

        return $db->lastInsertId('gc_enrolment_revision');
    }

    private function createAwardEnrolmentRevision(Connection $db, int $awardEnrolmentId)
    {
        $awardEnrolment = AwardHelper::loadEnrolment($db, $awardEnrolmentId);
        $db->insert('award_enrolment_revision', [
            'award_enrolment_id' => $awardEnrolmentId,
            'award_id'           => $awardEnrolment->award_id,
            'user_id'            => $awardEnrolment->user_id,
            'start_date'         => time() - 150,
            'end_date'           => time() - 100,
            'created'            => time(),
            'status'             => AwardEnrolmentStatuses::COMPLETED,
            'quantity'           => 90,
        ]);

        return $db->lastInsertId('award_enrolment_revision');
    }

    private function createEvents(Connection $db)
    {
        $location = [
            'instance_id'             => $this->portalId,
            'country'                 => 'AU',
            'administrative_area'     => 'foo',
            'sub_administrative_area' => 'bar',
            'locality'                => null,
            'dependent_locality'      => null,
            'postal_code'             => null,
            'thoroughfare'            => null,
            'premise'                 => null,
            'sub_premise'             => null,
            'organisation_name'       => null,
            'name_line'               => null,
        ];

        $this->events = $events = [
            [
                'start'    => (new \DateTime('1 week'))->format(DATE_ISO8601),
                'end'      => (new \DateTime('2 days'))->format(DATE_ISO8601),
                'timezone' => 'UTC',
                'location' => $location,
            ],
            [
                'start'    => (new \DateTime('3 week'))->format(DATE_ISO8601),
                'end'      => (new \DateTime('4 days'))->format(DATE_ISO8601),
                'timezone' => 'UTC',
                'location' => $location,
            ],
            [
                'start'    => (new \DateTime('5 week'))->format(DATE_ISO8601),
                'end'      => (new \DateTime('6 days'))->format(DATE_ISO8601),
                'timezone' => 'UTC',
            ],
        ];

        $this->courseEventId = $this->createCourse($db, ['title' => 'A.course event', 'instance_id' => $this->portalId, 'event' => $events[0]]);
        $this->liEventId1 = $this->createLO($db, ['title' => 'A.event 1', 'instance_id' => $this->portalId, 'event' => $events[1], 'type' => LiTypes::EVENT]);
        $this->liEventId2 = $this->createLO($db, ['title' => 'A.event 2', 'instance_id' => $this->portalId, 'event' => $events[2], 'type' => LiTypes::EVENT]);
        $this->link($db, EdgeTypes::HAS_LI, $this->courseEventId, $this->liEventId1);
        $this->link($db, EdgeTypes::HAS_LI, $this->courseEventId, $this->liEventId2);

        $this->courseArchiveEventId = $this->createCourse($db, ['title' => 'archive.course event', 'instance_id' => $this->portalId, 'event' => $events[0]]);
        $this->liEventId3 = $this->createLO($db, ['title' => 'archive.event 3', 'instance_id' => $this->portalId, 'event' => $events[1], 'type' => LiTypes::EVENT]);
        $this->link($db, EdgeTypes::HAS_LI, $this->courseArchiveEventId, $this->liEventId3);
        $this->liEventId4 = $this->createLO($db, ['title' => 'archive.event 4', 'instance_id' => $this->portalId, 'event' => $events[2], 'type' => LiTypes::EVENT]);
        $this->link($db, EdgeTypes::HAS_LI, $this->courseArchiveEventId, $this->liEventId4);
    }

    private function createEventSessions(Connection $event)
    {
        $event->insert('event_location', $location = [
            'id'                      => 111,
            'title'                   => 'Event location',
            'portal_id'               => $this->portalId,
            'country'                 => 'AU',
            'administrative_area'     => 'foo',
            'sub_administrative_area' => 'bar',
            'locality'                => null,
            'dependent_locality'      => null,
            'postal_code'             => null,
            'thoroughfare'            => null,
            'premise'                 => null,
            'sub_premise'             => null,
            'organisation_name'       => null,
            'name_line'               => null,
            'is_online'               => 1,
            'published'               => 1,
            'created_time'            => time(),
        ]);

        $this->eventSessions = [
            [
                'id'             => 222,
                'title'          => 'Event session 1',
                'lo_id'          => $this->liEventId1,
                'start_at'       => (new DefaultDateTime('9 week'))->format(DATE_ATOM),
                'end_at'         => (new DefaultDateTime('10 days'))->format(DATE_ATOM),
                'timezone'       => 'UTC',
                'instructor_ids' => serialize([1]),
                'location'       => (object) $location,
            ],
            [
                'id'             => 333,
                'title'          => 'Event session 2',
                'lo_id'          => $this->liEventId2,
                'start_at'       => (new DefaultDateTime('11 week'))->format(DATE_ATOM),
                'end_at'         => (new DefaultDateTime('12 days'))->format(DATE_ATOM),
                'timezone'       => 'UTC',
                'instructor_ids' => serialize([1]),
                'location'       => (object) $location,
            ],
        ];

        foreach ($this->eventSessions as $eventSession) {
            $event->insert('event_session', [
                'id'             => $eventSession['id'],
                'title'          => $eventSession['title'],
                'portal_id'      => $this->portalId,
                'lo_id'          => $eventSession['lo_id'],
                'location_id'    => 111,
                'start_at'       => $eventSession['start_at'],
                'end_at'         => $eventSession['end_at'],
                'timezone'       => $eventSession['timezone'],
                'instructor_ids' => $eventSession['instructor_ids'],
                'published'      => 1,
                'created_time'   => time(),
            ]);
        }
    }

    protected function appInstall(IndexService $app)
    {
        ini_set('xdebug.max_nesting_level', 1000);
        parent::appInstall($app);

        $db = $app['dbs']['go1'];
        $dbAward = $app['dbs']['award'];
        $dbEvent = $app['dbs']['event'];

        $this->createPortal($db, ['title' => $app['accounts_name']]);
        $this->portalId = $this->createPortal($db, ['title' => $this->portalName]);
        $this->portalId2 = $this->createPortal($db, ['title' => $this->portalName2]);
        $this->portalIdB = $this->createPortal($db, ['title' => $this->portalB]);
        $this->createPortalPublicKey($db, ['instance' => $this->portalName]);
        $this->createPortalPrivateKey($db, ['instance' => $this->portalName]);
        $this->createPortalPublicKey($db, ['instance' => $app['accounts_name']]);
        $this->createPortalPrivateKey($db, ['instance' => $app['accounts_name']]);
        $this->createPortalConfig($db, ['instance' => $this->portalName, 'name' => $this->portalId]);
        $this->createUser($db, ['mail' => 'user.0@a.mygo1.com', 'instance' => $app['accounts_name']]);
        $this->createUser($db, ['mail' => 'user.1@a.mygo1.com', 'instance' => $app['accounts_name']]);
        $this->accountIds[] = $this->createUser($db, ['mail' => 'userA@mail.com', 'profile_id' => $this->userAProfileId, 'instance' => $this->portalName]);
        $this->accountIds[] = $this->createUser($db, ['mail' => 'userB@mail.com', 'profile_id' => $this->userBProfileId, 'instance' => $this->portalName]);
        $this->userIds[] = $this->createUser($db, ['mail' => 'userA@mail.com', 'profile_id' => $this->userAProfileId, 'instance' => $app['accounts_name']]);
        $this->userIds[] = $this->createUser($db, ['mail' => 'userB@mail.com', 'profile_id' => $this->userBProfileId, 'instance' => $app['accounts_name']]);
        $this->userIdWithoutEnrollments = $this->createUser($db, ['mail' => 'userC@mail.com', 'profile_id' => 303, 'instance' => $app['accounts_name']]);

        $this->loId = $this->createCourse($db, ['title' => 'Course A', 'instance_id' => $this->portalId, 'price' => ['price' => 20], 'tags' => '[tag A] [tag B]']);
        $this->loId2 = $this->createCourse($db, ['title' => 'Course B', 'instance_id' => $this->portalId, 'tags' => '[tag B] [tag C]']);
        $this->enrolmentIds[] = $this->createEnrolment($db, ['profile_id' => $this->userAProfileId, 'lo_id' => $this->loId, 'taken_instance_id' => $this->portalId]);
        $this->enrolmentIds[] = $this->createEnrolment($db, ['profile_id' => $this->userBProfileId, 'lo_id' => $this->loId, 'taken_instance_id' => $this->portalId]);

        $body = json_decode(file_get_contents(APP_ROOT . '/tests/fixtures/event/create/1.json'))->body;
        $this->eventEnrolmentIds[] = $this->createEventEnrolment($app['dbs']['event'], (array) $body);

        foreach ($this->enrolmentIds as $enrolmentId) {
            $this->enrolmentRevisionIds[] = $this->createRevision($db, $enrolmentId);
        }

        $this->createEvents($db);
        $this->createEventSessions($dbEvent);

        $this->transactionIds[] = $this->createTransaction($db, []);
        $this->createTransactionItem($db, ['transaction_id' => $this->transactionIds[0], 'product_id' => $this->loId, 'data' => json_encode(['title' => 'Course A'])]);
        $this->transactionIds[] = $this->createTransaction($db);
        $this->createTransactionItem($db, ['transaction_id' => $this->transactionIds[1], 'product_id' => $this->courseEventId, 'data' => json_encode(['title' => 'A.course event'])]);
        $this->createTransactionItem($db, ['transaction_id' => $this->transactionIds[1], 'product_id' => $this->liEventId1, 'data' => json_encode(['title' => 'A.event 1'])]);

        $this->groupId = $this->createGroup($db, ['instance_id' => $this->portalId]);

        $this->createField($db, ['instance' => $this->portalName, 'field' => 'field_foo', 'entity' => 'account']);
        $this->createField($db, ['instance' => $this->portalName, 'field' => 'field_baz', 'entity' => 'account', 'type' => 'integer']);
        $this->createField($db, ['instance' => $this->portalName, 'field' => 'field_bar', 'entity' => 'lo']);

        $this->createEntityValues($db, $this->portalName, 'account', $this->accountIds[0], ['field_foo' => [0 => 'bar', 1 => 'bar 2'], 'field_baz' => [0 => 123, 1 => 234]]);
        $this->createEntityValues($db, $this->portalName, 'account', $this->accountIds[1], ['field_foo' => [0 => 'bar', 1 => 'bar 2'], 'field_baz' => [0 => 123, 1 => 234]]);
        $this->createEntityValues($db, $this->portalName, 'lo', $this->loId, ['field_bar' => [0 => 'foo']]);

        $quiz = $app['dbs']['quiz'];
        $questionUuid = 844391;
        $alternatives = [
            ['answer' => 'answer 1', 'id' => 'answer1'],
            ['answer' => 'answer 2', 'id' => 'answer2'],
            ['answer' => 'answer 3', 'id' => 'answer3'],
            ['answer' => 'answer 4', 'id' => 'answer4'],
        ];
        $matches = [
            ['left_text' => 'answer 1', 'left_id' => 'answer1', 'right_text' => 'question 1', 'right_id' => 'question1'],
            ['left_text' => 'answer 2', 'left_id' => 'answer2', 'right_text' => 'question 2', 'right_id' => 'question2'],
            ['left_text' => 'answer 3', 'left_id' => 'answer3', 'right_text' => 'question 3', 'right_id' => 'question3'],
            ['left_text' => 'answer 4', 'left_id' => 'answer4', 'right_text' => 'question 4', 'right_id' => 'question4'],
        ];
        $this->createQuizQuestionRevision($quiz, ['ruuid' => $questionUuid, 'data' => json_encode(['li_id' => 123, 'parent_lo_id' => 234]), 'config' => json_encode(['alternatives' => $alternatives, 'matches' => $matches])]);
        $personId = $this->createQuizPerson($quiz);
        $dbAnswer = json_encode(['answer1', 'answer2']);
        $answerId = $this->createQuizUserAnswer($quiz, ['question_ruuid' => $questionUuid, 'taker' => $personId, 'question_type' => 'multichoice', 'answer' => $dbAnswer]);
        $this->quizUserAnswerIds[] = $answerId;
        $this->quizESUserAnswers[$answerId] = ['answer 1', 'answer 2'];
        $dbAnswer = json_encode([
            ['left_id' => 'answer1', 'right_id' => 'question4'],
            ['left_id' => 'answer2', 'right_id' => 'question2'],
            ['left_id' => 'answer3', 'right_id' => 'question1'],
            ['left_id' => 'answer4', 'right_id' => 'question3'],
        ]);
        $answerId = $this->createQuizUserAnswer($quiz, ['question_ruuid' => $questionUuid, 'taker' => $personId, 'question_type' => 'matching', 'answer' => $dbAnswer]);
        $this->quizUserAnswerIds[] = $answerId;
        $this->quizESUserAnswers[$answerId] = ['answer 1 - question 4', 'answer 2 - question 2', 'answer 3 - question 1', 'answer 4 - question 3'];
        $dbAnswer = json_encode([
            ['answer' => 'answer filled in blank'],
        ]);
        $answerId = $this->createQuizUserAnswer($quiz, ['question_ruuid' => $questionUuid, 'taker' => $personId, 'question_type' => 'cloze', 'answer' => $dbAnswer]);
        $this->quizUserAnswerIds[] = $answerId;
        $this->quizESUserAnswers[$answerId] = ['answer filled in blank'];
        $dbAnswer = json_encode('text answer');
        $answerId = $this->createQuizUserAnswer($quiz, ['question_ruuid' => $questionUuid, 'taker' => $personId, 'question_type' => 'text_answer', 'answer' => $dbAnswer]);
        $this->quizUserAnswerIds[] = $answerId;
        $this->quizESUserAnswers[$answerId] = ['text answer'];

        $this->recordIds[] = $this->createManualRecord($db, ['entity_id' => $this->loId, 'user_id' => $this->userIds[0], 'instance_id' => $this->portalId]);
        $this->recordIds[] = $this->createManualRecord($db, ['entity_id' => $this->loId, 'user_id' => $this->userIds[1], 'instance_id' => $this->portalId]);

        $this->couponIds[] = $this->createCoupon($db, ['user_id' => $this->userIds[0], 'instance_id' => $this->portalId]);
        $this->couponIds[] = $this->createCoupon($db, ['user_id' => $this->userIds[1], 'instance_id' => $this->portalId]);

        $courseIdsB[] = $this->createCourse($db, ['title' => 'B.course 1 premium', 'instance_id' => $this->portalIdB, 'tags' => '[tag share] [tag A]']);
        $courseIdsB[] = $this->createCourse($db, ['title' => 'B.course 2 premium', 'instance_id' => $this->portalIdB]);
        $db->insert('gc_lo_group', [
            'instance_id' => $this->portalId,
            'lo_id'       => $courseIdsB[0],
        ]);
        $this->loGroupIds[] = "{$this->portalId}:{$courseIdsB[0]}";
        $lo2Id = $this->createCourse($db, ['title' => 'A.course 2', 'instance_id' => $this->portalId]);
        $db->insert('gc_lo_group', [
            'instance_id' => $this->portalId,
            'lo_id'       => $courseIdsB[1],
        ]);
        $this->loGroupIds[] = "{$this->portalId}:{$courseIdsB[1]}";
        $this->membershipEnrolmentId = $this->createEnrolment($db, ['profile_id' => $this->userAProfileId, 'lo_id' => $courseIdsB[0], 'taken_instance_id' => $this->portalId]);

        $this->createCredit($db, ['owner_id' => $this->userIds[0], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[0], 'status' => CreditStatuses::STATUS_AVAILABLE]);
        $this->createCredit($db, ['owner_id' => $this->userIds[0], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[0], 'status' => CreditStatuses::STATUS_DISABLED]);
        $this->createCredit($db, ['owner_id' => $this->userIds[0], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[0], 'status' => CreditStatuses::STATUS_AVAILABLE]);
        $this->createCredit($db, ['owner_id' => $this->userIds[0], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[0], 'status' => CreditStatuses::STATUS_USED]);
        $this->creditIds[] = "{$this->userIds[0]}:{$this->loId}";
        $this->createCredit($db, ['owner_id' => $this->userIds[1], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[1], 'status' => CreditStatuses::STATUS_DISABLED]);
        $this->createCredit($db, ['owner_id' => $this->userIds[1], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[1], 'status' => CreditStatuses::STATUS_USED]);
        $this->createCredit($db, ['owner_id' => $this->userIds[1], 'product_type' => 'lo', 'product_id' => $this->loId, 'portal_id' => $this->portalId, 'transaction_id' => $this->transactionIds[1], 'status' => CreditStatuses::STATUS_AVAILABLE]);
        $this->creditIds[] = "{$this->userIds[1]}:{$this->loId}";

        $this->awardIds[] = $this->createAward($dbAward, [
            'instance_id' => $this->portalId,
            'revision_id' => $fooAwardRevId = 1,
            'quantity'    => 5,
            'tags'        => '[tag A]',
        ]);
        $this->awardIds[] = $this->createAward($dbAward, [
            'instance_id' => $this->portalId,
            'revision_id' => $barAwardRevId = 2,
            'quantity'    => null,
        ]);

        $this->awardItemIds[] = $this->createAwardItem($dbAward, $fooAwardRevId, AwardItemTypes::LO, $this->loId, 2);
        $this->awardItemIds[] = $this->createAwardItem($dbAward, $fooAwardRevId, AwardItemTypes::LO, $lo2Id, 3);
        $this->awardItemIds[] = $this->createAwardItem($dbAward, $barAwardRevId, AwardItemTypes::LO, $this->loId, null);

        $this->awardItemManualIds[] = $this->createAwardItemManual($dbAward, ['user_id' => $this->userIds[0], 'award_id' => $this->awardIds[0], 'quantity' => 1, 'categories' => '[foo] [bar]']);
        $this->awardItemManualIds[] = $this->createAwardItemManual($dbAward, ['user_id' => $this->userIds[1], 'award_id' => $this->awardIds[1], 'quantity' => null, 'categories' => '[bar] [baz]']);

        $this->awardEnrolmentIds[] = $this->createAwardEnrolment($dbAward, ['award_id' => $this->awardIds[0], 'user_id' => $this->userIds[0], 'instance_id' => $this->portalId]);
        $this->awardEnrolmentIds[] = $this->createAwardEnrolment($dbAward, ['award_id' => $this->awardIds[1], 'user_id' => $this->userIds[1], 'instance_id' => $this->portalId]);

        foreach ($this->awardEnrolmentIds as $awardEnrolmentId) {
            $this->awardEnrolmentRevisionIds[] = $this->createAwardEnrolmentRevision($dbAward, $awardEnrolmentId);
        }
        $this->awardPlanIds[] = $this->createPlan($db, ['user_id' => $this->userIds[0], 'assigner_id' => null, 'instance_id' => $this->portalId, 'entity_type' => PlanTypes::ENTITY_AWARD, 'entity_id' => $this->awardIds[0]]);
        $this->awardPlanIds[] = $this->createPlan($db, ['user_id' => $this->userIds[1], 'assigner_id' => null, 'instance_id' => $this->portalId, 'entity_type' => PlanTypes::ENTITY_AWARD, 'entity_id' => $this->awardIds[1]]);

        $this->planIds[] = $this->createPlan($db, ['user_id' => $this->userIds[0], 'instance_id' => $this->portalId, 'entity_id' => $this->loId]);
        $this->planIds[] = $this->createPlan($db, ['user_id' => $this->userIds[1], 'instance_id' => $this->portalId, 'entity_id' => $this->loId]);
        $this->planIds[] = $this->createPlan($db, ['user_id' => $this->userIdWithoutEnrollments, 'instance_id' => $this->portalId, 'entity_id' => $this->loId]);
        $this->membershipPlanId = $this->createPlan($db, ['user_id' => $this->userIdWithoutEnrollments, 'instance_id' => $this->portalId, 'entity_id' => $courseIdsB[1]]);
        $this->awardPlanId = $this->createPlan($db, ['user_id' => $this->userIdWithoutEnrollments, 'instance_id' => $this->portalId, 'entity_type' => PlanTypes::ENTITY_AWARD, 'entity_id' => $this->awardIds[0]]);

        $this->awardItemEnrolmentIds[] = $this->createAwardItemEnrolment($dbAward, ['award_id' => $this->awardIds[0], 'user_id' => $this->userIds[0], 'instance_id' => $this->portalId, 'entity_id' => $this->loId, 'type' => AwardItemTypes::LO]);
        $this->awardItemEnrolmentIds[] = $this->createAwardItemEnrolment($dbAward, ['award_id' => $this->awardIds[1], 'user_id' => $this->userIds[1], 'instance_id' => $this->portalId, 'entity_id' => $this->loId, 'type' => AwardItemTypes::LO]);

        $app['repository.es']->installPortalIndex($this->portalId);

        # Content sharing
        $this->courseSharingId = $this->createCourse($db, ['title' => 'Course Sharing', 'instance' => $this->portalId2]);
        $groupId = $this->createGroup($db, ['title' => "go1:lo:$this->courseSharingId:foo", 'type' => GroupTypes::CONTENT_SHARING]);
        $this->createGroupItem($db, ['group_id' => $groupId, 'entity_type' => GroupItemTypes::PORTAL, 'entity_id' => $this->portalId]);

        $this->contractIds[] = $this->createContract($db, ['instance_id' => $this->portalId]);
        $this->contractIds[] = $this->createContract($db, ['instance_id' => $this->portalId]);

        $this->metricIds[] = $this->createMetric($db, []);
        $this->metricIds[] = $this->createMetric($db, []);
        $this->courseIdsB = $courseIdsB;
    }

    public function test()
    {
        $app = $this->getApp();
        $req = Request::create('/task?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace(['title' => 'reindex', 'index' => 'foo_' . time(), 'max_no_items' => 1]);
        $res = $app->handle($req);

        $this->assertEquals(200, $res->getStatusCode());
        $taskId = json_decode($res->getContent())->id;
        $repo = $this->taskRepository($app);
        $task = $repo->load($taskId);

        $base = ['index' => Schema::INDEX, 'routing' => Schema::INDEX];
        $client = $this->client($app);

        $lo = $client->get(['type' => Schema::O_LO, 'id' => $this->loId, 'routing' => $this->portalId] + $base);
        $eckLo = $client->get(['type' => Schema::O_ECK_METADATA, 'id' => "$this->portalName:lo"] + $base);
        $portal = $client->get(['type' => CustomerEsSchema::O_PORTAL, 'id' => $this->portalId] + $base);
        $eckUser = $client->get(['type' => Schema::O_ECK_METADATA, 'id' => "$this->portalName:account"] + $base);
        $event1Li = $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId1:$this->liEventId1", 'routing' => $this->portalId] + $base);
        $eventCourse = $client->get(['type' => Schema::O_EVENT, 'id' => "$this->courseEventId:$this->courseEventId", 'routing' => $this->portalId] + $base);
        $event1Course = $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId1:$this->courseEventId", 'routing' => $this->portalId] + $base);
        $configuration = $client->get(['type' => Schema::O_CONFIG, 'id' => "$this->portalName:foo:$this->portalId"] + $base);

        $eventSession1 = $client->get(['type' => Schema::O_EVENT, 'id' => "222:$this->liEventId1:$this->courseEventId", 'routing' => $this->portalId] + $base);
        $eventSession2 = $client->get(['type' => Schema::O_EVENT, 'id' => "333:$this->liEventId2:$this->courseEventId", 'routing' => $this->portalId] + $base);

        $this
            ->assertDocField($this->loId, $lo)
            ->assertDocField($this->portalId, $portal)
            ->assertDocField($this->portalName, $configuration, 'instance');

        foreach ($this->enrolmentIds as $enrolmentId) {
            $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => $enrolmentId, 'routing' => $this->portalId] + $base);
            $this->assertDocField($enrolmentId, $enrolment);

            $accountEnrolment = $client->get(['type' => CustomerEsSchema::O_ACCOUNT_ENROLMENT, 'id' => $enrolmentId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($enrolmentId, $accountEnrolment['_source']['id']);
            $this->assertEquals(EnrolmentTypes::TYPE_ENROLMENT, $accountEnrolment['_source']['type']);
        }

        $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => $this->membershipEnrolmentId, 'routing' => $this->portalId] + $base);
        $this->assertDocField($this->membershipEnrolmentId, $enrolment);
        $this->assertEquals(LoShareConsumer::id($this->portalId, $enrolment['_source']['lo_id']), $enrolment['_parent']);

        $this->assertTransaction($client);

        foreach ($this->accountIds as $accountId) {
            $account = $client->get(['type' => CustomerEsSchema::O_ACCOUNT, 'id' => $accountId, 'routing' => $this->portalId] + $base);
            $this->assertDocField($accountId, $account);
            $this->assertEquals(['bar', 'bar 2'], $account['_source']['fields_' . $this->portalId]['field_foo']['value_string']);
            $this->assertEquals([123, 234], $account['_source']['fields_' . $this->portalId]['field_baz']['value_integer']);
        }

        foreach ($this->enrolmentRevisionIds as $enrolmentRevisionId) {
            $enrolmentRevision = $client->get($base + ['type' => Schema::O_ENROLMENT_REVISION, 'id' => $enrolmentRevisionId]);
            $this->assertEquals($enrolmentRevisionId, $enrolmentRevision['_source']['id']);
        }

        $group = $client->get(['type' => Schema::O_GROUP, 'id' => $this->groupId, 'routing' => $this->portalId] + $base);
        $this->assertDocField($this->groupId, $group);

        $this->assertEquals(100, $task->percent);
        $this->assertEquals(Task::FINISHED, $task->status);

        foreach ($this->quizUserAnswerIds as $answerId) {
            $answer = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_QUIZ_USER_ANSWER, 'id' => $answerId]);
            $this->assertDocField($answerId, $answer);
            $this->assertDocField($this->quizESUserAnswers[$answerId], $answer, 'answer');
        }

        foreach ($this->recordIds as $recordId) {
            $record = $client->get(['type' => Schema::O_ENROLMENT, 'id' => "manual-record:{$recordId}", 'routing' => $this->portalId] + $base);
            $this->assertEquals(0, $record['_source']['id']);
            $this->assertEquals($this->loId, $record['_source']['lo']['id']);
        }

        foreach ($this->couponIds as $couponId) {
            $coupon = $client->get(['type' => Schema::O_COUPON, 'id' => $couponId, 'routing' => $this->portalId] + $base);
            $this->assertDocField($couponId, $coupon);
        }

        // LO_GROUP has been built in portal
        //        foreach ($this->loGroupIds as $loGroupId) {
        //            $loGroup = $client->get(['type' => Schema::O_LO_GROUP, 'id' => $loGroupId, 'routing' => $this->instanceId] + $base);
        //            $this->assertEquals($loGroupId, $loGroup['_id']);
        //        }

        $this->assertDocField($this->portalName, $eckUser, 'instance');
        $this->assertEquals('field_foo', $eckUser['_source']['field'][0]['name']);
        $this->assertEquals('field_bar', $eckLo['_source']['field'][0]['name']);

        foreach ($this->creditIds as $creditId) {
            $credit = $client->get($base + ['type' => Schema::O_CREDIT, 'id' => $creditId]);
            $this->assertEquals($creditId, $credit['_id']);
            $creditTransaction = $client->get($base + ['type' => Schema::O_PAYMENT_TRANSACTION, 'id' => $credit['_parent']]);
            $this->assertEquals($creditTransaction['_source']['credit_usage_count'], $credit['_source']['used']);
        }

        foreach ($this->awardIds as $awardId) {
            $award = $client->get(['type' => Schema::O_AWARD, 'id' => $awardId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardId, $award['_source']['id']);
        }

        foreach ($this->awardItemIds as $awardItemId) {
            $awardItem = $client->get(['type' => Schema::O_AWARD_ITEM, 'id' => $awardItemId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemId, $awardItem['_source']['id']);
        }

        foreach ($this->awardItemManualIds as $awardItemManualId) {
            $awardItemManual = $client->get(['type' => Schema::O_AWARD_ITEM_MANUAL, 'id' => $awardItemManualId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemManualId, $awardItemManual['_source']['id']);

            $awardManualEnrolment = $client->get(['type' => Schema::O_ENROLMENT, 'routing' => $this->portalId, 'id' => implode(':', [EnrolmentTypes::TYPE_AWARD, EnrolmentTypes::TYPE_MANUAL_RECORD, $awardItemManualId])] + $base);
            $this->assertEquals($awardItemManualId, $awardManualEnrolment['_source']['lo_id']);
        }

        foreach ($this->awardEnrolmentIds as $awardEnrolmentId) {
            $esId = AwardEnrolmentConsumer::id($awardEnrolmentId);
            $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => $esId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($esId, $enrolment['_id']);
            $this->assertEquals($awardEnrolmentId, $enrolment['_source']['id']);

            $accountEnrolment = $client->get(['type' => CustomerEsSchema::O_ACCOUNT_ENROLMENT, 'id' => $esId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($esId, $accountEnrolment['_id']);
            $this->assertEquals($awardEnrolmentId, $accountEnrolment['_source']['id']);
            $this->assertEquals(EnrolmentTypes::TYPE_AWARD, $accountEnrolment['_source']['type']);
        }

        foreach ($this->awardEnrolmentRevisionIds as $awardEnrolmentRevisionId) {
            $enrolmentRevision = $client->get($base + ['type' => Schema::O_ENROLMENT_REVISION, 'id' => EnrolmentTypes::TYPE_AWARD . ":{$awardEnrolmentRevisionId}"]);
            $this->assertEquals($awardEnrolmentRevisionId, $enrolmentRevision['_source']['id']);
        }

        foreach ($this->planIds as $planId) {
            $plan = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_PLAN, 'id' => $planId, 'routing' => $this->portalId]);
            $this->assertEquals($planId, $plan['_source']['id']);
        }
        $membershipPlan = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_ENROLMENT, 'id' => EnrolmentTypes::TYPE_PLAN_ASSIGNED . ":$this->membershipPlanId", 'routing' => $this->portalId]);
        $this->assertEquals(LoShareConsumer::id($this->portalId, $this->courseIdsB[1]), $membershipPlan['_parent']);
        $awardPlan = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_ENROLMENT, 'id' => EnrolmentTypes::TYPE_PLAN_ASSIGNED . ":$this->awardPlanId", 'routing' => $this->portalId]);
        $this->assertEquals(LoTypes::AWARD . ":{$this->awardIds[0]}", $awardPlan['_parent']);

        foreach ($this->awardItemEnrolmentIds as $awardItemEnrolmentId) {
            $itemEnrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => sprintf('%s:%s', EnrolmentTypes::TYPE_AWARD_ITEM, $awardItemEnrolmentId), 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemEnrolmentId, $itemEnrolment['_source']['id']);
        }

        // Suggestion check
        $categorySuggestions = $client->search(['index' => Schema::INDEX, 'type' => Schema::O_SUGGESTION_CATEGORY, 'body' => ['suggest' => ['categories' => [
            'regex'      => '.',
            'completion' => [
                'field'    => 'category',
                'size'     => 5,
                'contexts' => ['instance_id' => $this->portalId],
            ],
        ]]]]);
        $categorySuggestions = $categorySuggestions['suggest']['categories'][0]['options'];
        $this->assertCount(3, $categorySuggestions);
        $this->assertEquals(['input' => 'bar', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[0]['_source']['category']);
        $this->assertEquals(['input' => 'baz', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[1]['_source']['category']);
        $this->assertEquals(['input' => 'foo', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[2]['_source']['category']);

        $portalTagSuggestions = $client->search(['index' => Schema::INDEX, 'type' => Schema::O_SUGGESTION_TAG, 'body' => ['suggest' => ['tags' => [
            'regex'      => '.',
            'completion' => [
                'field'    => 'tag',
                'size'     => 10,
                'contexts' => ['instance_id' => $this->portalId],
            ],
        ]]]]);
        $portalTagSuggestions = $portalTagSuggestions['suggest']['tags'][0]['options'];
        $this->assertCount(4, $portalTagSuggestions);
        $this->assertEquals(['input' => 'tag A', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[0]['_source']['tag']);
        $this->assertEquals(['input' => 'tag B', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[1]['_source']['tag']);
        $this->assertEquals(['input' => 'tag C', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[2]['_source']['tag']);
        $this->assertEquals(['input' => 'tag share', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[3]['_source']['tag']);

        $portalBTagSuggestions = $client->search(['index' => Schema::INDEX, 'type' => Schema::O_SUGGESTION_TAG, 'body' => ['suggest' => ['tags' => [
            'regex'      => '.',
            'completion' => [
                'field'    => 'tag',
                'size'     => 10,
                'contexts' => ['instance_id' => $this->portalIdB],
            ],
        ]]]]);
        $portalBTagSuggestions = $portalBTagSuggestions['suggest']['tags'][0]['options'];
        $this->assertCount(2, $portalBTagSuggestions);
        $this->assertEquals(['input' => 'tag A', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalIdB]], $portalBTagSuggestions[0]['_source']['tag']);
        $this->assertEquals(['input' => 'tag share', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalIdB]], $portalBTagSuggestions[1]['_source']['tag']);

        $esLoTag = $client->search(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG]);
        $this->assertCount(3, $esLoTag['hits']['hits']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag A:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag A', 'type' => TagTypes::LOCAL], $esLoTag['_source']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag B:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag B', 'type' => TagTypes::LOCAL], $esLoTag['_source']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag C:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag C', 'type' => TagTypes::LOCAL], $esLoTag['_source']);

        // Assigned enrollment.
        $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => "plan-assigned:{$this->planIds[2]}", 'routing' => $this->portalId] + $base);
        $this->assertEquals(0, $enrolment['_source']['id']);

        $this->assertEquals($event1Li['_source']['start'], $this->events[1]['start']);
        $this->assertEquals($event1Li['_source']['end'], $this->events[1]['end']);
        $this->assertEquals($event1Li['_source']['timezone'], $this->events[1]['timezone']);
        $this->assertEquals($event1Li['_source']['country'], $countryCode = $this->events[1]['location']['country']);
        $this->assertEquals($event1Li['_source']['country_name'], Country::getName($countryCode));
        $this->assertEquals($event1Li['_source']['administrative_area'], $this->events[1]['location']['administrative_area']);
        $this->assertEquals($event1Li['_source']['administrative_area_name'], Country::getStateName($countryCode, $this->events[1]['location']['administrative_area']));

        $this->assertEquals($event1Course['_source']['start'], $this->events[1]['start']);
        $this->assertEquals($event1Course['_source']['end'], $this->events[1]['end']);
        $this->assertEquals($event1Course['_source']['timezone'], $this->events[1]['timezone']);
        $this->assertEquals($event1Course['_source']['country'], $countryCode = $this->events[1]['location']['country']);
        $this->assertEquals($event1Course['_source']['country_name'], Country::getName($countryCode));
        $this->assertEquals($event1Course['_source']['administrative_area'], $this->events[1]['location']['administrative_area']);
        $this->assertEquals($event1Course['_source']['administrative_area_name'], Country::getStateName($countryCode, $this->events[1]['location']['administrative_area']));

        $this->assertEquals($eventCourse['_source']['start'], $this->events[0]['start']);
        $this->assertEquals($eventCourse['_source']['end'], $this->events[0]['end']);
        $this->assertEquals($eventCourse['_source']['timezone'], $this->events[0]['timezone']);
        $this->assertEquals($eventCourse['_source']['country'], $countryCode = $this->events[0]['location']['country']);
        $this->assertEquals($eventCourse['_source']['country_name'], Country::getName($countryCode));
        $this->assertEquals($eventCourse['_source']['administrative_area'], $this->events[0]['location']['administrative_area']);
        $this->assertEquals($eventCourse['_source']['administrative_area_name'], Country::getStateName($countryCode, $this->events[0]['location']['administrative_area']));

        $this->assertEquals($eventSession1['_source']['start'], $this->eventSessions[0]['start_at']);
        $this->assertEquals($eventSession1['_source']['end'], $this->eventSessions[0]['end_at']);
        $this->assertEquals($eventSession1['_source']['timezone'], $this->eventSessions[0]['timezone']);
        $this->assertEquals($eventSession1['_source']['country'], $countryCode = $this->eventSessions[0]['location']->country);
        $this->assertEquals($eventSession1['_source']['country_name'], Country::getName($countryCode));
        $this->assertEquals($eventSession1['_source']['administrative_area'], $this->eventSessions[0]['location']->administrative_area);
        $this->assertEquals($eventSession1['_source']['administrative_area_name'], Country::getStateName($countryCode, $this->eventSessions[0]['location']->administrative_area));

        $this->assertEquals($eventSession2['_source']['start'], $this->eventSessions[1]['start_at']);
        $this->assertEquals($eventSession2['_source']['end'], $this->eventSessions[1]['end_at']);
        $this->assertEquals($eventSession2['_source']['timezone'], $this->eventSessions[1]['timezone']);
        $this->assertEquals($eventSession2['_source']['country'], $countryCode = $this->eventSessions[1]['location']->country);
        $this->assertEquals($eventSession2['_source']['country_name'], Country::getName($countryCode));
        $this->assertEquals($eventSession2['_source']['administrative_area'], $this->eventSessions[1]['location']->administrative_area);
        $this->assertEquals($eventSession2['_source']['administrative_area_name'], Country::getStateName($countryCode, $this->eventSessions[1]['location']->administrative_area));

        foreach ($this->eventEnrolmentIds as $eventEnrolmentId) {
            $eventEnrolment = $client->get(['type' => Schema::O_EVENT_ATTENDANCE, 'id' => $eventEnrolmentId, 'routing' => $this->portalId] + $base);
            $this->assertDocField($eventEnrolmentId, $eventEnrolment);
        }
        # Content sharing check
        $sharedCourse = $client->get(['type' => Schema::O_LO, 'id' => LoContentSharingConsumer::id($this->courseSharingId, $this->portalId), 'routing' => $this->portalId] + $base);
        $this->assertEquals($this->courseSharingId, $sharedCourse['_source']['id']);

        foreach ($this->contractIds as $contractId) {
            $contract = $client->get(['type' => Schema::O_CONTRACT, 'id' => $contractId, 'index' => Schema::INDEX]);
            $this->assertDocField($contractId, $contract);
        }

        foreach ($this->metricIds as $metricId) {
            $metric = $client->get(['type' => Schema::O_METRIC, 'id' => $metricId, 'index' => Schema::INDEX]);
            $this->assertDocField($metricId, $metric);
        }

        $req = Request::create('/stats?jwt=' . UserHelper::ROOT_JWT);
        $res = $app->handle($req);
        $stats = json_decode($res->getContent(), true);

        $this->assertEquals(200, $res->getStatusCode());
        $this->assertCount(count(REINDEX_HANDLERS), $stats);
    }

    public function testInstance()
    {
        $app = $this->getApp();
        $req = Request::create('/task?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace(['title' => 'reindex', 'index' => 'foo_' . time(), 'max_no_items' => 1, 'instance' => $this->portalName]);
        $res = $app->handle($req);
        $this->assertEquals(200, $res->getStatusCode());
        $taskId = json_decode($res->getContent())->id;
        $repo = $this->taskRepository($app);
        $task = $repo->load($taskId);

        $base = ['index' => Schema::INDEX, 'routing' => Schema::INDEX];
        $client = $this->client($app);

        $lo = $client->get(['type' => Schema::O_LO, 'id' => $this->loId, 'routing' => $this->portalId] + $base);
        $configuration = $client->get($base + ['type' => Schema::O_CONFIG, 'id' => "$this->portalName:foo:$this->portalId"]);
        //$portal = $client->get($base + ['type' => CustomerEsSchema::O_PORTAL, 'id' => $this->instanceId]);
        $eckUser = $client->get($base + ['type' => Schema::O_ECK_METADATA, 'id' => "$this->portalName:account"]);
        $eckLo = $client->get($base + ['type' => Schema::O_ECK_METADATA, 'id' => "$this->portalName:lo"]);

        $this->assertEquals($this->loId, $lo['_source']['id']);
        // @TODO need to review ECK for lo
        //$this->assertEquals('foo', $lo['_source']['fields_' . $this->instanceId]['field_bar']['value_string'][0]);
        //$this->assertEquals($this->instanceId, $portal['_source']['id']);
        $this->assertEquals($this->portalName, $configuration['_source']['instance']);

        foreach ($this->enrolmentIds as $enrolmentId) {
            $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => $enrolmentId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($enrolmentId, $enrolment['_source']['id']);

            $accountEnrolment = $client->get(['type' => CustomerEsSchema::O_ACCOUNT_ENROLMENT, 'id' => $enrolmentId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($enrolmentId, $accountEnrolment['_source']['id']);
            $this->assertEquals(EnrolmentTypes::TYPE_ENROLMENT, $accountEnrolment['_source']['type']);
        }

        $this->assertTransaction($client);

        foreach ($this->accountIds as $accountId) {
            $account = $client->get(['type' => CustomerEsSchema::O_ACCOUNT, 'id' => $accountId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($accountId, $account['_source']['id']);
            $this->assertEquals(['bar', 'bar 2'], $account['_source']['fields_' . $this->portalId]['field_foo']['value_string']);
            $this->assertEquals([123, 234], $account['_source']['fields_' . $this->portalId]['field_baz']['value_integer']);
            //$this->assertTrue(in_array($account['_parent'], $this->userIds));
        }

        $group = $client->get(['type' => Schema::O_GROUP, 'id' => $this->groupId, 'routing' => $this->portalId] + $base);
        $this->assertEquals($this->groupId, $group['_source']['id']);

        $this->assertEquals(Task::FINISHED, $task->status);
        $this->assertEquals(100, $task->percent);

        foreach ($this->quizUserAnswerIds as $answerId) {
            $answer = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_QUIZ_USER_ANSWER, 'id' => $answerId]);
            $this->assertEquals($answerId, $answer['_source']['id']);
        }

        foreach ($this->recordIds as $recordId) {
            $record = $client->get(['type' => Schema::O_ENROLMENT, 'id' => "manual-record:{$recordId}", 'routing' => $this->portalId] + $base);
            $this->assertEquals(0, $record['_source']['id']);
            $this->assertEquals($this->loId, $record['_source']['lo']['id']);
        }

        foreach ($this->enrolmentRevisionIds as $awardEnrolmentRevisionId) {
            $enrolmentRevision = $client->get($base + ['type' => Schema::O_ENROLMENT_REVISION, 'id' => $awardEnrolmentRevisionId]);
            $this->assertEquals($awardEnrolmentRevisionId, $enrolmentRevision['_source']['id']);
        }

        foreach ($this->couponIds as $couponId) {
            $coupon = $client->get(['type' => Schema::O_COUPON, 'id' => $couponId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($couponId, $coupon['_id']);
        }

        // LO_GROUP has been built in portal
        //        foreach ($this->loGroupIds as $loGroupId) {
        //            $loGroup = $client->get(['type' => Schema::O_LO_GROUP, 'id' => $loGroupId, 'routing' => $this->instanceId] + $base);
        //            $this->assertEquals($loGroupId, $loGroup['_id']);
        //        }

        $this->assertEquals($this->portalName, $eckUser['_source']['instance']);
        $this->assertEquals('field_foo', $eckUser['_source']['field'][0]['name']);

        $this->assertEquals($this->portalName, $eckUser['_source']['instance']);
        $this->assertEquals('field_bar', $eckLo['_source']['field'][0]['name']);

        foreach ($this->creditIds as $creditId) {
            $credit = $client->get($base + ['type' => Schema::O_CREDIT, 'id' => $creditId]);
            $this->assertEquals($creditId, $credit['_id']);
        }

        foreach ($this->awardIds as $awardId) {
            $award = $client->get(['type' => Schema::O_AWARD, 'id' => $awardId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardId, $award['_source']['id']);
        }

        foreach ($this->awardItemIds as $awardItemId) {
            $awardItem = $client->get(['type' => Schema::O_AWARD_ITEM, 'id' => $awardItemId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemId, $awardItem['_source']['id']);
        }

        foreach ($this->awardItemManualIds as $awardItemManualId) {
            $awardItemManual = $client->get(['type' => Schema::O_AWARD_ITEM_MANUAL, 'id' => $awardItemManualId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemManualId, $awardItemManual['_source']['id']);

            $awardManualEnrolment = $client->get(['type' => Schema::O_ENROLMENT, 'routing' => $this->portalId, 'id' => implode(':', [EnrolmentTypes::TYPE_AWARD, EnrolmentTypes::TYPE_MANUAL_RECORD, $awardItemManualId])] + $base);
            $this->assertEquals($awardItemManualId, $awardManualEnrolment['_source']['lo_id']);
        }

        foreach ($this->awardEnrolmentIds as $awardEnrolmentId) {
            $esId = AwardEnrolmentConsumer::id($awardEnrolmentId);
            $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => $esId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($esId, $enrolment['_id']);
            $this->assertEquals($awardEnrolmentId, $enrolment['_source']['id']);

            $accountEnrolment = $client->get(['type' => CustomerEsSchema::O_ACCOUNT_ENROLMENT, 'id' => $esId, 'routing' => $this->portalId] + $base);
            $this->assertEquals($esId, $accountEnrolment['_id']);
            $this->assertEquals($awardEnrolmentId, $accountEnrolment['_source']['id']);
            $this->assertEquals(EnrolmentTypes::TYPE_AWARD, $accountEnrolment['_source']['type']);
        }

        foreach ($this->awardEnrolmentRevisionIds as $awardEnrolmentRevisionId) {
            $enrolmentRevision = $client->get($base + ['type' => Schema::O_ENROLMENT_REVISION, 'id' => EnrolmentTypes::TYPE_AWARD . ":{$awardEnrolmentRevisionId}"]);
            $this->assertEquals($awardEnrolmentRevisionId, $enrolmentRevision['_source']['id']);
        }

        foreach ($this->planIds as $planId) {
            $plan = $client->get(['index' => Schema::INDEX, 'type' => Schema::O_PLAN, 'id' => $planId, 'routing' => $this->portalId]);
            $this->assertEquals($planId, $plan['_source']['id']);
        }

        foreach ($this->awardItemEnrolmentIds as $awardItemEnrolmentId) {
            $itemEnrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => sprintf('%s:%s', EnrolmentTypes::TYPE_AWARD_ITEM, $awardItemEnrolmentId), 'routing' => $this->portalId] + $base);
            $this->assertEquals($awardItemEnrolmentId, $itemEnrolment['_source']['id']);
        }

        // Suggestion check
        $categorySuggestions = $client->search(['index' => Schema::INDEX, 'type' => Schema::O_SUGGESTION_CATEGORY, 'body' => ['suggest' => ['categories' => [
            'regex'      => '.',
            'completion' => [
                'field'    => 'category',
                'size'     => 5,
                'contexts' => ['instance_id' => $this->portalId],
            ],
        ]]]]);
        $categorySuggestions = $categorySuggestions['suggest']['categories'][0]['options'];
        $this->assertCount(3, $categorySuggestions);
        $this->assertEquals(['input' => 'bar', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[0]['_source']['category']);
        $this->assertEquals(['input' => 'baz', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[1]['_source']['category']);
        $this->assertEquals(['input' => 'foo', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $categorySuggestions[2]['_source']['category']);

        $portalTagSuggestions = $client->search(['index' => Schema::INDEX, 'type' => Schema::O_SUGGESTION_TAG, 'body' => ['suggest' => ['tags' => [
            'regex'      => '.',
            'completion' => [
                'field'    => 'tag',
                'size'     => 10,
                'contexts' => ['instance_id' => $this->portalId],
            ],
        ]]]]);
        $portalTagSuggestions = $portalTagSuggestions['suggest']['tags'][0]['options'];
        $this->assertCount(4, $portalTagSuggestions);
        $this->assertEquals(['input' => 'tag A', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[0]['_source']['tag']);
        $this->assertEquals(['input' => 'tag B', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[1]['_source']['tag']);
        $this->assertEquals(['input' => 'tag C', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[2]['_source']['tag']);
        $this->assertEquals(['input' => 'tag share', 'weight' => 1, 'contexts' => ['instance_id' => $this->portalId]], $portalTagSuggestions[3]['_source']['tag']);

        $esLoTag = $client->search(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG]);
        $this->assertCount(3, $esLoTag['hits']['hits']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag A:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag A', 'type' => TagTypes::LOCAL], $esLoTag['_source']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag B:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag B', 'type' => TagTypes::LOCAL], $esLoTag['_source']);
        $esLoTag = $client->get(['index' => Schema::portalIndex($this->portalId), 'type' => Schema::O_LO_TAG, 'id' => "tag C:$this->portalId"]);
        $this->assertArraySubset(['title' => 'tag C', 'type' => TagTypes::LOCAL], $esLoTag['_source']);

        # Content sharing check
        $sharedCourse = $client->get(['type' => Schema::O_LO, 'id' => LoContentSharingConsumer::id($this->courseSharingId, $this->portalId), 'routing' => $this->portalId] + $base);
        $this->assertEquals($this->courseSharingId, $sharedCourse['_source']['id']);

        // Assigned enrollment.
        $enrolment = $client->get(['type' => Schema::O_ENROLMENT, 'id' => "plan-assigned:{$this->planIds[2]}", 'routing' => $this->portalId] + $base);
        $this->assertEquals(0, $enrolment['_source']['id']);
        $req = Request::create("/verify/$this->portalId?jwt=" . UserHelper::ROOT_JWT, 'GET');
        $res = $app->handle($req);
        // @TODO Need to add a new type field to learning object to indicate where #lo come from(portal, premium, shared)
        // $this->assertEquals(204, $res->getStatusCode());

        $this->createCourse($app['dbs']['default'], ['title' => 'A.course extra', 'instance_id' => $this->portalId]);
        $req = Request::create("/verify/$this->portalId?jwt=" . UserHelper::ROOT_JWT, 'GET');
        $res = $app->handle($req);
        $this->assertEquals(200, $res->getStatusCode());
        $stats = json_decode($res->getContent(), true);

        $this->assertNotEquals($stats['db'][LoReindex::NAME], $stats['es'][LoReindex::NAME]);
        $this->assertEquals($stats['db'][EnrolmentReindex::NAME], $stats['es'][EnrolmentReindex::NAME]);
        $this->assertEquals($stats['db'][AccountReindex::NAME], $stats['es'][AccountReindex::NAME]);
        # $this->assertEquals($stats['db'][LoShareReindex::NAME], $stats['es'][LoShareReindex::NAME]);
    }

    private function assertTransaction(Client $client)
    {
        $base = ['index' => Schema::INDEX, 'routing' => Schema::INDEX];
        foreach ($this->transactionIds as $transactionId) {
            $transaction = $client->get(['type' => Schema::O_PAYMENT_TRANSACTION, 'id' => $transactionId, 'index' => Schema::INDEX]);
            foreach ($transaction['_source']['items'] as $item) {
                $lo = $client->get(['type' => Schema::O_LO, 'id' => $item['product_id'], 'routing' => $this->portalId] + $base);
                $this->assertEquals($item['product_title'], $lo['_source']['title']);
                if ($item['product_parent_id']) {
                    $lo = $client->get(['type' => Schema::O_LO, 'id' => $item['product_parent_id'], 'routing' => $this->portalId] + $base);
                    $this->assertEquals($item['product_parent_title'], $lo['_source']['title']);
                }
            }
            $this->assertDocField($transactionId, $transaction);
        }
    }

    private function assertNumOfDoc(Client $client, $portalId, $type, $expectedNum)
    {
        $hit = $client->count(['index' => Schema::portalIndex($portalId), 'type' => $type]);
        $this->assertEquals($expectedNum, $hit['count']);

        return $this;
    }

    private function archiveEvent(Connection $db, $id, $status)
    {
        $q = $db->createQueryBuilder()->update('gc_lo')->set('published', $status)->where('id = :id')->setParameter('id', $id);
        $q->execute();
    }

    public function dataEvent()
    {
        return [[8, LoStatuses::ARCHIVED], [12, LoStatuses::PUBLISHED], [12, LoStatuses::UNPUBLISHED]];
    }

    /**
     * @dataProvider dataEvent
     */
    public function testArchiveEvent($numOfDoc, $status)
    {
        $app = $this->getApp();
        $db = $app['dbs']['go1'];
        $this->archiveEvent($db, $this->liEventId3, $status);
        $this->archiveEvent($db, $this->liEventId4, $status);
        $req = Request::create('/task?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace(['title' => 'reindex', 'index' => 'foo_' . time(), 'max_no_items' => 1]);
        $res = $app->handle($req);

        $this->assertEquals(200, $res->getStatusCode());

        $base = ['index' => Schema::INDEX, 'routing' => Schema::INDEX];
        $client = $this->client($app);
        $this->assertNumOfDoc($client, $this->portalId, Schema::O_EVENT, $numOfDoc);

        if (LoStatuses::ARCHIVED == $status) {
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->liEventId3", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->liEventId4", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            });
        } else {
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->liEventId3", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->liEventId4", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
        }
        $client->get(['type' => Schema::O_EVENT, 'id' => "$this->courseArchiveEventId:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
    }

    public function dataCourse()
    {
        return [[7, LoStatuses::ARCHIVED], [12, LoStatuses::PUBLISHED], [12, LoStatuses::UNPUBLISHED]];
    }

    /**
     * @dataProvider dataCourse
     */
    public function testArchiveCourse($numOfDoc, $status)
    {
        $app = $this->getApp();
        $db = $app['dbs']['go1'];
        $this->archiveEvent($db, $this->courseArchiveEventId, $status);
        $req = Request::create('/task?jwt=' . UserHelper::ROOT_JWT, 'POST');
        $req->request->replace(['title' => 'reindex', 'index' => 'foo_' . time(), 'max_no_items' => 1]);
        $res = $app->handle($req);

        $this->assertEquals(200, $res->getStatusCode());

        $base = ['index' => Schema::INDEX, 'routing' => Schema::INDEX];
        $client = $this->client($app);
        $this->assertNumOfDoc($client, $this->portalId, Schema::O_EVENT, $numOfDoc);

        if (LoStatuses::ARCHIVED == $status) {
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->liEventId3", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->liEventId4", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            });
            $this->assertMissing404Exception(function () use ($client, $base) {
                $client->get(['type' => Schema::O_EVENT, 'id' => "$this->courseArchiveEventId:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            });
        } else {
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->liEventId3", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId3:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->liEventId4", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->liEventId4:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
            $client->get(['type' => Schema::O_EVENT, 'id' => "$this->courseArchiveEventId:$this->courseArchiveEventId", 'routing' => $this->portalId] + $base);
        }
    }
}
