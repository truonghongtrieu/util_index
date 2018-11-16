<?php

namespace go1\util_index;

use go1\core\learning_record\enrolment\index\ManualRecordReindex;
use go1\core\learning_record\enrolment\index\PlanReindex;
use Pimple\Container;
use Pimple\ServiceProviderInterface;

class ReindexServiceProvider implements ServiceProviderInterface
{
    const WORKER_REINDEX              = 'worker.index.reindex';
    const INDEX_REMOVE_REDUNDANT_DATA = 'worker.index.remove-redundant-data';

    public function register(Container $c)
    {
        $c['reindex.transaction'] = function (Container $c) {
            return new ReindexTransaction(
                $c['dbs']['default'],
                $c['dbs']['go1'],
                $c['go1.client.mq'],
                $c['go1.client.es'],
                $c
            );
        };

        $c['reindex.handler.manual_record'] = function (Container $c) {
            return new ManualRecordReindex($c['dbs']['enrolment'], $c['consumer.manual_record']);
        };

        $c['reindex.handler.plan'] = function (Container $c) {
            return new PlanReindex($c['dbs']['go1'], $c['consumer.plan'], $c['consumer.plan.enrolment-virtual']);
        };
    }
}
