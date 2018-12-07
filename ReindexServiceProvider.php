<?php

namespace go1\util_index;

use Pimple\Container;
use Pimple\ServiceProviderInterface;

class ReindexServiceProvider implements ServiceProviderInterface
{
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
    }
}
