<?php

namespace go1\util_index\tests;

use go1\util\user\UserHelper;
use Symfony\Component\HttpFoundation\Request;

class ReindexHandlersControllerTest extends IndexServiceTestCase
{
    protected function indices()
    {
        return []; # Disabled indices creation.
    }

    public function test()
    {
        $app = $this->getApp();
        $req = Request::create('/reindex/handlers?jwt=' . UserHelper::ROOT_JWT, 'GET');
        $res = $app->handle($req);
        $this->assertEquals(200, $res->getStatusCode());
    }
}
