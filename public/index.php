<?php

use go1\util_index\IndexService;

return call_user_func(function () {
    if (!defined('APP_ROOT')) {
        throw new RuntimeException('Undefined APP_ROOT');
    }

    require_once APP_ROOT . '/vendor/autoload.php';
    $cnf = is_file(APP_ROOT . '/config.php') ? APP_ROOT . '/config.php' : APP_ROOT . '/config.default.php';
    $app = new IndexService(require $cnf);

    return ('cli' === php_sapi_name()) ? $app : $app->run();
});
