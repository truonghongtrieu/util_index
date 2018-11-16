<?php

namespace go1\util_index\controller;

use go1\util\AccessChecker;
use go1\util\Error;
use go1\util_index\ReindexInterface;
use Pimple\Container;
use ReflectionClass;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class ReindexHandlersController
{
    private $c;

    public function __construct(Container $c)
    {
        $this->c = $c;
    }

    public function get(Request $req)
    {
        $access = new AccessChecker;
        if (!$access->isAccountsAdmin($req)) {
            return Error::jr403('Internal resource');
        }

        $handlers = [];
        $priorities = [];
        foreach ($this->c->keys() as $service) {
            if (0 === strpos($service, 'reindex.handler.')) {
                $handler = $this->c->offsetGet($service);

                if ($handler instanceof ReindexInterface) {
                    $reflect = new ReflectionClass(get_class($handler));
                    $priority = $reflect->hasConstant('REINDEX_PRIORITY') ? $handler::REINDEX_PRIORITY : 0;
                    $ableToReindexPerPortal = $reflect->hasConstant('ABLE_TO_REINDEX_PER_PORTAL') && $handler::ABLE_TO_REINDEX_PER_PORTAL;

                    $name = $handler::NAME;
                    $handler = get_class($handler);
                    $handler = explode('\\', $handler);
                    $handler = end($handler);
                    preg_match_all('/((?:^|[A-Z])[a-z]+)/', $handler, $matches);
                    array_pop($matches[1]);
                    $handler = implode(' ', $matches[1]);

                    $priorities[$name] = $priority;
                    $handlers['portal'][$name] = $handler;
                    $ableToReindexPerPortal && $handlers['portal-single'][$name] = $handler;
                }
            }
        }

        $sortByPriority = function ($name1, $name2) use ($priorities) {
            return $priorities[$name1] > $priorities[$name2];
        };

        uksort($handlers['portal'], $sortByPriority);
        uksort($handlers['portal-single'], $sortByPriority);

        return new JsonResponse($handlers);
    }
}
