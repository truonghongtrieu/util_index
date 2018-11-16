<?php

namespace go1\util_index\worker;

use Doctrine\DBAL\Connection;
use go1\util\contract\ConsumerInterface;
use go1\util\portal\PortalChecker;
use go1\util\portal\PortalHelper;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\UtilIndexServiceProvider;
use stdClass;

class InstallConsumer implements ConsumerInterface
{
    private $go1;
    private $repository;
    private $portalChecker;

    public function __construct(
        Connection $go1,
        ElasticSearchRepository $repository,
        PortalChecker $portalChecker
    )
    {
        $this->go1 = $go1;
        $this->repository = $repository;
        $this->portalChecker = $portalChecker;
    }

    public function aware(string $event): bool
    {
        return in_array($event, [UtilIndexServiceProvider::INDEX_INSTALL_PORTAL]);
    }

    public function consume(string $routingKey, stdClass $data, stdClass $context = null): bool
    {
        switch ($routingKey) {
            case UtilIndexServiceProvider::INDEX_INSTALL_PORTAL:
                if ($portal = PortalHelper::load($this->go1, $data->portalId)) {
                    if (!$this->portalChecker->isLegacy($portal)) {
                        $this->repository->installPortalIndex($data->portalId);
                    }
                }
                break;
        }

        return true;
    }
}
