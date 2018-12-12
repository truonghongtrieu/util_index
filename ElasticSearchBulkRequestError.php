<?php

namespace go1\util_index;

use Exception;

class ElasticSearchBulkRequestError extends Exception
{
    private $response;

    public function __construct(string $message, array $response)
    {
        parent::__construct($message);

        $this->response = $response;
    }

    public function getResponse()
    {
        return $this->response;
    }
}
