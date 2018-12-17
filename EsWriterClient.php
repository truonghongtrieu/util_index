<?php

namespace go1\util_index;

use go1\clients\MqClient;

class EsWriterClient
{
    private $mqClient;
    private $routingKey;

    public function __construct(MqClient $mqClient, string $routingKey)
    {
        $this->mqClient = $mqClient;
        $this->routingKey = $routingKey;
    }

    public function delete($params)
    {
        $uri = sprintf(
            '/%s/%s/%s?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $this->mqClient->publish([
            'uri'         => $uri,
            'http_method' => 'DELETE',
        ], $this->routingKey);
    }

    public function updateByQuery($params)
    {
        $uri = sprintf(
            '/%s/%s/_update_by_query',
            $params['index'],
            $params['type']
        );

        isset($params['routing']) && $uri .= sprintf('?routing=%s', $params['routing']);

        $this->mqClient->publish([
            'uri'  => $uri,
            'body' => $params['body'],
        ], $this->routingKey);
    }

    public function index($params)
    {
        $uri = sprintf(
            '/%s/%s/%s/_update?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $this->mqClient->publish([
            'uri'  => $uri,
            'body' => $params['body'],
        ], $this->routingKey);
    }

    public function create($params)
    {
        $this->index($params);
    }

    public function update($params)
    {
        $this->index($params);
    }

    public function bulk($params)
    {

    }
}
