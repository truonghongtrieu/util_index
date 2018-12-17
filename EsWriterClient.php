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
        $this->create($params);
    }

    public function create($params)
    {
        $uri = sprintf(
            '/%s/%s/%s/_create?routing=%s',
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

    public function update($params)
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

    public function bulk($params)
    {
        # Parse ElasticSearch\Client::bulk into es writer
        $offset = 0;
        while (isset($params['body'][$offset])) {
            $metadata = $params['body'][$offset];
            $body = $params['body'][$offset + 1];
            $op = array_keys($metadata)[0];

            $_params = array_merge([
                'index' => $params['index'],
                'type'  => $params['type'],
                'body'  => $body,
            ], $metadata[$op]);

            $this->{$op}($_params);

            $offset += 2;
        }
    }
}
