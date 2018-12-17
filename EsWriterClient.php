<?php

namespace go1\util_index;

use Assert\Assert;
use go1\clients\MqClient;
use RuntimeException;

class EsWriterClient
{
    private $mqClient;
    private $routingKey;

    public function __construct(MqClient $mqClient, string $routingKey)
    {
        $this->mqClient = $mqClient;
        $this->routingKey = $routingKey;
    }

    private function validate(array $params, string $requiredFields)
    {
        $assertion = Assert::lazy();
        foreach (explode(",", $requiredFields) as $requiredField) {
            $assertion->that($params[$requiredField] ?? null, $requiredField)->notEmpty();
        }
        $assertion->verifyNow();
    }

    public function delete($params)
    {
        $this->validate($params, 'index,type,id,body');
        $uri = sprintf(
            '/%s/%s/%s?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $this->mqClient->publish(['uri' => $uri, 'http_method' => 'DELETE'], $this->routingKey);
    }

    public function updateByQuery($params)
    {
        $this->validate($params, 'index,type,body,routing');

        $uri = sprintf('/%s/%s/_update_by_query', $params['index'], $params['type']);
        isset($params['routing']) && $uri .= sprintf('?routing=%s', $params['routing']);
        $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function index($params)
    {
        $this->create($params);
    }

    public function create($params)
    {
        $this->validate($params, 'index,type,id,body,routing');
        $uri = sprintf(
            '/%s/%s/%s/_create?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );
        $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function update($params)
    {
        $this->validate($params, 'index,type,id,body,routing');
        $uri = sprintf(
            '/%s/%s/%s/_update?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function bulk($params)
    {
        $this->validate($params, 'index,type,body');

        # Parse ElasticSearch\Client::bulk into es writer
        $offset = 0;
        while (isset($params['body'][$offset])) {
            $metadata = $params['body'][$offset];
            $body = $params['body'][$offset + 1];
            $op = array_keys($metadata)[0];

            $_params = array_merge([
                'index'   => $params['index'],
                'type'    => $params['type'],
                'id'      => $metadata['_id'] ?? null,
                'routing' => $metadata['_routing'] ?? null,
                'body'    => $body,
            ], $metadata[$op]);

            switch ($op) {
                case 'index':
                    $this->index($_params);
                    break;

                case 'create':
                    $this->create($_params);
                    break;

                case 'update':
                    $this->update($_params);
                    break;

                case 'delete':
                    $this->delete($_params);
                    break;

                default:
                    throw new RuntimeException('Unknown operator.');
            }

            $offset += 2;
        }
    }
}
