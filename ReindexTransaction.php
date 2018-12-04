<?php

namespace go1\util_index;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\util\DB;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use Pimple\Container;

class ReindexTransaction
{
    public  $db;
    public  $go1;
    public  $queue;
    public  $client;
    public  $id;
    public  $currentHandler;
    public  $handlers;
    public  $instance;
    public  $alias;
    public  $offset   = 0;
    public  $limit    = 500;
    public  $messages = [];
    public  $percent  = 0;
    public  $status;
    private $container;

    const NOT_STARTED = 'Not Started';
    const IN_PROGRESS = 'In Progress';
    const FINISHED    = 'Finished';

    public function __construct(
        Connection $db,
        Connection $go1,
        MqClient $queue,
        Client $client,
        Container $container
    ) {
        $this->db = $db;
        $this->go1 = $go1;
        $this->queue = $queue;
        $this->client = $client;
        $this->container = $container;
    }

    public function load(string $id): ReindexTransaction
    {
        $value = $this->db->fetchColumn('SELECT value FROM index_kv WHERE name = ?', [$id]);
        if (is_scalar($value)) {
            if ($value = json_decode($value)) {
                $this->id = $id;
                $this->currentHandler = $value->currentHandler;
                $this->handlers = $value->handlers;
                $this->instance = $value->instance ? PortalHelper::load($this->go1, $value->instance) : false;
                $this->alias = $value->alias;
                $this->messages = json_decode(json_encode((isset($value->messages) ? $value->messages : [])), true);
                $this->percent = $this->percent();
                $this->status = $value->status ?? static::NOT_STARTED;

                return $this;
            }
        }
    }

    public function percent()
    {
        $total = count($this->handlers);
        $processed = 0;
        foreach ($this->messages as $message) {
            if ($message['processed'] >= $message['total']) {
                $processed++;
            }
        }

        return ($processed > 0) ? round(100 * ($processed / $total)) : 0;
    }

    private function save()
    {
        $value = [
            'currentHandler' => $this->currentHandler,
            'handlers'       => $this->handlers,
            'instance'       => $this->instance ? $this->instance->id : false,
            'alias'          => $this->alias,
            'messages'       => $this->messages,
            'status'         => $this->status,
            'percent'        => $this->percent,
        ];

        DB::safeThread($this->db, 'index_kv', 3, function () use ($value) {
            $this->db->update('index_kv', ['value' => json_encode($value)], ['name' => $this->id]);
        });
    }

    public function next()
    {
        $messages = &$this->messages[$this->currentHandler];
        ++$messages['processed'];
        if ($messages['processed'] >= $messages['total']) {
            return $this->nextHandler();
        }
        $this->save();
    }

    private function nextHandler()
    {
        $position = array_search($this->currentHandler, $this->handlers);

        if ($position !== false) {
            if (isset($this->handlers[++$position])) {
                $this->currentHandler = $this->handlers[$position];

                return $this->queue();
            }
        }

        return $this->finish();
    }

    private function finish()
    {
        if (Schema::INDEX != $this->alias) {
            $params['body']['actions'][]['add'] = [
                'index' => $this->alias,
                'alias' => Schema::INDEX,
            ];

            $params['body']['actions'][]['remove_index'] = [
                'index' => Schema::INDEX,
            ];
            $this->client->indices()->updateAliases($params);
        }

        $name = $this->id . ':finish:' . time();

        $this->status = static::FINISHED;
        $this->percent = 100;
        $this->save();
        $this->db->update('index_kv', ['name' => $name], ['name' => $this->id]);
    }

    public function queue()
    {
        $handler = $this->container["reindex.handler.{$this->currentHandler}"];
        $total = $handler->count($this);
        $this->messages[$this->currentHandler]['total'] = ceil($total / $this->limit);
        $this->messages[$this->currentHandler]['processed'] = 0;
        $this->status = static::IN_PROGRESS;
        $this->save();

        if ($total == 0) {
            return $this->nextHandler();
        }

        for ($offset = 0; $offset < $total; $offset += $this->limit) {
            $this->queue->queue(
                ['name' => $this->currentHandler, 'id' => $this->id, 'offset' => $offset],
                ReindexServiceProvider::WORKER_REINDEX
            );
        }
    }
}
