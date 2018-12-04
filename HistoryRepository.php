<?php

namespace go1\util_index;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;

class HistoryRepository
{
    private $db;

    public function __construct(Connection $db)
    {
        $this->db = $db;
    }

    public function write($type, $id, $status, $data = null, $timestamp = null)
    {
        $this->db->insert('index_history', [
            'type'      => $type,
            'id'        => $id,
            'status'    => $status,
            'data'      => !$data ? null : (is_string($data) ? $data : json_encode($data)),
            'timestamp' => $timestamp ?: time(),
        ]);

        if (class_exists(TestCase::class, false)) {
            if (is_string($data)) {
                trigger_error('error: ' . $data);
            }
        }
    }

    public function bulkLog(array $response)
    {
        if (empty($response['errors'])) {
            return null;
        }

        foreach ($response['items'] as $item) {
            foreach ($item as $action => $data) {
                if (empty($data['error'])) {
                    continue;
                }

                $this->write($data['_type'], $data['_id'], $data['status'], $data['error']);
            }
        }
    }
}
