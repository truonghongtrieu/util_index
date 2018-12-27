<?php

namespace go1\util_index;

use Doctrine\DBAL\Connection;
use go1\util\DB;
use go1\util\es\Schema;
use go1\util\model\User;
use stdClass;

class IndexHelper
{
    public static function loIndices(stdClass $lo)
    {
        $indices[] = Schema::portalIndex($lo->instance_id);

        return $indices;
    }

    public static function enrolmentIndices(stdClass $enrolment)
    {
        $portalId = $enrolment->taken_instance_id ?? null;
        $portalId && $indices[] = Schema::portalIndex($portalId);

        return $indices ?? [];
    }

    public static function awardEnrolmentIndices(stdClass $awardEnrolment, stdClass $award)
    {
        $portalId = $awardEnrolment->instance_id ?? null;
        $portalId && $indices[] = Schema::portalIndex($portalId);

        return $indices ?? [];
    }

    public static function userIsChanged(stdClass $user): bool
    {
        if (!isset($user->original)) {
            return false;
        }

        $original = User::create($user->original);
        $fields = ['first_name', 'last_name', 'avatar', 'roles', 'status'];
        $diff = array_intersect_key($original->diff($user), array_combine($fields, $fields));

        return !empty($diff);
    }

    public static function eckEntityIsChanged(stdClass $entity)
    {
        if (!isset($entity->original)) {
            return true;
        }

        $original = $entity->original;
        foreach ($entity as $key => $value) {
            if (!in_array($key, ['instance', 'entity_type', 'id', 'original']) && is_array($value)) {
                $originalValue = $original->{$key}[0]->value ?? null;
                $value = $value[0]->value ?? null;

                if ($originalValue != $value) {
                    return true;
                }
            }
        }

        return false;
    }

    public static function firstAssessor(Connection $db, array $assessorIds)
    {
        if (!$assessorIds) {
            return null;
        }

        $assessorId = reset($assessorIds);
        $assessor = $db
            ->executeQuery('SELECT id, mail, first_name, last_name, data FROM gc_user WHERE id = ?', [$assessorId])
            ->fetch(DB::OBJ);

        if (!$assessor) {
            return null;
        }

        if (isset($assessor->data) && is_scalar($assessor->data)) {
            $assessor->data = json_decode($assessor->data);
        }

        if (empty($assessor->first_name)) {
            $assessor->name = $assessor->last_name;
        } elseif (empty($assessor->last_name)) {
            $assessor->name = $assessor->first_name;
        } else {
            $assessor->name = "{$assessor->first_name} {$assessor->last_name}";
        }

        $assessor->avatar = isset($assessor->data->avatar->uri) ? $assessor->data->avatar->uri : null;
        unset($assessor->data);

        return $assessor;
    }
}
