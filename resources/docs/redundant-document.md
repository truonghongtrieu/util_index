Redundant document
====

Redundant document are record exist in elastic search but no longer exist in database. It appears when #index failed to 
process message or message never delivery to #index.

## How to recognise redundant document?

A document stored in elastic search has a field `metadata.updated_at`. It indicates latest time the document updated.
After we do re-index for a certain portal, if records do not update latest time, It's out of date.

## How to remove redundant document?

Each type of data has its own re-index handler which fetch data from database then push to elastic search. We can 
implement remove redundant in there.

For instance, we have implemented for LoReindex:

    public function removeRedundant(Task $task): BuilderInterface
    {
        $query = new BoolQuery();
        $query->add(new RangeQuery('metadata.updated_at', [RangeQuery::LT => $task->updated]));
        $query->add(new TermQuery('_type', Schema::O_LO));

        if ($task->instance) {
            $query->add(new TermQuery('metadata.instance_id', $task->instance->id));
            $query->add(new TermQuery('instance_id', $task->instance->id));
        }

        return $query;
    }
