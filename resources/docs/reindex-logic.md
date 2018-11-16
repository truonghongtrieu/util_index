Reindex logic
====

1. First, create new index
2. Loop all of resources need to be index. Each resource has a particular handler  
3. Count items in each handler and separates into messages
4. Queue will process messages in parallel
5. Wait for all messages of handler processed then move to next handler until finish all of handlers.
6. Finally, replace current index by new one via ES alias
