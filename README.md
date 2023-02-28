# erdb - an educational relational dbms

A relational database just for fun and learning purposes. Storage layout is Postgres inspired. Work is still very much in progress. Quite a lot of things are missing.

Currently focusing on transactions (using MVCC). READ COMMITTED is already implemented (READ UNCOMITTED will never work, currently working on REPEATABLE READ, maybe SERIALIZABLE some day).

### Examples of READ COMMITTED transactions (default isolation level)
#### SELECT
![Example of select transactions](img/transactions.png)

#### DELETE
![Example of delete transactions](img/transaction_delete_commit.png)

![Example of delete transactions](img/transaction_delete_rollback.png)

#### UPDATE
![Example of update transactions](img/transaction_update.png)