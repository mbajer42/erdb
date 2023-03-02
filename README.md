# erdb - an educational relational dbms

A relational database just for fun and learning purposes. Storage layout is Postgres inspired. Work is still very much in progress. Quite a lot of things are missing.

Transaction isolation is achieved by MVCC. READ COMMITTED and REPEATABLE READ are already implemented (READ UNCOMMITTED will never work, maybe SERIALIZABLE some day).

### Examples of READ COMMITTED transactions (default isolation level)
#### SELECT
![Example of select transactions](img/transactions.png)

#### DELETE
![Example of delete transactions](img/transaction_delete_commit.png)

![Example of delete transactions](img/transaction_delete_rollback.png)

#### UPDATE
![Example of update transactions](img/transaction_update.png)

### Example of REPEATABLE READ transaction

![Example of repeatable read update transaction](img/transaction_repeatable_read.png)