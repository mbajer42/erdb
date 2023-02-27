# erdb - an educational relational dbms

A relational database just for fun and learning purposes. Storage layout is Postgres inspired. Work is still very much in progress. Quite a lot of things are missing.

Currently focusing on transactions. READ COMMITTED is default isolation level, SELECT and DELETE statements work already (UPDATE statements are currently in progress):

#### SELECT
![Example of select transactions](img/transactions.png)

#### DELETE
![Example of delete transactions](img/transaction_delete_commit.png)

![Example of delete transactions](img/transaction_delete_rollback.png)