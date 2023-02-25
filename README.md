# erdb - an educational relational dbms

A relational database just for fun and learning purposes. Storage layout is Postgres inspired. Work is still very much in progress. Quite a lot of things are missing.

Currently focusing on transactions. READ COMMITTED is default isolation level and works already (but DELETE and UPDATE statements are not implemented yet):

![Example of transactions](img/transactions.png)