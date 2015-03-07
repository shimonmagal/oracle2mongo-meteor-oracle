This is an attemt to create a replicator from oracle to mongoDB.

The replicator is designed to be a continuous replicator, but will also have a version for a one-time replication.

You can use this replicator for a bunch of purposes:

1. Migration - if you are migrating from Oracle to MongoDB, you can use this replicator to create a copy of your DB in the MongoDB format.

2. Mixed mode - set the replicator in live setting, and enjoy keeping your infromation both in tabular format, for the advantages of supporting existing code, transactions etc., but also keep a mongoDB copy for sharding, text and geo-searches, and out-of-the-box json query responses for json-loving frontend, like web.

3. The most exciting purpose - Do you have an existing system which is coupled with your DB? Yet, you wish to create or upgrade to a web frontend with all of the great features meteorjs provides? Well, now (soon) you will be able to. By continuously replicating your Oracle db to MongoDB, you will be able to close a circuit and get a reactive web-app using your oracle DB.