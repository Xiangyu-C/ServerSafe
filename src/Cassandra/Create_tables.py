from cassandra.cluster import Cluster

cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('cyber_id')

# Create the predictions table to hold all all_predictions
# Use a compound primary to key to ensure uniqueness of each record
cass_session.execute("""
                    CREATE TABLE cyber_id.all_predictions ( 
                     "Timestamp" text,
                     "Source" text,
                     "Destination" text,
                     "Label" text,
                     prediction float,
                     PRIMARY KEY ("Timestamp", "Source", "Destination")
                   )
                   """)

# Create a table to hold aggregations of predicted attacks for all server ips
cass_session.execute("""
                    CREATE TABLE cyber_id.count_attack (
                     id text,
                     t timeuuid,
                     a float,
                     b float,
                     c float,
                     d float,
                     e float,
                     f float,
                     g float,
                     h float,
                     i float,
                     j float,
                     k float,
                     l float,
                     m float,
                     PRIMARY KEY (id, t)
                    ) WITH CLUSTERING ORDER BY (t DESC)
                """)
# Create a table to hold aggregations of total events happening at each server
cass_session.execute("""
                    CREATE TABLE cyber_id.count_traffic (
                     id text,
                     t timeuuid,
                     a float,
                     b float,
                     c float,
                     d float,
                     e float,
                     f float,
                     g float,
                     h float,
                     i float,
                     j float,
                     k float,
                     l float,
                     m float,
                     PRIMARY KEY (id, t)
                    ) WITH CLUSTERING ORDER BY (t DESC)
                """)
