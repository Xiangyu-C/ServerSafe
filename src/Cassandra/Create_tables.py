# Datastax Apache cassandra driver. do pip install cassandra-driver
from cassandra.cluster import Cluster
from tools import utility

if __name__ == '__main__':
    cass_session = utility.cass_conn()
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
