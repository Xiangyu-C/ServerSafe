from cassandra.cluster import cluster

cluster = Cluster([18.232.2.76])
cass_session = cluster.connect('cyber_id')
rows=session.execute('select id, blah, label from de')
for user_row in rows:
    print(user_row.id, user_row.blah, user_row.label)
