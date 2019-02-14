from cassandra.cluster import Cluster

cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('test')
rows=cass_session.execute('select id, blah, label from de')
for user_row in rows:
    print(user_row.id, user_row.blah, user_row.label)
cass_session.execute(
"""
insert into de (id, blah, label)
values (%s, %s, %s)
""",
(3, 'what?', 4.0)
)
rows=cass_session.execute('select id, blah, label from de')
for user_row in rows:
    print(user_row.id, user_row.blah, user_row.label)
