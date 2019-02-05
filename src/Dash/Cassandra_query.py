from cassandra.cluster import Cluster

# Utility script for querying cassandra tables

def make_session(table_name):
    """
    Build a connetion to a Cassandra table and return the session
    """
    cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
    cass_session = cluster.connect(table_name)
    return(cass_session)

def accuracy_query(table_name):
    """
    Fetch the prediction table as a dataframe and calculate
    accuarcies as well as the total count of each prediciton
    """
    session = make_session('cyber_ml')
    df = pd.DataFrame(list(session.execute('select * from'+' '+ table_name)))
    total_benign = df['true_label'].value_counts()['Benign']
    total_malicious = len(df)-total_benign
    benign_predicted = df[(df['prediction']==0) & (df['true_label']=='Benign')].count()
    malicious_predicted = df[(df['prediction']==1) & (df['true_label']!='Benign')].count()
    benign_rate = str(round((benign_predicted/total_benign)[0],2))
    malicious_rate = str(round((malicious_predicted/total_malicious)[0], 2))

    return(total_benign, total_malicious, benign_rate, malicious_rate)

def server_traffic():
    """
    Cacluate potential attacks happening at each server every second
    """
    
