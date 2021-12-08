def delete_table(client, table_name):
    try:
        client.delete_table(TableName=table_name)
        print("%s already exists, removing" % table_name)
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
    except client.exceptions.ResourceNotFoundException as e:
        print("%s does not exist, creating" % table_name)

def delete_table(client, table_name):
    try:
        client.delete_table(TableName=table_name)
        print("%s already exists, removing" % table_name)
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
    except client.exceptions.ResourceNotFoundException as e:
        print("%s does not exist, creating" % table_name)