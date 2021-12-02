import boto3
from PlayerDAO import PLAYER_NAME, SALARY, PLAYER_TABLE_NAME, PlayerDAO, PLAYER_TEAM, SALARY_INDEX
from TeamDAO import TeamDAO, WINS, TEAM_TABLE_NAME, TEAM_NAME

# I work for AWS, my opinions are my own.

# DynamoDB was launched by Amazon in 2021
# Response to 2004 retail outages
# Original paper 2007 [1]
# Had more than 7500 Oracle Databases that were migrated [2]
# Project rolling stone

# Dynamo is a NoSQL database that focuses on simplicity.
# First we need credentials
AWS_ACCESS_KEY = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN = ""

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)
client = session.client('dynamodb')


def create_team_table():
    # Now let's create our first table.
    # The docs for this API are here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    # One major difference between traditional databases and Dynamo is that these tables should be designed based on your
    # access pattern rather than normalized. This is because the primary key you choose is the ONLY way you can select
    # an item without doing a table scan. While being able to make general queries is incredibly helpful during prototyping
    # and when doing data analytics, often the production use case for a database can be classified into only a few queries.
    # Often the recommendation is to use a more generic database engine such as MySQL (or Aurora/RDS) for back end analytics
    # and Dynamo for Production. AWS publishes this idea as "Purpose Built Databases" https://aws.amazon.com/products/databases/

    # For this example we know we are going to need three queries for our application.
    # 1. Wins by team
    # 2. What players played for which teams
    # 3. Highest paid player on that team

    # Dynamo is "schema-less". When we specify our attributes we have a choice of 3 types: String, Number, Binary (TODO, what about map).
    # This does not mean that Dynamo is magic and handles your schema for you, quite the opposite. Dynamo pushes
    # schema management into your application, primarily because of the performance benefits. This can be incredibly
    # dangerous, your application is now responsible for preventing illegal schema writes.
    # Note that when defining a table you actually don't need to specify any attributes you don't plan to use for a key.
    # Because this is a schema-less database, you are free to add those columns at a later date
    # We will touch on this later, but it is a major challenge of managing a dynamo table
    AttributeDefinitions = [
        {
            'AttributeName': TEAM_NAME,
            'AttributeType': 'S'
        }
    ]

    # There are two types of key types in Dynamo: Hash and Range. For now we are just going to include a single attribute
    # in our
    KeySchema = [
        {
            'AttributeName': TEAM_NAME,
            'KeyType': 'HASH'
        },
    ]

    # First lets make sure the table doesn't already exist
    try:
        client.delete_table(TableName=TEAM_TABLE_NAME)
        print("%s already exists, removing" % TEAM_TABLE_NAME)
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=TEAM_TABLE_NAME)
    except client.exceptions.ResourceNotFoundException as e:
        print("%s does not exist, creating" % TEAM_TABLE_NAME)

    # Then we can create the teams table
    print("Creating Table: %s" % TEAM_TABLE_NAME)
    client.create_team_table(
        AttributeDefinitions=AttributeDefinitions,
        KeySchema=KeySchema,
        TableName=TEAM_TABLE_NAME,
        BillingMode='PAY_PER_REQUEST'  # TODO: explain provisioned
    )

    print("Waiting for table creation")
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=TEAM_TABLE_NAME)

    print(client.describe_table(
        TableName=TEAM_TABLE_NAME
    ))

    # Now we can add data to our table. A frequent pattern when working with Dynamo is to use a Data Access Object
    # to abstract away any table specific logic such as maintaining schema
    # https://en.wikipedia.org/wiki/Data_access_object#:~:text=In%20computer%20software%2C%20a%20data,exposing%20details%20of%20the%20database.
    teams = [("Phillies", 81), ("Yankees", 103), ("Dodgers", 106)]

    team_table = TeamDAO(session)

    for team in teams:
        team_table.write(team[0], team[1])


def get_team_data():
    team_table = TeamDAO(session)

    # Now we are ready to complete our first query. Let's get the number of wins the Phillies had in 2019
    print("The Philles had %s wins in 2019" % team_table.read("Phillies")[WINS])

    # If we were to request an attribute that didn't exist we get a key error, this is just coming from
    # Python in this case, since the Dynamo API just returns an object
    try:
        print("The Philles had %s wins in 2019" % team_table.read("Phillies")["fake-attribute"])
    except KeyError as e:
        print("Key %s does not exist" % e)


def create_player_table():
    # We are now going to create a second table for players. There are actually several designs you could use here
    # 1. Single table: Because we are able to create compound keys in Dynamo, it is possible to store multiple types
    # of records in a single table. For more information see https://www.alexdebrie.com/posts/dynamodb-single-table/
    # 2. Store an array of players inside the team item, as JSON. For more information see https://aws.amazon.com/blogs/developer/storing-json-documents-in-amazon-dynamodb-tables/
    # 3. Multiple tables: Perhaps the most obvious, since players will probably be called independently with different access patters.


    # Using a seperate table will allow us to take advantage of a Secondary Index to acount for our seconda dn third query.
    # Secondary Indexes in Dynamo allow you to use different keys to enable a different access pattern of the same data.
    # They come in two flavors: Global and Local. The differences are explained here: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-indexes-general.html
    # In this case we can use a local index, because we are using the same partition key. Local indexes are beneficial because
    # they don't require a copy of the data to be made, unlike a GSI. They also do not have the same coonsistency issues,
    # which can cause race conditions when not properly accounted for.
    AttributeDefinitions = [
        {
            'AttributeName': PLAYER_TEAM,
            'AttributeType': 'S'
        },
        {
            'AttributeName': PLAYER_NAME,
            'AttributeType': 'S'
        },
        {
            'AttributeName': SALARY,
            'AttributeType': 'N'
        }
    ]

    KeySchema = [
        {
            'AttributeName': PLAYER_TEAM,
            'KeyType': 'HASH'
        },
        {
            'AttributeName': PLAYER_NAME,
            'KeyType': 'RANGE'
        }
    ]

    LocalSecondaryIndexes = [
        {
            'IndexName': SALARY_INDEX,
            'KeySchema': [
                {
                    'AttributeName': PLAYER_TEAM,
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': SALARY,
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL',
            }
        },
    ]

    # First lets make sure the table doesn't already exist
    try:
        client.delete_table(TableName=PLAYER_TABLE_NAME)
        print("%s already exists, removing" % PLAYER_TABLE_NAME)
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=PLAYER_TABLE_NAME)
    except client.exceptions.ResourceNotFoundException as e:
        print("%s does not exist, creating" % PLAYER_TABLE_NAME)

    # Then we can create the players table
    print("Creating Table: %s" % PLAYER_TABLE_NAME)
    client.create_table(
        AttributeDefinitions=AttributeDefinitions,
        KeySchema=KeySchema,
        TableName=PLAYER_TABLE_NAME,
        LocalSecondaryIndexes=LocalSecondaryIndexes,
        BillingMode='PAY_PER_REQUEST',
    )

    print("Waiting for table creation")
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=PLAYER_TABLE_NAME)

    print(client.describe_table(
        TableName=PLAYER_TABLE_NAME
    ))

    # Lets add data to this table
    players = [("Bryce Harper", 11538462, "Phillies"), ("Rhys Hoskins", 57500, "Phillies"),
               ("J.A. Happ", 17000000, "Yankees"), ("Giancarlo Stanton", 26000000, "Yankees"),
               ("Homer Bailey", 23000000, "Dodgers"), ("Justin Turner", 19000000, "Dodgers")]

    player_table = PlayerDAO(session)

    for player in players:
        player_table.write(player[0], player[1], player[2])


def get_player_data():
    player_table = PlayerDAO(session)

    # Now it's time for our second query. We will take advantage of the range index to get the players in salary order
    # We could also use a scan query, but that's expensive
    phillies = player_table.get_by_team(team_name="Phillies")
    print("The following players play for the Phillies:")
    for player in phillies:
        print(player[PLAYER_NAME])


def get_top_salary():
    # Now time for our last query. In the previous step we set up our GSI to have a sort key of salary. This means that
    # We can simply return the first player to get the highest salary. Unfortunately Dynamo sorts low to high, so we
    # have to add "ScanIndexForward=False".
    player_table = PlayerDAO(session)
    player = player_table.get_by_salary(team_name="Phillies")[0]
    print("%s has the highest salary of $%s" % (player[PLAYER_NAME], "{:,}".format(player[SALARY])))


if __name__ == "__main__":
    # create_team_table()
    # get_team_data()
    create_player_table()
    get_player_data()
    get_top_salary()
    pass

# Source:
# https://www.allthingsdistributed.com/2012/01/amazon-dynamodb.html
# http://www.cs.cornell.edu/courses/cs5414/2017fa/papers/dynamo.pdf
# https://aws.amazon.com/blogs/aws/migration-complete-amazons-consumer-business-just-turned-off-its-final-oracle-database/
# https://softwareengineeringdaily.com/2020/07/02/dynamodb-with-alex-debrie/
