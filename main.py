import os

import boto3

from single_table import SingleTableDAO
from multitable.PlayerDAO import PLAYER_NAME, SALARY, PLAYER_TABLE_NAME, PlayerDAO, PLAYER_TEAM
from single_table.PlayerDAOV2 import PlayerDAOV2
from single_table.SingleTableDAO import TABLE_NAME, SORT_KEY
from multitable.TeamDAO import TeamDAO, WINS, TEAM_TABLE_NAME, TEAM_NAME

# I work for AWS, my opinions are my own.

# DynamoDB was launched by Amazon in 2012
# Response to 2004 retail outages
# Original paper 2007 [1]
# Had more than 7500 Oracle Databases that were migrated [2]
# Project rolling stone

# Dynamo is a NoSQL database that focuses on simplicity.
# First we need credentials
from single_table.TeamDAOV2 import TeamDAOV2
from single_table.TeamSummaryPageDAO import TeamSummaryPageDAO
from helpers import delete_table

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    # aws_session_token=os.environ["AWS_SESSION_TOKEN"] # Note that you may not have a session token if you are using an IAM User, just comment this out
)
client = session.client('dynamodb')


def create_team_table():
    # Now let's create our first table.
    # The docs for this API are here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    # One major difference between traditional databases and Dynamo is that these tables should be designed based on your
    # access pattern rather than normalized. This is because Dynamo does not suppot "joins".
    # While being able to make general queries is incredibly helpful during prototyping and when doing data analytics, often
    # the production use case for a database can be classified into only a few queries.
    # Often the recommendation is to use a more generic database engine such as MySQL (or Aurora/RDS) for back end analytics
    # and Dynamo for Production. AWS publishes this idea as "Purpose Built Databases" https://aws.amazon.com/products/databases/

    # For this example we know we are going to need three queries for our application.
    # 1. Wins by team
    # 2. What players played for which teams
    # 3. Highest paid player on that team

    # Dynamo is "schema-less". This does not mean that Dynamo is magic and handles your schema for you, quite the opposite.
    # Dynamo pushes schema management into your application, primarily because of the performance benefits. This can be incredibly
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

    # There are two types of key types in Dynamo: Partition and Sort. Partition keys are used to distribute your data
    # across storage nodes in your database. It is important to use a high-cardinality attribute as a partition key
    # since you don't want all of your data access to hit a single partition. Partition keys are always of type "HASH",
    # and sort keys are always of type "RANGE". For our Team database we are going to only
    # specify a partition key.
    # https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/
    KeySchema = [
        {
            'AttributeName': TEAM_NAME,
            'KeyType': 'HASH'
        },
    ]

    # First lets make sure the table doesn't already exist
    delete_table(client, TEAM_TABLE_NAME)

    # Then we can create the Teams table
    print("Creating Table: %s" % TEAM_TABLE_NAME)
    client.create_table(
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


    # Using a seperate table will allow us to take advantage of a Secondary Index to acount for our second and third query.
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
            'AttributeName': SALARY,
            'AttributeType': 'N'
        }
    ]

    # For our player table we are also specifying a Sort Key. Sort Keys are optional, and are used when you have multiple
    # records with the same partition key. The key (partition,sort) must be unique. The Sort Key is useful for gathering related
    # information togethet or defining one-to-many relationships in your data. In the case of our Player table we will
    # use a sort key to define the team->player relationship
    KeySchema = [
        {
            'AttributeName': PLAYER_TEAM,
            'KeyType': 'HASH'
        },
        {
            'AttributeName': SALARY,
            'KeyType': 'RANGE'
        }
    ]

    # First lets make sure the table doesn't already exist
    delete_table(client, PLAYER_TABLE_NAME)

    # Then we can create the players table
    print("Creating Table: %s" % PLAYER_TABLE_NAME)
    client.create_table(
        AttributeDefinitions=AttributeDefinitions,
        KeySchema=KeySchema,
        TableName=PLAYER_TABLE_NAME,
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
    # Now it's time for our second query.
    phillies = player_table.get_by_team(team_name="Phillies")
    print("The following players play for the Phillies:")
    for player in phillies:
        print(player[PLAYER_NAME])

    print("The top paid player is %s"%(phillies[0][PLAYER_NAME]))

def single_table():
    # So what happens if we are at huge scale? Right now if we want a "Team Summary" page that includes information about both players and teams. Right now we would have to make two queries to get that
    # (one to the teams table and one to the players table). We know our access pattern is going to always call the same set of queries, so we should "pre-join"
    # our data to match. We will make use of a single table design to do this. We take advantage of the no sql aspect of Dynamo here, we will have two different
    # types of items in a single table, so all items will not have all attributes. It is up to our access layer to help us make sense of this.

    # We are going to make the assumption that the majority of our access patterns are going to start with "WHERE Team=Something AND...", so our patition key
    # will be Team. In other cases you may want to use a customer id, or other identifier that groups like records together. For more information see https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/

    # One common pattern in Dynamo is to use a compound sort key to enable several access patterns.
    AttributeDefinitions = [
        {
            'AttributeName': SingleTableDAO.TEAM_NAME,
            'AttributeType': 'S'
        },
        {
            'AttributeName': SORT_KEY,
            'AttributeType': 'S'
        }
    ]

    KeySchema = [
        {
            'AttributeName': SingleTableDAO.TEAM_NAME,
            'KeyType': 'HASH'
        },
        {
            'AttributeName': SORT_KEY,
            'KeyType': 'RANGE'
        },
    ]

    # # Dynamodb does not have a concept of a "join", instead we create "Indexes" to enable our access patterns. These can be
    # # "Global Secondary Indexes" which actually make a copy of your data (effectively a second table with the same data
    # # which dynamo (lazily) keeps in sync with your main table), and "Local Secondary Indexes". LSIs are more limited in that
    # # they must have the same parition key, but do not require a copy of your data and do not have the same consistency
    # # concerns as GSIs. In this case we are going to use a LSI because query #3 will still be using "Team" as the parition key.
    #
    # First lets make sure the table doesn't already exist
    delete_table(client, TABLE_NAME)

    # Then we can create the players table
    print("Creating Table: %s" % TABLE_NAME)
    client.create_table(
        AttributeDefinitions=AttributeDefinitions,
        KeySchema=KeySchema,
        TableName=TABLE_NAME,
        BillingMode='PAY_PER_REQUEST',
    )

    print("Waiting for table creation")
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=TABLE_NAME)

    teams = [("Phillies", 81), ("Yankees", 103), ("Dodgers", 106)]

    team_table = TeamDAOV2(session)

    for team in teams:
        team_table.write(team[0], team[1])

    players = [("Rhys Hoskins", 57500, "Phillies"), ("Bryce Harper", 11538462, "Phillies"),
               ("J.A. Happ", 17000000, "Yankees"), ("Giancarlo Stanton", 26000000, "Yankees"),
               ("Homer Bailey", 23000000, "Dodgers"), ("Justin Turner", 19000000, "Dodgers")]

    player_table = PlayerDAOV2(session)

    for player in players:
        player_table.write(player[0], player[1], player[2])

def get_single_data():
    team_summary_table = TeamSummaryPageDAO(session)
    print(team_summary_table.read("Phillies"))




# Operating a DynamoDB database:
# SLA 4 9's https://aws.amazon.com/dynamodb/sla/
# For customer facing workloads we usually use provisioned capacity to reduce p99 latency
# Backups/Global tables for higher reliability
# DynamoDB table throttle metrics
# 80% of any table's available read or write IOPS


if __name__ == "__main__":
    create_team_table()
    get_team_data()
    create_player_table()
    get_player_data()
    single_table()
    get_single_data()
    pass

# Source:
# https://www.allthingsdistributed.com/2012/01/amazon-dynamodb.html
# http://www.cs.cornell.edu/courses/cs5414/2017fa/papers/dynamo.pdf
# https://aws.amazon.com/blogs/aws/migration-complete-amazons-consumer-business-just-turned-off-its-final-oracle-database/
# https://softwareengineeringdaily.com/2020/07/02/dynamodb-with-alex-debrie/
# https://github.com/ferdingler/dynamodb-single-table
# https://www.alexdebrie.com/posts/dynamodb-single-table/
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-modeling-nosql-B.html
# https://github.com/alexdebrie/dynamodb-instagram
