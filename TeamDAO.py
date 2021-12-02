from botocore.exceptions import ClientError

TEAM_NAME = "TeamName"
WINS = "Wins"
TEAM_TABLE_NAME = "Teams-abc123"

class TeamDAO():
    key_prefix = "team_"

    def __init__(self, session):
        self.team_table = session.resource('dynamodb').Table(TEAM_TABLE_NAME)

    def write(self, team_name, wins):
        self.team_table.put_item(
            Item={
                TEAM_NAME: self.key_prefix + team_name,
                WINS: wins,
            }
        )

    def read(self, team_name):
        try:
            response = self.team_table.get_item(Key={TEAM_NAME: self.key_prefix + team_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response['Item']