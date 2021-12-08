from botocore.exceptions import ClientError

from single_table.SingleTableDAO import TABLE_NAME, TEAM_NAME, SORT_KEY

TEAM_TYPE = "Team"
WINS = "Wins"
TEAM_SORT_KEY = "TEAM#"

class TeamDAOV2():
    def __init__(self, session):
        self.team_table = session.resource('dynamodb').Table(TABLE_NAME)

    def write(self, team_name, wins):
        self.team_table.put_item(
            Item={
                TEAM_NAME: team_name,
                SORT_KEY: TEAM_SORT_KEY,
                WINS: wins,
            }
        )

    def read(self, team_name):
        try:
            response = self.team_table.get_item(Key={TEAM_NAME: team_name, SORT_KEY: TEAM_SORT_KEY}) # Use the SK to get only the "Team" items
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return self._from_dynamo_item(response['Item'])


    @staticmethod
    def _from_dynamo_item(dynamo_item):
        return {k: dynamo_item[k] for k in (TEAM_NAME, WINS)}