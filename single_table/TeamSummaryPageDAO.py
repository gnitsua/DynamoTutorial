from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from single_table.PlayerDAOV2 import PLAYER_SORT_KEY_PREFIX, PlayerDAOV2
from single_table.SingleTableDAO import TABLE_NAME, TEAM_NAME, SORT_KEY
from single_table.TeamDAOV2 import TEAM_SORT_KEY, TeamDAOV2

class TeamSummaryPageDAO:
    def __init__(self, session):
        self.table = session.resource('dynamodb').Table(TABLE_NAME)

    def read(self, team_name):
        try:
            response = self.table.query(
                KeyConditionExpression=Key(TEAM_NAME).eq(team_name),
                ScanIndexForward=False
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return self._from_dynamo_item(response['Items'])

    @staticmethod
    def _from_dynamo_item(dynamo_item):
        team = TeamDAOV2._from_dynamo_item(next(filter(lambda item: item[SORT_KEY]==TEAM_SORT_KEY,dynamo_item))) # Only get one team item (since there should only be one)
        players = list(map(PlayerDAOV2._from_dynamo_item, filter(lambda item: item[SORT_KEY].startswith(PLAYER_SORT_KEY_PREFIX),dynamo_item)))

        return {
            "Team": team,
            "Players": players
        }

