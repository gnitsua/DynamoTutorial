from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

PLAYER_NAME = "PlayerName"
SALARY = "Salary"
PLAYER_TEAM = "Team"
PLAYER_TABLE_NAME = "Players-abc123"
SALARY_INDEX = 'TeamSalary'

class PlayerDAO():
    def __init__(self, session):
        self.player_table = session.resource('dynamodb').Table(PLAYER_TABLE_NAME)

    def write(self, player_name, salary, team):
        self.player_table.put_item(
            Item={
                PLAYER_NAME: player_name,
                SALARY: salary,
                PLAYER_TEAM: team
            }
        )

    def read(self, player_name):
        try:
            response = self.player_table.get_item(Key={PLAYER_NAME: player_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response['Item']

    def get_by_team(self, team_name):
        return self.player_table.query(
            KeyConditionExpression=Key(PLAYER_TEAM).eq(team_name)
        )["Items"]

    @staticmethod
    def _from_dynamo_item(dynamo_item):
        return {k: dynamo_item[k] for k in (PLAYER_NAME, PLAYER_TEAM, SALARY)}

    # def get_by_salary(self, team_name):
    #     return self.player_table.query(
    #         IndexName=SALARY_INDEX,
    #         KeyConditionExpression=Key(PLAYER_TEAM).eq(team_name),
    #         ScanIndexForward=False,
    #         Limit=1
    #     )["Items"]
