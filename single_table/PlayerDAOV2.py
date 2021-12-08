from boto3.dynamodb.conditions import Key

from single_table.SingleTableDAO import TEAM_NAME, SORT_KEY, TABLE_NAME

PLAYER_SORT_KEY_PREFIX = "PLAYER#"
PLAYER_TYPE = "Player"
PLAYER_NAME = "PlayerName"
SALARY = "Salary"
SALARY_INDEX = 'TeamSalary'

class PlayerDAOV2():
    def __init__(self, session):
        self.player_table = session.resource('dynamodb').Table(TABLE_NAME)

    def write(self, player_name, salary, team):
        self.player_table.put_item(
            Item={
                TEAM_NAME: team,
                SORT_KEY: self._get_sort_key(salary),
                PLAYER_NAME: player_name,
                SALARY: salary,
            }
        )

    def get_by_team(self, team_name):
        return list(map(self._from_dynamo_item, self.player_table.query(
            KeyConditionExpression=Key(TEAM_NAME).eq(team_name) & Key(SORT_KEY).begins_with(PLAYER_SORT_KEY_PREFIX)
        )["Items"]))

    # def get_by_salary(self, team_name):
    #     return list(map(self._from_dynamo_item,self.player_table.query(
    #         # IndexName=SALARY_INDEX,
    #         KeyConditionExpression=Key(TEAM_NAME).eq(team_name),
    #         ScanIndexForward=False,
    #     )["Items"]))

    @staticmethod
    def _get_sort_key(salary):
        # We reverse the salary so that larger numbers are longer strings (and there for are sorted alphabetically)
        return PLAYER_SORT_KEY_PREFIX + str(salary)[::-1]

    @staticmethod
    def _from_dynamo_item(dynamo_item):
        return {k: dynamo_item[k] for k in (PLAYER_NAME, TEAM_NAME, SALARY)}
