# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 21:21:32 2022

@author: Lenovo T450
"""
from dateutil import parser
import pymongo
import csv


class SalaryParser:
    def __init__(self, file):
        self._file = file
        

    def add_salaries_to_db(self, url, db_name):
        client = pymongo.MongoClient(url)
        db = client[db_name]
        salaries = []
        self._salaries = {}
        with open(self._file, 'r', encoding = 'cp850') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                salary = get_salary(row)
                salaries.append(salary)
                
                
        db['salaries'].insert_many(salaries)
        
def get_salary(row) -> dict:
    return {
        'playerName': row['playerName'],
        'seasonStartYear': row['seasonStartYear'],
        'salary': int(row['salary'][1:].replace(',', '')),
        'inflationAdjSalary': int(row['inflationAdjSalary'][1:].replace(',', '')),
    }

class PlayerInfoParser:
    def __init__(self, file):
        self._file = file
        

    def add_players_to_db(self, url, db_name):
        client = pymongo.MongoClient(url)
        db = client[db_name]
        players = []
        self._players = {}
        with open(self._file, 'r', encoding = 'cp850') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                player = get_player(row)
                players.append(player)
                
                
        db['players'].insert_many(players)
        
def get_player(row) -> dict:
    return {
        'playerName': row['playerName'],
        'From': row['From'],
        'To': row['To'],
        'Pos': row['Pos'],
        'Ht': row['Ht'],
        'Wt': row['Wt'],
        'birthDate': row['birthDate'],
        'Colleges': row['Colleges'],
    }

class BoxScoreParser:
    def __init__(self, file):
        self._file = file
        

    def add_boxscores_to_db(self, url, db_name):
        
        client = pymongo.MongoClient(url)
        db = client[db_name]
        boxscores = []
        self._boxscores = {}
        with open(self._file, 'r', encoding = 'cp850') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                boxscore = get_boxscore(row)
                if(len(boxscore)!=0):
                    boxscores.append(boxscore)

                
                
        db['boxscores'].insert_many(boxscores)
        
def get_boxscore(row) -> dict:

    if (row['MP']!='Player Suspended' and row['MP']!='Did Not Play' 
        and row['MP']!='Not With Team' and row['MP']!='Did Not Dress'):
        return {  
            'game_id': row['game_id'],
            'teamName': row['teamName'],
            'playerName': row['playerName'],
            'MP': row['MP'],
            'FG': int(row['FG']),
            'FGA': int(row['FGA']),
            '3P': int(row['3P']),
            '3PA':int(row['3PA']),
            'FT': int(row['FT']),
            'FTA': int(row['FTA']),
            'ORB': int(row['ORB']),
            'DRB': int(row['DRB']),
            'TRB': int(row['TRB']),
            'AST': int(row['AST']),
            'STL': int(row['STL']),
            'BLK': int(row['BLK']),
            'TOV': int(row['TOV']),
            'PF': int(row['PF']),
            'PTS': int(row['PTS']),
            '+/-': row['+/-'],
            'isStarter': int(row['isStarter']),
            'PPM': get_ppm(row['MP'],row['PTS']),
            'attendance' : parse_attendance(row['attendance']),
            'seasonStartYear': row['seasonStartYear'],
            'homeTeam': row['homeTeam'],
            'awayTeam': row['awayTeam'],
            'pointsHome': int(row['pointsHome']),
            'pointsAway': int(row['pointsAway']),
            'pointsTotal': int(row['pointsHome'])+int(row['pointsAway']),
            
            }
    else:
        return {}
    

def parse_attendance(attendance):
    if (attendance==""):
        return 0
    return int(float(attendance))
    
def get_ppm(time,pts):
    if(time[0]=='0'):
        return 0
    if(time[1]==':'):
        return round(int(pts)/int(time[:1]),2)
    return round(int(pts)/int(time[:2]),2)



if __name__ == '__main__':
    salary_parser = SalaryParser('salaries.csv')
    salary_parser.add_salaries_to_db(url = 'mongodb://localhost:27017/', db_name = 'nba_stats')

    player_parser = PlayerInfoParser('player_info.csv')
    player_parser.add_players_to_db(url = 'mongodb://localhost:27017/', db_name = 'nba_stats')

    boxscore_parser = BoxScoreParser('boxgames.csv')
    boxscore_parser.add_boxscores_to_db(url = 'mongodb://localhost:27017/', db_name = 'nba_stats')