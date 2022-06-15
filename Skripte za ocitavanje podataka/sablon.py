# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 21:14:01 2022

@author: Lenovo T450
"""

import pymongo
import csv





class GamesBoxScoreParser:
    def __init__(self, file):
        self._file = file
       

    def add_games_boxscores_to_db(self, url, db_name):
        
        client = pymongo.MongoClient(url)
        db = client[db_name]
        games_boxscores = []
        with open(self._file, 'r', encoding = 'cp850') as csv_file_games:
            reader = csv.DictReader(csv_file_games)
            for row in reader:
                boxscores=[]
                with open('boxscores_shortened.csv', 'r', encoding = 'cp850') as csv_file_boxscore:
                    reader_boxscore = csv.DictReader(csv_file_boxscore)
                    for row_boxscore in reader_boxscore:
                        if(row['game_id']==row_boxscore['game_id']):
                            boxscore = get_boxscore(row_boxscore)
                            if(len(boxscore)!=0):
                                boxscores.append(boxscore)
                game_boxscore = get_game_boxscore(row,boxscores)
                if(len(game_boxscore)!=0):
                    games_boxscores.append(game_boxscore)

        if (len(games_boxscores) > 0):
            db['games_boxscores'].insert_many(games_boxscores)
                    
def get_game_boxscore(row,boxscores):


        return {  
            'seasonStartYear': row['seasonStartYear'],
            'awayTeam': row['awayTeam'],
            'pointsAway': int(row['pointsAway']), 
            'homeTeam': row['homeTeam'],
            'pointsHome': int(row['pointsHome']),
            'pointsTotal': int(row['pointsHome'])+int(row['pointsAway']),
            'attendance': parse_attendance(row['attendance']),
            'notes': row['notes'],
            'startET': row['startET'],
            'datetime': row['datetime'],
            'isRegular': int(row['isRegular']),
            'game_id': row['game_id'],
            'boxscores': boxscores,
            }
    


        
def get_boxscore(row) -> dict:

    if (row['MP']!='Player Suspended' and row['MP']!='Did Not Play' 
        and row['MP']!='Not With Team' and row['MP']!='Did Not Dress'):
        return {  
            'teamName': row['teamName'],
            'playerName': row['playerName'],
            'MP': row['MP'],
            '3P': int(row['3P']),
            '3PA':int(row['3PA']),   
            'AST': int(row['AST']),
            'PF': int(row['PF']),
            'PTS': int(row['PTS']),
            'isStarter': int(row['isStarter']),
            'PPM': get_ppm(row['MP'],row['PTS']),
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

 #   boxscore_parser = BoxScoreParser('boxscore.csv')
  #  boxscore_parser.add_boxscores_to_db(url = 'mongodb://localhost:27017/', db_name = 'nba_stats')
    
    games_boxscores_parser = GamesBoxScoreParser('games_shortened.csv')
    games_boxscores_parser.add_games_boxscores_to_db(url = 'mongodb://localhost:27017/', db_name = 'nba_stats')
0