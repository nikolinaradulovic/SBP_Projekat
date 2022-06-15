//4
db.getCollection("test_games_boxscores").aggregate(
[
{$unwind: "$boxscores"},
       {
    $group: {
      _id: { game_id : "game_id", playerName : "$boxscores.playerName" },
      avgPTS: {  $avg : "$boxscores.PTS" },
      avgAST: {  $avg : "$boxscores.AST" },
      gamesPlayed: {$count : {}},
      
    }
  },
       {$match: {"gamesPlayed": {$gte: 100}}},
      {$sort : {"avgPTS" : -1}},
      {$limit:100},
      {$sort : {"avgAST" : -1}},
      {$limit:10},
      {$project: {_id: 0,playerName: "$_id.playerName", avgAST: {$round: [ "$avgAST", 2 ]}}}  

])


//3
db.getCollection("test_games_boxscores").aggregate(
[
{$unwind: "$boxscores"},
       {$match: {"attendance": {$gte: 20000}}},
       {
    $group: {
      _id: {playerName : "$boxscores.playerName" },
      avgPPM: {  $avg : "$boxscores.PPM" },
      gamesPlayed: {$count : {}},
    }
  },
    {$match: {"gamesPlayed": {$gte: 100}}},
    {$sort : {"avgPPM" : -1}},
    {$limit:1},
    {$project: {_id: 0,playerName: "$_id.playerName", avgPPM: {$round: [ "$avgPPM", 2 ]}  , gamesPlayed: "$gamesPlayed"}} 
    
    ])
    
//6
db.getCollection("test_games_boxscores").aggregate(
[
{$unwind: "$boxscores"},
  {
    $group: {
      _id: {game_id : "$game_id", teamName : "$boxscores.teamName"   },
      numberOfDifferentPlayers: {  $count : {} },
    }
  },
    {$sort : {"_id.game_id" : 1}},
      {
    $group: {
      _id: { teamName : "$_id.teamName"   },
      avgNumberOfDifferentPlayersByTeam: {  $avg : "$numberOfDifferentPlayers" },
    }
  }, 
      {$sort : {"avgNumberOfDifferentPlayersByTeam" : -1}},
        {$limit: 1},  
     {$project: {teamName: "$_id.teamName", _id: 0,   avgNumberOfDifferentPlayersByTeam: {$round: [ "$avgNumberOfDifferentPlayersByTeam", 2 ]}}} 

])

