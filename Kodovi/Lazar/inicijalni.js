db.getCollection("boxscores").aggregate(
[

  {
    $group: {
      _id: {game_id : "$game_id", teamName : "$teamName"   },
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


//2.Koji je igrac (po godini) imao najvecu prosecnu platu (uzimajuci u obzir
//i inflaciju) ?

db.getCollection("salaries").aggregate(
[
  {
    $group: {
      _id: {year : "$seasonStartYear", playerName : "$playerName"   },
      avgInflationSalary: {  $avg : "$inflationAdjSalary" },
    }
  },
  {$sort : {"_id.year" : -1,"avgInflationSalary" : -1}},
  {
      $group: {
      _id: {year : "$_id.year"  },
      playerName: { $first: "$_id.playerName" },
      maxAvgSalary: {  $first: "$avgInflationSalary"}
    }
   } ,
   {$sort : {"_id.year" : -1}},
   {
    $group: {
      _id: {playerName : "$playerName"   },
      playerCount: {  $count : {} },
    }
  },
      {$sort : {"playerCount" : -1}},
      {$limit: 1},
     {$project: {playerName: "$_id.playerName", _id: 0, playerCount: "$playerCount"}} 
   
])



//3.Koji igrac sa barem 100 odigranih utakmica je imao najveci point per minut
//u proseku na utakmicama sa preko 20000 gledaoca?

db.getCollection("boxscores").aggregate(
[

   {$match: {"attendance": {$gte: 20000}}},
       {
    $group: {
      _id: {playerName : "$playerName" },
      avgPPM: {  $avg : "$PPM" },
      gamesPlayed: {$count : {}},
    }
  },
    {$match: {"gamesPlayed": {$gte: 100}}},
    {$sort : {"avgPPM" : -1}},
    {$limit:1},
    {$project: {_id: 0,playerName: "$_id.playerName", avgPPM: {$round: [ "$avgPPM", 2 ]}  , gamesPlayed: "$gamesPlayed"}} 

])


//4.Prikazati igrace koji su u top 10 u prosecno postignutim asistencijama a da su odigrali barem 100 partija i pripadaju grupi top 100 prosecnih najboljih strelaca u ligi.

db.getCollection("boxscores").aggregate(
[
       {
    $group: {
      _id: { playerName : "$playerName" },
      avgPTS: {  $avg : "$PTS" },
      avgAST: {  $avg : "$AST" },
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



//5.Prikazati rangirano koliko su gostujuce ekipe u proseku, po sezonama postizale poena na kraju svakog od perioda.

db.getCollection("games_play_data").aggregate(
[
    {$match: {"period": {$lte: 4}}},
    {"$match" : 
   {
     "$or" : 
     [
       {"event_away": {$eq: 'End of 1st quarter'}},
       {"event_home": {$eq: 'End of 1st quarter'}},
       {"event_away": {$eq: 'End of 2nd quarter'}},
       {"event_home": {$eq: 'End of 2nd quarter'}},
       {"event_away": {$eq: 'End of 3rd quarter'}},
       {"event_home": {$eq: 'End of 3rd quarter'}},
       {"event_away": {$eq: 'End of 4th quarter'}},
       {"event_home": {$eq: 'End of 4th quarter'}},
     ]
   }
  },
      {
    $group: {
      _id: {seasonStartYear: "$seasonStartYear", awayTeam : "$awayTeam", period : "$period" },
      avgAwayTeamPoints: {  $avg : "$awayPoints" },
    }
  },
      {$sort : {"_id.seasonStartYear":1, "_id.period" : 1, "avgAwayTeamPoints":-1}},
  {$project: {_id:0,seasonStartYear: "$_id.seasonStartYear", _id: 0, period: "$_id.period", college: "$_id.awayTeam",avgAwayTeamPoints: "$avgAwayTeamPoints"}}

   
])



