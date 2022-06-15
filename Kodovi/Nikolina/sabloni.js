//sablon
//1

db.getCollection("games_boxscores").aggregate([
     {$match:{seasonStartYear:{$in:[1996,2000]}}},
     { $group: {
            _id: { year: "$seasonStartYear" },
            home_team: {$first: "$homeTeam"}, 
            maximum_of_attendance: { $max: "$attendance" }}},
    { $sort: {"maximum_of_attendance": -1}},
    { $limit: 10 },
    { $project: {
            _id: 0,
            Year: "$_id.year",
            Team: "$home_team",
            max_attendance: "$maximum_of_attendance"}}
])

//2
db.getCollection("games_boxscores").aggregate([
    
   {$unwind: "$boxscores"},
   { $group: { _id: { seasonStartYear: "$seasonStartYear", awayTeam: "$awayTeam", homeTeam: "$homeTeam" },
            total_points: {$first: "$pointsTotal"},
            playerName: {$first: "$boxscores.playerName"},
            three_p: { $avg: "$boxscores.3P" }}},
    { $match: { "total_points": { $lt: 200 }}},
    { $sort: { "three_p": -1 }},
    { $limit: 10},
    { $project: {
            _id: 0,
            seasonStartYear: "$_id.seasonStartYear",
            away_team: "$_id.awayTeam",
            home_team: "$_id.homeTeam",
            sum: "$total_points",
            Name_of_player: "$playerName",
            _for_three: "$three_p"}}
    ])
    
//4
db.getCollection("games_boxscores").aggregate([
    {$unwind: "$boxscores"},
    { $match: {"boxscores.isStarter":0}},
    { $group: { _id: {playerName: "$boxscores.playerName", teamName: "$boxscores.teamName"},
     avg_PTS: {$avg: "$boxscores.PTS"},
     pf: {$first: "$boxscores.PF"}}},
    { $group: {_id: {player: "$_id"},
     max_PTS: {$max: "$avg_PTS"},
     faults: {$max: "$pf"}}},
    { $sort: {"max_PTS": -1, "faults": -1}},
    {$limit:1},
    { $project: {_id: "$_id",
                 avg_pts: "$max_PTS",
                 fault: "$faults"}}])           
                 
