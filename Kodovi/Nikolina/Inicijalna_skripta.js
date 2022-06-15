//INICJIALNA UPITI

//1
 
db.getCollection("boxscores").aggregate([
    { $match: {"seasonStartYear": {$in:[1996,2000] }}},
    { $group: {
            _id: { year: "$seasonStartYear" },
            home_team: {$first: "$homeTeam"}, 
            maximum_of_attendance: { $max: "$attendance" }}},
    { $sort: {"maximum_of_attendance": -1}},
    { $project: {
            _id: 0,
            Year: "$_id.year",
            Team: "$home_team",
            max_attendance: "$maximum_of_attendance"}}])
            
//2

db.getCollection("boxscores").aggregate([
    { $group: { _id: { seasonStartYear: "$seasonStartYear", awayTeam: "$awayTeam", homeTeam: "$homeTeam" },
            total_points: {$first: "$pointsTotal"},
            playerName: {$first: "$playerName"},
            three_p: { $avg: "$3P" }}},
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
            _for_three: "$three_p"}}])
            
            
//3
  

db.getCollection("coaches").aggregate([
    {$match: {$expr: {$eq: ["$W_reg", "$L_reg"]}}},
    {$group: {_id: {Name: "$coachName", type: "coachType"},
        win: {$first: "$W_reg"},
        lost: {$first: "$L_reg"},
        rang : {$max: "$Finish"}}},
    {$sort: {"rang": 1}},
    {$project: {
        _id : { Coach: "$_id.Name", Type: "$_id.type"},
        win:1,
        lost:1,
        rang:1}}])
        
//4

db.getCollection("boxscores").aggregate([
    { $match: {isStarter: 0}},
    { $group: { _id: {playerName: "$playerName", teamName: "$teamName"},
     avg_PTS: {$avg: "$PTS"},
     pf: {$first: "$PF"}}},
    { $group: {_id: {player: "$_id"},
     max_PTS: {$max: "$avg_PTS"},
     faults: {$max: "$pf"}}},
    { $sort: {"max_PTS": -1, "faults": -1}},
    {$limit:1},
    { $project: {_id: "$_id",
                 avg_pts: "$max_PTS",
                 fault: "$faults"}}]) 

//5

db.getCollection("games_play_data").aggregate([
    { $match: {$and: [{period:2}, {isRegular:1},
              {seasonStartYear:{$in:[2018,2019]}}]}},
    { $group: { _id: {year: "$seasonStartYear"},
                away_team: {$first: "$awayTeam"},
                home_team: {$first: "$homeTeam"},
                eventAway: {$first: "$event_away"}, 
                Attendance: {$max: "$attendance"}}},
     { $sort: {"Attendance": -1}},
     { $project:{
                _id:0, 
                Year: "$_id.year",
                Away_team: "$away_team", 
                Home_team: "$home_team",
                Event: "$eventAway", 
                attendance_: "$Attendance"}}])