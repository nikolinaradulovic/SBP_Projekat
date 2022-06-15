//INDEKSIRANI UPITI

//1
db.boxscores.createIndex({"seasonStartYear":1})
 

//2
db.boxscores.createIndex({"three_p":1}) 
                        
//3
db.coaches.createIndex({"rang":1})  


        
//4
db.boxscores.createIndex({"max_PTS":1})


//5

db.games_play_data.createIndex({"Attendance":1})

