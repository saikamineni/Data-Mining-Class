# part - 1

ratings = sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/ratings.dat")

users = sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/users.dat")

users_rdd = users.map(lambda x: (x.split('::')[0],x.split('::')[1]))
ratings_rdd = ratings.map(lambda x: (x.split('::')[0], x.split('::')[1], x.split('::')[2]))


joined_rdd = users_rdd.map(lambda x:(x[0],x[1])).join(ratings_rdd.map(lambda x:(x[0], (x[1],x[2])))).map(lambda x: ((x[1][1][0], x[1][0]), (int(x[1][1][1]), 1)))

final_rdd = joined_rdd.reduceByKey(
    (lambda x, y: (x[0]+y[0],x[1]+y[1]))).map(lambda x: ((int(x[0][0]), x[0][1]), 1.0 * x[1][0]/x[1][1])).sortByKey()

final = final_rdd.map(lambda x: [x[0][0], x[0][1], float(x[1])]).map(lambda x: ','.join(str(y) for y in x))
final.take(5)

final.coalesce(1).saveAsTextFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/SaiSree_Kamineni_result_task1.txt")


# part - 2

ratings = sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/ratings.dat")

users = sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/users.dat")

movies = sc.textFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/movies.dat")

users_rdd = users.map(lambda x: (x.split('::')[0],x.split('::')[1]))
ratings_rdd = ratings.map(lambda x: (x.split('::')[0], x.split('::')[1], x.split('::')[2]))
movies_rdd = movies.map(lambda x: (x.split('::')[0], x.split('::')[2]))

joined_rdd = users_rdd.map(lambda x:(x[0],x[1])).join(ratings_rdd.map(lambda x:(x[0], (x[1],x[2])))).map(lambda x: (x[1][1][0], (x[1][0], x[1][1][1]))).join(movies_rdd.map(lambda x: (x[0], x[1]))).map(lambda x: (((x[1][1], x[1][0][0]), (int(x[1][0][1]), 1))))

final_rdd = joined_rdd.reduceByKey(
    (lambda x, y: (x[0]+y[0],x[1]+y[1]))).map(lambda x: ((x[0][0], x[0][1]), 1.0 * x[1][0]/x[1][1])).sortByKey()


final = final_rdd.map(lambda x: [x[0][0],x[0][1],float(x[1])]).map(lambda x: ','.join(str(y) for y in x))
final.take(5)

final.coalesce(1).saveAsTextFile("/Users/saisreekamineni/Documents/Masters/Fall_17/DM_553/assignments/HW1/ml-1m/SaiSree_Kamineni_result_task2.txt")

