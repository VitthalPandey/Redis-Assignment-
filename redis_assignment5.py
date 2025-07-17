import redis
import csv
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

class Redis_Client:
    def __init__(self):
        self.redis = None

    def connect(self):
        try:
            self.redis = redis.Redis(host='localhost', 
                                    port=6379, 
                                    decode_responses=True)
            self.redis.flushdb()
            print("Connected to Redis.")
        except Exception as e:
            print("Failed to Connect:", e)

    def load_users(self, file="users.txt"):
        count=0
        try:
            with open(file, 'r', encoding='utf-8') as f:
                for line in f:
                    tokens = line.strip().split('" "')
                    tokens = [token.replace('"', '') for token in tokens]
                    key = tokens[0]
                    fields = dict(zip(tokens[1::2], tokens[2::2]))
                    self.redis.hset(key, mapping=fields)
                    count+=1


            self.redis.ft("idx:user").create_index(
                fields=[
                    TextField("first_name"),
                    TextField("last_name"),
                    TextField("gender"),
                    TagField("country"),
                    NumericField("latitude"),
                    NumericField("longitude"),
                ],
                definition=IndexDefinition(prefix=["user:"], index_type=IndexType.HASH)
            )
            print(f"Index created. Loaded {count} users into Redis")
        except Exception as e:
            print("Index already exists or error:", e)

    def load_scores(self, file="userscores.csv"):
        count=0
        with open(file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                leaderboard = f"leaderboard:{row['leaderboard']}"
                self.redis.zadd(leaderboard, {row['user:id']: int(row['score'])})
                count+=1
        print(f"Scores loaded. Loaded scores for {count} users.")

    def query1(self, usr_id):
        print("#####################")
        print("$$$$ Answer Query 1 $$$$ ")
        try:
            user_data= self.redis.hgetall(f"user:{usr_id}")
            if user_data:
                print(f"User {usr_id} data:")
                for field,value in user_data.items():
                    print(f"{field}: {value}")
            else:
                print(f"No user found with ID: {usr_id}")
        except Exception as e:
            print("Error in query1: ",e)
        return user_data

    def query2(self, usr_id):
        print("#####################")
        print("$$$$ Answer Query 2 $$$$ ")
        try: 
            user_data = self.redis.hgetall(f"user:{usr_id}")
            if not user_data:
                print(f" No user found with ID: {usr_id}")
                return
            longitude= user_data.get("longitude")
            latitude= user_data.get("latitude")
            if longitude and latitude:
                print(f"Coordinated of user {usr_id}:")
                print(f"Logitude: {longitude}")
                print(f"Latitude: {latitude}")
        except Exception as e:
            print ("Error in query 2: ",e)
        return longitude, latitude

    def query3(self):
        print("#####################")
        print("$$$$ Answer Query 3 $$$$ ")
        cursor = 1280
        usrids, lastnames = [], []
        try:
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=10)
                for key in keys:
                    uid = key.split(":")[1]
                    if uid and int(uid[0]) % 2 == 0:
                        usrids.append(key)
                        last_name=self.redis.hget(key, "last_name")
                        lastnames.append(last_name)
                        print(f"{key} : {last_name}")
                if cursor == 0:
                    break
        except Exception as e:
            print ("Error in Query 3", e)
    
        return usrids, lastnames

    def query4(self):
        print("#####################")
        print("$$$$Answer Query 4$$$$ ")
        result = []
        for key in self.redis.scan_iter("user:*"):
            user = self.redis.hgetall(key)
            try:
                gender = user.get("gender")
                country = user.get("country")
                latitude = float(user.get("latitude", 0))

                if (
                    gender == "female"
                    and country in ["China", "Russia"]
                    and 40 <= latitude <= 46
                ):
                    result.append(user)
                    print(f"{user.get('first_name', '')} ({country}) , latitude: {latitude}")
            except:
                continue
        return result


    def query5(self):
        results=[]
        print("#####################")
        print("$$$$ Answer Query 5 $$$$ ")
        top_users = self.redis.zrevrange("leaderboard:2", 0, 9)
        for user_id in top_users:
            email_id=self.redis.hget(user_id, "email") 
            if email_id:
                results.append(email_id)
                print(f"{email_id}")
            else:
                print(f"No email found for {user_id}")

        return results

if __name__ == "__main__":
    r = Redis_Client()
    r.connect()
    r.load_users("users.txt")
    r.load_scores("userscores.csv")

    r.query1("590")
    r.query2("1604")
    r.query3()
    r.query4()
    r.query5()
