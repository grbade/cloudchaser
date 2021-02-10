#!/opt/cloudchasers/cloudchasers/bin/python3
import random
import datetime

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.cluster import PasswordAuthenticator
from couchbase.cluster import QueryOptions
from couchbase.management.queries import CreatePrimaryQueryIndexOptions

######## Update this to your cluster

############## Remove me to activate again
#return
############## Remove me to activate again

endpoint = '163de767-a39d-4474-8f06-10efe90849b3.dp.cloud.couchbase.com'
username = "dbuser"
password = "cloudchasersQ!1"
bucketName = 'couchbasecloudbucket'

# Initialize the Connection
cluster = Cluster('couchbases://' + endpoint + '?ssl=no_verify', ClusterOptions(PasswordAuthenticator(username, password)))
cb = cluster.bucket(bucketName)
cb_coll = cb.default_collection()

# Create a N1QL Primary Index (but ignore if it exists)
cluster.query_indexes().create_primary_index(bucketName, ignore_if_exists=True)

cities = ["New York", "Munich", "Oslo", "Manchester", "Paris", "Stockholm", "London", "San Francisco"]

# normal distributions values
# distribution_params = [mu, sigma]
day_temp_distr = [5, 4]
night_temp_distr = [0, 4]
wind_speed_distr = [4, 2]
humidity_distr = [80, 8]
rain_distr = [0.003, 0.0005]

sunshine_prob = 0.3

for city in cities:
    humidity = -1
    wind_speed = -1
    timestamp = int(datetime.datetime.now().timestamp())

    while wind_speed < 0:
        wind_speed = random.normalvariate(wind_speed_distr[0], wind_speed_distr[1])

    while (humidity > 100 or humidity < 0):
        humidity = random.normalvariate(humidity_distr[0], humidity_distr[1])

    if (random.random() <= sunshine_prob):
        sunshine = 1
    else:
        sunshine = 0

    hour = datetime.datetime.now().hour
    if (hour > 7 or hour > 19):
        temperature = random.normalvariate(day_temp_distr[0], day_temp_distr[1])
    else:
        temperature = random.normalvariate(night_temp_distr[0], night_temp_distr[1])

    if (random.random() <= (14/30)):
        rainfall_mm_sec = random.normalvariate(rain_distr[0], rain_distr[1])
    else:
        rainfall_mm_sec = 0

    key = city + "::" + str(timestamp)
    value = {
        "location": city,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "sunshine": sunshine,
        "rainfall": rainfall_mm_sec
    }

    cb_coll.upsert(key, value)

print("Script executed at: ", datetime.datetime.now())
