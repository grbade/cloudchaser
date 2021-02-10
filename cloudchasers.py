from flask import Flask
from flask import render_template
from flask import send_from_directory
import datetime
import random
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.cluster import PasswordAuthenticator
from couchbase.cluster import QueryOptions
from couchbase.exceptions import CouchbaseException
from couchbase.cluster import AnalyticsOptions
from couchbase.management.queries import CreatePrimaryQueryIndexOptions


endpoint = '163de767-a39d-4474-8f06-10efe90849b3.dp.cloud.couchbase.com'
username = "dbuser"
password = "cloudchasersQ!1"
bucketName = 'couchbasecloudbucket'

# Initialize the Connection
cluster = Cluster('couchbases://' + endpoint + '?ssl=no_verify', ClusterOptions(PasswordAuthenticator(username, password)))
cb = cluster.bucket(bucketName)
cb_coll = cb.default_collection()

app = Flask(__name__)

#cties = ['Munich', 'Manchester', 'Stockholm', 'Paris', 'Oslo', 'London']

query = "select location from couchbasecloudbucket group by location;"
cties = []
for row in cluster.query(query):
    cties.append(row.get('location', ''))


@app.route("/")
def index():
    query = "select location from couchbasecloudbucket group by location;"
    cties = []
    for row in cluster.query(query):
        cties.append(row.get('location', ''))
        cities = []

    for city in cties:
        temperature = []
        humidity = []
        wind_speed = []
        rainfall = []
        sunshine = []

        query = "select lw.temperature, lw.humidity, lw.wind_speed, lw.sunshine, lw.rainfall, lw.timestamp from couchbasecloudbucket lw where lw.location = '{city}' and lw.timestamp > {timestamp} and type is missing order by lw.timestamp asc;"
        for row in cluster.query(query.format(**{'city': city, 'timestamp': (int(datetime.datetime.now().timestamp()) - 3600 * 4)})):

            temperature.append([row.get('timestamp',0) * 1000, row.get('temperature', 0)])
            humidity.append([row.get('timestamp',0) * 1000, row.get('humidity', 0)])
            wind_speed.append([row.get('timestamp',0) * 1000, row.get('wind_speed', 0)])
            sunshine.append([row.get('timestamp',0) * 1000, int(bool(row.get('sunshine', False)) == True)])
            rainfall.append([row.get('timestamp',0) * 1000, row.get('rainfall', 0)])

        cities.append({'name': city,
            'temperature': temperature,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'rainfall': rainfall,
            'sunshine': sunshine
            })
    return render_template('dashboard.html', cities=cities)

@app.route("/analytics")
def analytics():
    analytics = {}

    ct = 1
    query = "select lw.location, sum(lw.rainfall) as rainfall_total from cloudchasers lw where lw.timestamp > %s group by lw.location order by rainfall_total desc;" % (int(datetime.datetime.now().timestamp()) - 604800)
    rainfall_total = []
    res = cluster.analytics_query(query)
    for row in res:
        row['counter'] = ct
        row['rainfall_total'] = format(row.get('rainfall_total', 0) * 60, '.2f')
        ct+=1
        rainfall_total.append(row)
    analytics['rainfall_total'] = rainfall_total

    ct = 1 
    query = "select lw.location, sum(lw.sunshine) as sunshine_total from cloudchasers lw where lw.timestamp > %s group by lw.location order by sunshine_total desc;" % (int(datetime.datetime.now().timestamp()) - 604800)
    sunshine_total = []
    res = cluster.analytics_query(query)
    for row in res:
        row['counter'] = ct
        row['sunshine_total'] = format(row.get('sunshine_total', 0) / 60, '.1f')
        ct+=1
        sunshine_total.append(row)
    analytics['sunshine_total'] = sunshine_total

    return render_template('analytics.html', analytics=analytics)

@app.route("/reports")
def reports():
    reports = {}

    query = "select temperature, location, timestamp from couchbasecloudbucket where type is missing order by temperature asc limit 10;"
    res = cluster.query(query)
    res_transf = []
    ct = 1
    for row in res:
        row['counter'] = ct
        ct+=1
        row['timestamp'] = datetime.datetime.utcfromtimestamp(row.get('timestamp', 0)).isoformat()
        row['temperature'] = format(row.get('temperature', 0), '.2f')
        res_transf.append(row)
    reports['temperature_low'] = res_transf

    query = "select temperature, location, timestamp from couchbasecloudbucket where type is missing order by temperature desc limit 10;"
    res = cluster.query(query)
    res_transf = []
    ct = 1
    for row in res:
        row['counter'] = ct
        ct+=1
        row['temperature'] = format(row.get('temperature', 0), '.2f')
        row['timestamp'] = datetime.datetime.utcfromtimestamp(row.get('timestamp', 0)).isoformat()
        res_transf.append(row)
    reports['temperature_high'] = res_transf

    query = "select humidity, location, timestamp from couchbasecloudbucket where type is missing order by humidity asc limit 10;"
    res = cluster.query(query)
    res_transf = []
    ct = 1
    for row in res:
        row['counter'] = ct
        ct+=1
        row['humidity'] = format(row.get('humidity', 0), '.2f')
        row['timestamp'] = datetime.datetime.utcfromtimestamp(row.get('timestamp', 0)).isoformat()
        res_transf.append(row)
    reports['humidity_low'] = res_transf

    query = "select humidity, location, timestamp from couchbasecloudbucket where type is missing order by humidity desc limit 10;"
    res = cluster.query(query)
    res_transf = []
    ct = 1
    for row in res:
        row['counter'] = ct
        ct+=1
        row['humidity'] = format(row.get('humidity', 0), '.2f')
        row['timestamp'] = datetime.datetime.utcfromtimestamp(row.get('timestamp', 0)).isoformat()
        res_transf.append(row)
    reports['humidity_high'] = res_transf

    return render_template('reports.html', reports=reports)

@app.route("/alarms")
def alarms():
    query = "select location from couchbasecloudbucket group by location;"
    cties = []
    for row in cluster.query(query):
        cties.append(row.get('location', ''))

    # normal distributions values
    # distribution_params = [mu, sigma]
    day_temp_distr = [5, 4]
    night_temp_distr = [0, 4]
    wind_speed_distr = [4, 2]
    humidity_distr = [80, 8]
    rain_distr = [0.003, 0.0005]

    sunshine_prob = 0.3

    for city in cties:
        humidity = -1
        wind_speed = -1
        timestamp = int(datetime.datetime.now().timestamp())

        rnd = random.random()
        if rnd > 0.6:
            wind_speed = rnd * 50
        else:
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

    alarms = {}

    query = "select location, status, wind_speed from couchbasecloudbucket where type = 'WindAlert';"
    res = cluster.query(query)
    res_transf = []
    for row in res:
        status = row.get('status', '')
        if status == 'No storm':
            status = '<i style="color:MediumSeaGreen;font-size:20px" class="bi bi-emoji-sunglasses"></i>'
        elif status == 'Violent storm':
            status = '<i style="color:Tomato;font-size:20px" class="bi bi-emoji-dizzy"></i>'
        elif status == 'Storm':
            status = '<i style="color:Orange;font-size:20px" class="bi bi-emoji-frown"></i>'
        elif status == 'Hurricane':
            status = '<i style="color:Violet;font-size:20px" class="bi bi-emoji-dizzy-fill"></i>'
        row['status_icon'] = status
        row['wind_speed'] = format(row.get('wind_speed', 0), '.2f')
        res_transf.append(row)
    alarms['alarms'] = res_transf
    return render_template('alarms.html', alarms=alarms)


@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('templates/js', path)

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('templates/css', path)

@app.route('/assets/<path:path>')
def send_assets(path):
    return send_from_directory('templates/assets', path)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
    

