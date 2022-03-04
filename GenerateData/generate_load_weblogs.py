
from datetime import datetime
from faker import Faker
import numpy
import random
import pandas as pd
import sqlalchemy as db


numLines = 10000
liWeblog = []

for i in range(numLines):
    faker = Faker()
    ipAddress = faker.ipv4()
    username = faker.email()
    requestTime = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
    requestLine = numpy.random.choice(
        ["GET", "POST", "DELETE", "PUT"], p=[0.6, 0.1, 0.1, 0.2])
    statusCode = numpy.random.choice(
        ["200", "404", "500", "301"], p=[0.9, 0.04, 0.02, 0.04])
    numByte = str(int(random.gauss(5000, 50)))
    referer = faker.uri()
    userAgent = numpy.random.choice([faker.firefox, faker.chrome, faker.safari,
                                    faker.internet_explorer, faker.opera], p=[0.4, 0.4, 0.1, 0.05, 0.05])()

    weblog = str(ipAddress + ' - ' + username +
                 ' [' + requestTime + '] "' + requestLine +
                 ' / HTTP/1.0" ' + statusCode + ' ' + numByte +
                 ' "' + referer + '" "' + userAgent + '"')
    liWeblog.append(weblog)

# print(liWeblog)

df = pd.DataFrame(liWeblog, columns=['logs'])

engineWebServer = db.create_engine(
    'mysql+mysqlconnector://airflow:airflow@localhost:3306/WebServer', echo=False)
df.to_sql(con=engineWebServer, name='weblogs',
          if_exists='append', index=False, chunksize=1000, method='multi')
