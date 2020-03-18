import websockets
import asyncio
import pyodbc

import urllib
import json
import requests
import dateutil.parser
import datetime
import nest_asyncio

from config import account_number, password, client_id
from authentication import get_access_token

nest_asyncio.apply()


def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


print("starting get userprincipals..", flush=True)

# we need to go to the User Principals endpoint to get the info we need to make a streaming request
endpoint = "https://api.tdameritrade.com/v1/userprincipals"

# get our access token
headers = get_access_token(account_number, password, client_id)
print(headers, flush=True)

# this endpoint, requires fields which are separated by ','
params = {'fields': 'streamerSubscriptionKeys,streamerConnectionInfo'}

# make a request
content = requests.get(url=endpoint, params=params, headers=headers)
userPrincipalsResponse = content.json()


print('userPrincipalsResponse:' + str(userPrincipalsResponse), flush=True)

# we need to get the timestamp in order to make our next request, but it needs to be parsed
tokenTimeStamp = userPrincipalsResponse['streamerInfo']['tokenTimestamp']
date = dateutil.parser.parse(tokenTimeStamp, ignoretz=True)
tokenTimeStampAsMs = unix_time_millis(date)

# we need to define our credentials that we will need to make our stream
credentials = {
    "userid": userPrincipalsResponse['accounts'][0]['accountId'],
    "token": userPrincipalsResponse['streamerInfo']['token'],
    "company": userPrincipalsResponse['accounts'][0]['company'],
    "segment": userPrincipalsResponse['accounts'][0]['segment'],
    "cddomain": userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
    "usergroup": userPrincipalsResponse['streamerInfo']['userGroup'],
    "accesslevel": userPrincipalsResponse['streamerInfo']['accessLevel'],
    "authorized": "Y",
    "timestamp": int(tokenTimeStampAsMs),
    "appid": userPrincipalsResponse['streamerInfo']['appId'],
    "acl": userPrincipalsResponse['streamerInfo']['acl']
}

print(userPrincipalsResponse, flush=True)

# define a request
login_request = {
  "requests": [{
    "service": "ADMIN",
    "requestid": "0",
    "command": "LOGIN",
    "account": userPrincipalsResponse['accounts'][0]['accountId'],
    "source": userPrincipalsResponse['streamerInfo']['appId'],
    "parameters": {
        "credential": urllib.parse.urlencode(credentials),
        "token": userPrincipalsResponse['streamerInfo']['token'],
        "version": "1.0"
    }}]}


# define a request for different data sources
data_request = {
  "requests": [{
    "service": "ACTIVES_NASDAQ",
    "requestid": "1",
    "command": "SUBS",
    "account": userPrincipalsResponse['accounts'][0]['accountId'],
    "source": userPrincipalsResponse['streamerInfo']['appId'],
    "parameters": {
        "keys": "NASDAQ-60",
        "fields": "0,1"
    }
  }, {
    "service": "LEVELONE_FUTURES",
    "requestid": "2",
    "command": "SUBS",
    "account": userPrincipalsResponse['accounts'][0]['accountId'],
    "source": userPrincipalsResponse['streamerInfo']['appId'],
    "parameters": {
        "keys": "/ES",
        "fields": "0,1,2,3,4"
    }}]}


# create it into a JSON string, as the API expects a JSON string.
login_encoded = json.dumps(login_request)
data_encoded = json.dumps(data_request)

print('login_encoded', login_encoded, flush=True)


class WebSocketClient():

    def __init__(self):
        self.data_holder = []
        # self.file = open('td_ameritrade_data.txt', 'a')
        self.cnxn = None
        self.crsr = None

    def database_connect(self):

        # define the server and the database, YOU WILL NEED TO CHANGE THIS TO YOUR OWN DATABASE AND SERVER
        server = 'YOUR_SERVER' 
        database = 'YOUR_DATABASE'  
        sql_driver = '{ODBC Driver 17 for SQL Server}'

        # define our connection, autocommit MUST BE SET TO TRUE, also we can edit data.
        self.cnxn = pyodbc.connect(driver = sql_driver, 
                                   server = server, 
                                   database = database, 
                                   trusted_connection ='yes')

        self.crsr = self.cnxn.cursor()

    def database_insert(self, query, data_tuple):   

        # execute the query, commit the changes, and close the connection
        self.crsr.execute(query, data_tuple)
        self.cnxn.commit()
        self.cnxn.close()

        print('Data has been successfully inserted into the database.', flush=True)

    async def connect(self):
        '''
            Connecting to webSocket server
            websockets.client.connect returns a WebSocketClientProtocol, which is used to send and receive messages
        '''

        # define the URI of the data stream, and connect to it.
        uri = "wss://" + userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws"
        print('Connecting to websocket server: ' + uri, flush=True)
        self.connection = await websockets.client.connect(uri)

        print('connection opened?', flush=True)
        print(self.connection, flush=True)

        # if all goes well, let the user know.
        if self.connection.open:
            print('Connection established. Client correctly connected', flush=True)
            return self.connection
        else:
            print('Encountered error', flush=True)

    async def sendMessage(self, message):
        '''
            Sending message to webSocket server
        '''
        await self.connection.send(message)

    async def receiveMessage(self, connection):
        print('Receiving message...')
        '''
            Receiving all server messages and handle them
        '''
        while True:
            try:

                # grab and decode the message
                message = await connection.recv()
                print('message decoded: ' + str(message), flush=True)
                message_decoded = json.loads(message)
                print('message decoded: ' + str(message_decoded), flush=True)

                # # prepare data for insertion, connect to database
                # query = "INSERT INTO td_service_data (service, timestamp, command) VALUES (?,?,?);"
                # self.database_connect()

                # # check if the response contains a key called data if so then it contains the info we want to insert.
                # if 'data' in message_decoded.keys():

                #     # grab the data
                #     data = message_decoded['data'][0]
                #     data_tuple = (data['service'], str(data['timestamp']), data['command'])

                #     # insert the data
                #     self.database_insert(query, data_tuple)

                print('-'*20, flush=True)
                print('Received message from server: ' + str(message), flush=True)

            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed', flush=True)
                break

    async def heartbeat(self, connection):
        '''
            Sending heartbeat to server every 5 seconds
            Ping - pong messages to verify connection is alive
        '''
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed', flush=True)
                break


async def main():
    # Creating client object
    client = WebSocketClient()

    loop = asyncio.get_event_loop()

    # # Start connection and get client connection protocol
    # connection = await asyncio.wait_for(client.connect(), timeout=60.0)
    connection = loop.run_until_complete(client.connect())

    # # Start listener and heartbeat
    tasks = [
             # asyncio.ensure_future(client.receiveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(login_encoded)),
             # asyncio.ensure_future(client.receiveMessage(connection)),
             # asyncio.ensure_future(client.sendMessage(data_encoded)),
             # asyncio.ensure_future(client.receiveMessage(connection))
             ]

    loop.run_until_complete(asyncio.wait(tasks))


if __name__ == '__main__':
    asyncio.run(main())
