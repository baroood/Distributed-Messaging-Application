import pymongo
import credentials
import datetime
from flask import Flask, request, jsonify, render_template
from uuid import uuid1, getnode
from random import getrandbits
from threading import Timer


app = Flask(__name__)

master_connection_string = "mongodb+srv://admin:admin@cluster0.nbz64.mongodb.net/cluster?retryWrites=true&w=majority"
master_Server = pymongo.MongoClient(master_connection_string)
master_db = master_Server["MasterBD"]
ConversationChunkMappingCollection = master_db["ConversationChunkMapping"]


collections = []
MaxChunkSize = 2
NumberOfReplicas = 2


@app.route('/anotherFrontend')
def anotherFrontend():
    senderID = '0'
    conversations = [{'ID': '0', 'name': 'a'}, {
        'ID': '1', 'name': 'b'}, {'ID': '2', 'name': 'c'}]
    return render_template("index.html", conversations=conversations, senderID=senderID)


@app.route('/')
def hello():
    return "Hello World!"


@app.route('/getAll', methods=['GET'])
def getAll():
    s = str(list(ConversationChunkMappingCollection.find()))
    s += "\n"
    for i in collections:
        if i == None:
            continue
        s += str(list(i.find()))
        s += "\n"
    return s


def getFreeLocations():

    dbSize = []
    for i in range(len(collections)):
        if credentials.SERVERS_LIST[i]['status'] != "available":
            continue
        dbSize.append([collections[i].count(), i])
    dbSize.sort(key=lambda x: x[0])
    return [dbSize[i][1] for i in range(min(NumberOfReplicas, len(dbSize)))]


_my_clock_seq = getrandbits(14)


def getChunkIDs(size=NumberOfReplicas):
    return[str(uuid1(clock_seq=_my_clock_seq)) for _ in range(min(NumberOfReplicas, size))]


def createNewChunk(conversationID, chunkNum):
    try:
        locations = getFreeLocations()
        chunkIDs = getChunkIDs(len(locations))
        ConversationChunkMappingCollection.insert_one({
            "conversationID": conversationID,
            "chunkNum": chunkNum,
            "size": 0,
            "locations": locations,
            "chunkIDs": chunkIDs

        })
        return ConversationChunkMappingCollection.find({"$query": {"conversationID": conversationID}, "$orderby": {"chunkNum": -1}})[0]

    except:
        return -1


def getServerLocation(conversationID):
    try:


<< << << < HEAD
        chunkInfo = ConversationChunkMappingCollection.find(
            {"$query": {"conversationID": conversationID}, "$orderby": {"chunkNum": -1}})[0]
        print(chunkInfo)
== == == =
        chunkInfo = ConversationChunkMappingCollection.find(
            {"$query": {"conversationID": conversationID}, "$orderby": {"chunkNum": -1}})[0]
>>>>>> > 898274fcb3aa5cedeca5355b16b56c32cee1a52f

        if chunkInfo["size"] >= MaxChunkSize:
            chunkInfo = createNewChunk(conversationID, chunkInfo["chunkNum"]+1)
            if chunkInfo == -1:
                return [], [], -1

        return chunkInfo["locations"], chunkInfo["chunkIDs"], chunkInfo["chunkNum"]
    except:
        return [], [], -1


@app.route('/createNewConversation', methods=['POST'])
def createNewConversation():

    locations = getFreeLocations()
    chunkIDs = getChunkIDs(len(locations))
    conversationID = str(uuid1(clock_seq=_my_clock_seq))
    ConversationChunkMappingCollection.insert_one({
        "conversationID": conversationID,
        "chunkNum": 0,
        "size": 0,
        "locations": locations,
        "chunkIDs": chunkIDs

    })
    return conversationID


@app.route('/insert', methods=['POST'])
def insert():
    senderID = request.get_json()['senderID']
    conversationID = request.get_json()['conversationID']
    message = request.get_json()['message']

    try:
        severIds, chunkIds, chunkNum = getServerLocation(conversationID)
        if chunkNum == -1:
            return jsonify({
                "status": 1,
                "message": "Unable to find Server Location"
            })
        timeStamp = datetime.datetime.now()
        messageID = str(uuid1(clock_seq=_my_clock_seq))
        for i in range(len(severIds)):
            collections[severIds[i]].insert_one({
                "messageID": messageID,
                "chunkID": chunkIds[i],
                "senderID": senderID,
                "message": message,
                "timeStamp": timeStamp
            })

        ConversationChunkMappingCollection.update_one(
            {"conversationID": conversationID, "chunkNum": chunkNum},
            {"$inc": {"size": 1}}
        )

        return jsonify({
            "status": 0,
            "message": "Success"
        })
    except:
        return jsonify({
            "status": 1,
            "message": "Something Went Wrong"
        })


def getLatestChunkNum(conversationID):
    try:
        return ConversationChunkMappingCollection.find({"$query": {"conversationID": conversationID}, "$orderby": {"chunkNum": -1}})[0]["chunkNum"]
    except:
        return -1


@app.route('/getChunk', methods=['GET'])
def getChunk():
    try:
        conversationID = request.get_json()['conversationID']
        chunkNum = request.get_json()['chunkNum']
        if chunkNum == -1:
            chunkNum = getLatestChunkNum(conversationID)

        chunkInfo = ConversationChunkMappingCollection.find(
            {"$query": {"conversationID": conversationID, "chunkNum": chunkNum}})[0]
        msglist = []

        for i in collections[chunkInfo["locations"][0]].find({"chunkID": chunkInfo["chunkIDs"][0]}):
            i["_id"] = str(i["_id"])
            msglist.append(i)

        return {
            "status": 0,
            "message": "Success",
            "msgList": msglist
        }

    except:
        return {
            "status": 1,
            "message": "Failure",
            "msgList": []
        }



@app.route('/deleteMsg', methods=['POST'])
def deleteMsg():
    try:
        conversationID = request.get_json()['conversationID']
        chunkNum = request.get_json()['chunkNum']
        messageID = request.get_json()['messageID']

        locations = ConversationChunkMappingCollection.find(
            {"$query": {"conversationID": conversationID, "chunkNum": chunkNum}})[0]["locations"]
        msglist = []

        for i in locations:
            collections[i].remove({"messageID": messageID})

        return jsonify({
            "status": 0,
            "message": "Success",
        })

    except:
        return jsonify({
            "status": 1,
            "message": "Failure",
        })

@app.route('/updateMsg', methods=['POST'])
def updateMsg():
    try:
        conversationID = request.get_json()['conversationID']
        chunkNum = request.get_json()['chunkNum']
        messageID = request.get_json()['messageID']
        message = request.get_json()['message']

        locations = ConversationChunkMappingCollection.find(
            {"$query": {"conversationID": conversationID, "chunkNum": chunkNum}})[0]["locations"]
        msglist = []

        for i in locations:
            collections[i].update_one(
                {"messageID": messageID},
                {"$set": {"message": message}}
            )

        return jsonify({
            "status": 0,
            "message": "Success",
        })

    except:
        return {
            "status" : 1,  
            "message" : "Failure",
            }


@app.route('/delete', methods=['POST'])
def delete():
    ConversationChunkMappingCollection.remove()
    for i in collections:
        if i == None:
            continue
        i.remove()
    return "Done"


def getAnotherLocation(serverIDs):
    dbSize = []
    for i in range(len(collections)):
        if i in serverIDs or credentials.SERVERS_LIST[i]['status'] != "available":
            continue
        dbSize.append([collections[i].count(), i])
    if len(dbSize) == 0:
        return -1
    dbSize.sort(key=lambda x: x[0])
    return dbSize[0][1]


def transferData(serverID, delete = False):
    try:
        documents = ConversationChunkMappingCollection.find( { "locations": {"$eq": serverID }})

        for document in documents:
            locIdx = -1
            for idx, sid in enumerate(document["locations"]):
                if sid != serverID and credentials.SERVERS_LIST[sid]['status'] == "available":
                    locIdx = sid
                    break
            if locIdx == -1:
                continue
            
            server2ID = document["locations"][locIdx]
            chunk2ID = document["chunkIDs"][locIdx]

            newServerID = getAnotherLocation(document["locations"])
            if newServerID == -1:
                continue

            messages = collections[server2ID].find( { "chunkID": chunk2ID } )

            for message in messages:
                del message["_id"]
                collections[newServerID].insert_one( message )

            ConversationChunkMappingCollection.find_and_modify(
                query = { "_id": document["_id"] },
                update = { "$set": { "locations.$[element]": newServerID } },
                arrayFilters = [ { "element": { "$eq": serverID } } ],
                new = True
            )

        if delete == True:
            collections[serverID].remove()

        return "Done"
    except:
        return "Error"


def checkClusters():
    for serverID, server_dict in enumerate(credentials.SERVERS_LIST):
        if server_dict['status'] == 'STOP' or server_dict['status'] == "PAUSE":
            continue
        try:
            server = pymongo.MongoClient(server_dict['cred'])
            server.server_info()
            if collections[serverID] == None:
                db = server["cluster"]
                collections[serverID] = db["messages"]
            if server_dict['status'] != "available":
                collections[serverID].remove()
            server_dict['status'] = "available"
        except:
            if server_dict['status'] == "available":
                server_dict['status'] = "faulty"
    
    for server_dict in credentials.SERVERS_LIST:
        print(server_dict['status'])

    for serverID, server_dict in enumerate(credentials.SERVERS_LIST):
        if server_dict['status'] == "faulty":
            server_dict['status'] = "unavailable"
            transferData(serverID)


@app.route('/pause', methods=['POST'])
def pause():
    serverID = request.get_json()['serverID']
    if collections[serverID] != None and credentials.SERVERS_LIST[serverID] != "PAUSE":
        credentials.SERVERS_LIST[serverID] = "PAUSE"
        return transferData(serverID)
    return "Done"


@app.route('/start', methods=['POST'])
def start():
    serverID = request.get_json()['serverID']
    credentials.SERVERS_LIST[serverID] = "START"
    return "Done"

if __name__ == '__main__':
    for server_dict in credentials.SERVERS_LIST:
        if server_dict['status'] == 'STOP':
            collections.append(None)
            continue
        try:
            server = pymongo.MongoClient(server_dict['cred'])
            server.server_info()
            db = server["cluster"]
            collections.append(db["messages"])
            server_dict['status'] = "available"
        except:
            collections.append(None)
            server_dict['status'] = "unavailable"

    time = 300
    timer = Timer(time, checkClusters)
    timer.start()

    app.run(debug=True)
