import json
import os
import subprocess
import requests
import traceback

startBlock = 1 
rpcServer = "localhost"
rpcPort = 22555

rpcUser = "yourUsername"
rpcPassword = "yourPassword"

transactionFile = "transactions.json"
transactionsCsvFile = "transactions.csv"

class CryptoDaemon:
    def __init__(self, server, user, password, port):
        self.server = server
        self.user = user
        self.password = password
        self.port = port

        self.lastRetarget = None
        self.currentBlockNr = False

        self.url = "http://{}:{}@{}:{}".format(self.user, self.password, self.server, self.port)
        self.headers = {'content-type': 'application/json'}

    def method(self, command, params):
        payload = json.dumps({"method": command, "params": params, "jsonrpc": "2.0"})

        try:        
            responseRaw = requests.get(self.url, headers = self.headers, data = payload)
        except:
            print("RCP Not responding...", end = "")
            return False

        response = responseRaw.json()
        
        if response['error'] is None and response['result'] is not None:
            return response['result']
        else:
            print("RPC Error: ")
            print(response)
            return False

    def getBlockCount(self):
        self.currentBlockNr = self.method("getblockcount", None)
        return self.currentBlockNr
    
    def getBlockHash(self, blockHeight):
        return self.method("getblockhash", [blockHeight])


    def getBlockByHash(self, blockHash):
        return self.method("getblock", [blockHash])

    def getBlockByHeight(self, blockHeight):
        return self.getBlockByHash(self.getBlockHash(blockHeight))

    def getBlock(self, block):
        return getBlockByHash(block) if isinstance(block, str) else self.getBlockByHeight(block)


def str_to_file(input, filename, append = True):
    mode = 'a' if append else 'w'
    try:
        with open(filename, mode) as f:
            f.write(input)
    except Exception as e:
        print("\nWarning: File writing error writing to file {} (mode '{}\n{}')."\
            .format(filename, mode, (input[:80] + '...') if len(input) > 80 else input))
        print(e)
        sys.exit()


def save_json(jsonObject, filename):
    with open(filename, 'w') as f:
        json.dump(jsonObject, f, indent = 4, sort_keys = True)

def load_json(inputFilename):
    try:
        with open(inputFilename, 'r') as f:
            jsonObject = json.load(f)
        print("loaded {} items from json file".format(len(jsonObject)))
        return jsonObject

    except Exception as e:
        # errorString = traceback.format_exc()
        # print(errorString)
        # print("New Json object (no file found at {})".format(inputFilename))
        return {}



coinye = CryptoDaemon(rpcServer, rpcUser, rpcPassword, rpcPort)

lastBlock = coinye.getBlockCount()

block = load_json(transactionFile)
txSerialNr = 0

for blockNr in range(startBlock, lastBlock + 1):
    block[blockNr] = coinye.getBlockByHeight(blockNr)

    transactionHashes = block[blockNr]["tx"]
    newTxList = {}

    for txNrInBlock, txHash in enumerate(transactionHashes):
        coinbase = "cb" if txNrInBlock == 0 else "n" # Coinbase / normal
        raw = coinye.method("getrawtransaction", [txHash])
        decode = coinye.method("decoderawtransaction", [raw])
        decode["raw"] = raw

        receiveAddr = []
        for v in decode["vout"]:
            for a in v["scriptPubKey"]["addresses"]:
                receiveAddr.append(a)

        newTxList[txHash] = decode

        csvData = "\t".join(map(str, [
            txSerialNr,
            blockNr,
            block[blockNr]["time"],
            coinbase,
            txHash,
            raw, 
            len(decode["vin"]),
            len(decode["vout"]),
            len(receiveAddr),
            receiveAddr

        ]))
        str_to_file(csvData + "\n", transactionsCsvFile)

        txSerialNr += 1

    block[blockNr]["tx"]= newTxList

    if (blockNr % 10 == 0):
        print("\rreading block {} from daemon  ".format(blockNr))

save_json(block, transactionFile)