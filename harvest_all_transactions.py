import json
import os
import subprocess
import requests
import traceback
import time

startBlock = 1 # This is only used when there is no tsv file; otherwise, it'll try and resume.
               # also: Blockchain starts at 1. 

lastBlock = False # False for all the way to the newest and hippest block the Daemon knows

rpcServer = "localhost"
rpcPort = 22555

rpcUser = "coinye"
rpcPassword = "coinye"

transactionsTsvFile = "transactions.tsv"

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


    def tryMethod(self, command, params, retries = 8):
        while retries:
            output = self.method(command, params)
            if output is False:
                time.sleep(16 - (2 *retries))
                retries -= 1
            else:
                return output
        print("Out of retries for command {} ({}). ".format(command, params))    

        # Do you want to 
        exit()
        # or
        # return False


    def method(self, command, params):
        payload = json.dumps({"method": command, "params": params, "jsonrpc": "2.0"})

        try:        
            responseRaw = requests.get(self.url, headers = self.headers, data = payload)
        except:
            print("RCP Not responding... ")
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
        return {}

def tail(filename, nrOfLines):
    fileSize = os.path.getsize(filename)
    readSize, lines = 40 * (nrOfLines + 1), []
    with open(filename, 'r') as f:
        while len(lines) <= nrOfLines:
            try:
                f.seek(fileSize - readSize, 0)
            except IOError:
                f.seek(0)
                break
            finally:
                lines = list(f)
            readSize += 40 * nrOfLines
        # print("Took {} to get {} line from {}".format(readSize, nrOfLines, filename))
    return lines[-nrOfLines:]



if __name__ == "__main__":

    coinye = CryptoDaemon(rpcServer, rpcUser, rpcPassword, rpcPort)
    
    if lastBlock is False:
        lastBlock = coinye.getBlockCount()

    if os.path.isfile(transactionsTsvFile):
        lastLine = tail(transactionsTsvFile,  1)[0].strip()
        startBlock = int(lastLine.split('\t')[1])
        txSerialNr = int(lastLine.split('\t')[0])

        print("getting last block from {} as a starting point: {}, txNr = {}".format(transactionsTsvFile, startBlock, txSerialNr))
    else:
        txSerialNr = 0
        lastLine = False
        str_to_file("txSerialNr\tblockNr\tblockTime\tcoinbase\ttxHash\traw\tvin\tvout\tnrOfRecipients\trecipients\n", transactionsTsvFile)

    # block = load_json(transactionFile)
   
    for blockNr in range(startBlock, lastBlock + 1):
        block = {}
        
        block[blockNr] = coinye.getBlockByHeight(blockNr)

        transactionHashes = block[blockNr]["tx"]
        newTxList = {}

        for txNrInBlock, txHash in enumerate(transactionHashes):
            coinbase = "cb" if txNrInBlock == 0 else "n" # Coinbase / normal

            raw = coinye.tryMethod("getrawtransaction", [txHash])
            decode = coinye.tryMethod("decoderawtransaction", [raw])

            decode["raw"] = raw

            try:
                receiveAddr = []
                for v in decode["vout"]:
                    if "addresses" in v["scriptPubKey"]:
                        for a in v["scriptPubKey"]["addresses"]:
                            receiveAddr.append(a)
                    else:
                        print("Output {} of transaction {} doesn't have a receive address... not sure if this is weird".format(v["n"], txHash))
            except:
                print("Problem interpreting block {}\ntx: {}\n\n".format(blockNr, txHash))
                print(json.dumps(decode, indent = 4))
                exit()

            newTxList[txHash] = decode

            tsvData = "\t".join(map(str, [
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

            # Wait for the transaction to look exactly like the last transaction and then resume writing.
            if lastLine:
                if lastLine == tsvData:
                    lastLine = False
                    txSerialNr += 1
            else:
                str_to_file(tsvData + "\n", transactionsTsvFile)
                txSerialNr += 1

        block[blockNr]["tx"] = newTxList

        if (blockNr % 10 == 0):
            print("reading block {} from daemon  ".format(blockNr))

        if lastLine: 
            print("Something went wrong resuming. Last line from file:")
            
            print("[[[{}]]] {}".format(lastLine, len(lastLine)))
            print("TSV Line:")
            print("[[[{}]]] {}".format(tsvData, len(tsvData)))
            print("Check the last line of {} and try deleting it.".format(transactionsTsvFile))
            exit()

