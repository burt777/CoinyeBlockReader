import csv
import sqlite3




# c.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='blocks'")
# tableExists = c.fetchall()[0]
# print (tableExists ? "Table exists" : "No table exists yet")

#         if not row == 0:
#             header = row
#         elif row == 1:
#             # now we know data types, so we can create a table if it doesn't exist:
#             if not tableExists:
#                 c.execute ('''CREATE TABLE IF NOT EXISTS blocks
#                 (id INTEGER PRIMARY KEY AUTOINCREMENT, userid TEXT, domain TEXT, password      TEXT, webserver TEXT, sqlserver TEXT)''')
#             conn.commit()

#             print c.execute ("SELECT seq from SQLITE_SEQUENCE WHERE name='blocks'")

#         else:



#         row++
# if row == 0: 
#     print("Error: No csv data found")

def csvToSqlite(csvFileName, sqliteFileName, tableName, primaryColumn = 0, delimiter = '\t'):
    conn = sqlite3.connect(sqliteFileName)
    c = conn.cursor()

    sqliteHeader = sqliteGetHeader(conn, "blocks")
    csvHeader = csvGetHeader(csvFileName)
    types = csvGetTypes(csvFileName)

    if not sqliteHeader:
        fieldString = ""
        for colNr, field in enumerate(csvHeader):
            fieldString += "{} {}, ".format(field, types[colNr][0])

        fieldString = fieldString.replace(',', ' PRIMARY KEY,', primaryColumn + 1)
        fieldString = fieldString.replace(' PRIMARY KEY,', ',', primaryColumn)

        query = "CREATE TABLE IF NOT EXISTS {} ({})".format(tableName, fieldString[:-2])
        print(query)
        c.execute(query)
        sqliteLastBlock = -1

    elif sqliteHeader != csvHeader:
        print("Error! Sqlite header and CSV header don't match")
        print(sqliteHeader)
        print(csvHeader)
        return False
    else:
        # table already exists and header matches:

        query = "SELECT MAX({}) FROM {}".format(sqliteHeader[0], tableName)
        c.execute(query)
        sqliteLastBlock = c.fetchall()[0][0]
        if sqliteLastBlock == None:
            sqliteLastBlock = -1 # Table fine, but no blocks at all
        print("Last block in sqlite table is {}".format(sqliteLastBlock))

    with open(csvFileName) as csvFile:
        headerString = ", ".join(csvHeader)
        valueString = ("?, " * len(csvHeader))[:-2]
        query = "INSERT INTO {}({}) VALUES ({})".format(tableName, headerString, valueString)
        nrCached = 0

        print(query)
        csvPointer = csv.reader(csvFile, delimiter = '\t')
        next(csvPointer) # Skip header
        for row in csvPointer:
            for index, field in enumerate(row):
                if types[index][0] == "INTEGER":
                    row[index] = int(row[index])
                elif types[index][0] == "FLOAT":
                    row[index] = float(row[index])

            # print(row)
            idNr = row[0]

            if idNr > sqliteLastBlock:
                c.execute(query, row)
                sqliteLastBlock = idNr

                nrCached += 1
                if nrCached > 1000:
                    conn.commit()
                    print("Cached up to block {}".format(idNr))
                    nrCached = 0

            # if idNr > 15: 
            #     break

        conn.commit()



def sqliteGetHeader(sqliteConnection, tableName):
    try:
        cursor = sqliteConnection.execute("select * from {}".format(tableName))
        header = [description[0] for description in cursor.description]
        return header
    except:
        return False

def csvGetHeader(csvFileName, delimiter = '\t'):
    try:
        with open(csvFileName) as csvFile:
            csvPointer = csv.reader(csvFile, delimiter = delimiter)
            for row in csvPointer:
                return row
        return False
    except:
        return False


def csvGetTypes(csvFile, skipLine = 1, nrOfLines = 10, delimiter = '\t'):
    types = []
    length = []
    rowNr = 0

    with open(csvFileName) as csvFile:
        csvPointer = csv.reader(csvFile, delimiter = delimiter)
        for row in csvPointer:
            if rowNr < skipLine:
                pass
            else:
                for index, field in enumerate(row):
                    if index >= len(types):
                        types.append("INTEGER")
                        length.append(0)

                    if types[index] == "TEXT":
                        # Can't get any worse
                        if length[index] < len(field):
                            length[index] = len(field)
                    elif types[index] == "INTEGER" and not field.isdigit() and isFloat(field):
                        types[index] = "FLOAT"
                    elif not isFloat(field):
                        types[index] = "TEXT"
                        length[index] = len(field)


            rowNr += 1
            if rowNr > (nrOfLines + skipLine):
                break
    returnValue = list(zip(types, length))
    return returnValue

def isFloat(inputVar):
    try: 
        x = float(inputVar)
        return True
    except ValueError:
        return False





csvFileName = "E:/temp/coinye_stats.csv"
sqliteFileName = "blockchain2.sqlite"
tableName = "blocks"

csvToSqlite(csvFileName, sqliteFileName, tableName)
