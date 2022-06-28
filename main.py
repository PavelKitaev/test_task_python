# pip install fastapi
# pip install uvicorn
# pip install python-multipart
# pip install minio
# pip install psycopg2

from psycopg2 import Error
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from typing import List
from minio import Minio
import datetime
import psycopg2
import uuid
import json
import unittest

client = None
con = None
app = FastAPI()


def get_actual_code():
    global con
    temp_code = 0
    cur = con.cursor()
    cur.execute("SELECT MAX(code) FROM inbox")
    result_last_record = cur.fetchall()

    if len(result_last_record) != 0:
        if result_last_record[0][0] == None:
            temp_code = 100
        else:
            temp_code = result_last_record[0][0]
    cur.close()
    return temp_code


def start():
    global code
    global con
    global client

    # Connecting to min.io.
    try:
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
    except (Exception, Error) as error:
        print("Problem connecting to min.io.", error)

    # Database connection
    try:
        con = psycopg2.connect(
            database="task1",
            user="postgres",
            password="1",
            host="127.0.0.1",
            port="5432"
        )
    except (Exception, Error) as error:
        print("Database connection problem.", error)

    cur = con.cursor()

    # Checking if a table exists
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'inbox')")
    result_table_existence = cur.fetchall()

    if (result_table_existence[0][0] == True):
        print("f_start-- Table exists")
    else:
        print("f_start-- Table does not exist")
        cur.execute(
            '''CREATE TABLE inbox 
            (code INT, 
            bucket varchar(255) NOT NULL, 
            file_name varchar(255) NOT NULL PRIMARY KEY, 
            dat TIMESTAMP DEFAULT NOW())'''
        )
    con.commit()
    cur.close()

    # Check if the bucket exists
    found = client.bucket_exists(datetime.datetime.now().strftime("%Y%m%d"))
    if not found:
        client.make_bucket(datetime.datetime.now().strftime("%Y%m%d"))
    else:
        print("f_start-- Backet", datetime.datetime.now().strftime("%Y%m%d"), "exists")


start()


@app.post("/frames/")
async def upload_files(files: List[UploadFile] = File()):
    cur = con.cursor()
    code = get_actual_code() + 10
    data_set = []

    # File Processing
    i = 0
    for temp_file in files:
        if temp_file.content_type == 'image/jpeg' and i < 15:
            temp_uuid = uuid.uuid4()
            temp_file_name = str(temp_uuid) + ".jpg"
            cur.execute("SELECT * FROM inbox WHERE file_name = '" + temp_file_name + "'")
            result_uuid = cur.fetchall()

            # Replacing the UUID, if such already exists
            while len(result_uuid) != 0:
                temp_uuid = uuid.uuid4()
                temp_file_name = str(temp_uuid) + ".jpg"
                result_uuid = cur.execute("SELECT * FROM inbox WHERE file_name = '" + temp_file_name + "'")

            dat = datetime.datetime.now().strftime("%Y%m%d")
            client.fput_object(dat, temp_file_name, temp_file.file.fileno())
            cur.execute("INSERT INTO inbox (code, bucket, file_name) VALUES ('" + str(
                code) + "', '" + dat + "', '" + temp_file_name + "')")
            data_set.append({"Added": temp_file.filename})
            i += 1
        else:
            print("f_upload_files-- The resulting file does not match the contents of the image/jpeg, "
                  "or the limit has been reached.")
            data_set.append({"Ignored": temp_file.filename})
    con.commit()
    cur.close()

    return {"CODE:" + str(code) + json.dumps(data_set)}


@app.get("/frames/{pk}")
def get_files(pk: int):
    cur = con.cursor()
    cur.execute("SELECT * FROM inbox WHERE code = '" + str(pk) + "'")
    result = cur.fetchall()

    # Generation JSON
    data_set = []
    for row in result:
        data_set.append({"Bucket": row[1], "File name": row[2], "Date": str(row[3])})
    # print(json.dumps(data_set, indent=4))

    cur.close()
    return {json.dumps(data_set)}


@app.delete("/frames/{pk}")
def delete_files(pk: int):
    cur = con.cursor()
    print("sfssdff")
    cur.execute("SELECT * FROM inbox WHERE code = '" + str(pk) + "'")
    result = cur.fetchall()
    for row in result:
        print("f_delete_files-- Deleting file from UUID", row[2])
        client.remove_object(row[1], row[2])

    cur.execute("DELETE FROM inbox WHERE code = '" + str(pk) + "'")
    con.commit()
    cur.close()
    return {row[2] for row in result}


@app.get("/")
async def main():
    content = """
<body>
<form action="/frames/" enctype="multipart/form-data" method="post">
<input name="files" type="file" accept=".jpg" multiple>
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)


class TestInbox(unittest.TestCase):

    def test1(self):
        client_web = TestClient(FastAPI())


if __name__ == "__main__":
    unittest.main()
