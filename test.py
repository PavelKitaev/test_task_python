from fastapi.testclient import TestClient
from main import app, get_actual_code

client_test = TestClient(app)


def test_main():
    response = client_test.get("/")
    assert response.status_code == 200


def test_upload_files():
    response = client_test.post("/frames/", files={"files": ("new3.jpg", open("new3.jpg", "rb"), "image/jpeg")})
    assert response.json() == ['[{"Added": "new3.jpg"}]']


def test_get_file():
    code = get_actual_code()
    response = client_test.get("/frames/" + str(code))
    assert response.status_code == 200


def test_delete_file():
    code = get_actual_code()
    response = client_test.delete("/frames/" + str(code))
    assert response.status_code == 200