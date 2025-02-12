from fastapi.testclient import TestClient


class TestS1Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_first(self, client: TestClient) -> None:
        # Implement tests if you want
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert True
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert response.status_code == 200, "Prepare endpoint failed"
        with client as client:
            response = client.get("/api/s1/aircraft?num_results=5&page=1")
            assert not response.is_error, "Error at the aircraft endpoint with pagination"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) <= 5, "Pagination did not work correctly"
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=10")
            assert response.status_code == 200, "Failed to download with file_limit=10"

    def test_download_large_file_limit(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=100")
            assert response.status_code == 200, "Failed to download with large file_limit=100"

    def test_aircraft_no_duplicates(self, client: TestClient) -> None:
        with client as client:
            page = 0
            results = []
            while True:
                response = client.get(f"/api/s1/aircraft/?num_results=500&page={page}")
                assert response.status_code == 200, "Failed to fetch aircraft data"
                data = response.json()
                if not data:
                    break
                results.extend(a["icao"] for a in data)
                page += 1
            assert len(results) == len(set(results)), "Duplicate aircraft detected"

    def test_positions_ordered(self, client: TestClient) -> None:
        test_icao = "a972d3"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{test_icao}/positions?num_results=1000&page=0")
            assert response.status_code == 200, "Failed to fetch positions data"
            positions = response.json()
            timestamps = [pos["timestamp"] for pos in positions]
            assert timestamps == sorted(timestamps), "Positions are not ordered by timestamp"

    def test_non_existent_position(self, client: TestClient) -> None:
        test_icao = "invalid123"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{test_icao}/positions")
            assert response.status_code == 404, "Expected 404 for non-existent position"

    def test_non_existent_stats(self, client: TestClient) -> None:
        test_icao = "invalid123"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{test_icao}/stats")
            assert response.status_code == 404, "Expected 404 for non-existent position"

    def test_aircraft_pagination(self, client: TestClient) -> None:
        with client as client:
            for num_results in [20, 100]:
                response = client.get(f"/api/s1/aircraft?num_results={num_results}&page=1")
                assert response.status_code == 200, f"Failed pagination with num_results={num_results}"
                data = response.json()
                assert len(data) <= num_results, "Pagination limit exceeded"

    def test_positions_ordered_asc(self, client: TestClient) -> None:
        icao = "a972d3"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page=0")
            assert response.status_code == 200, "Failed to retrieve positions"
            positions = response.json()
            timestamps = [p["timestamp"] for p in positions]
            assert timestamps == sorted(timestamps), "Positions are not sorted in ascending order"

class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_download(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the download endpoint"

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
