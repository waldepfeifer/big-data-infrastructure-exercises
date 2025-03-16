import pytest
from fastapi.testclient import TestClient


def sample_icao(client: TestClient) -> str:
    """Get a valid ICAO from the database to use in tests."""
    response = client.get("/api/s7/aircraft?num_results=1")
    if response.status_code == 200 and response.json():
        return response.json()[0]["icao"]
    # Fallback to a known common ICAO if none found
    return "a65800"


class TestS7Student:
    """
    Tests for the s7 module implementation that uses RDS for data storage.
    """

    def test_prepare_data(self, client: TestClient) -> None:
        """Test the prepare endpoint that loads S3 data into RDS."""
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert response.status_code == 200, "Failed to prepare aircraft data"
            assert response.text == '"OK"', "Unexpected response from prepare endpoint"

    def test_list_aircraft(self, client: TestClient) -> None:
        """Test listing aircraft with pagination."""
        with client as client:
            response = client.get("/api/s7/aircraft?num_results=10&page=0")
            assert response.status_code == 200, "Failed to list aircraft"
            data = response.json()
            assert isinstance(data, list), "Result is not a list"
            # Only check if we have data, as the exact count depends on the DB state
            if data:
                for aircraft in data:
                    assert "icao" in aircraft, "Missing 'icao' field"
                    assert "registration" in aircraft, "Missing 'registration' field"
                    assert "type" in aircraft, "Missing 'type' field"

    @pytest.mark.parametrize("num_results,page", [
        (5, 0),   # Small page, first page
        (20, 0),  # Medium page, first page
        (100, 0), # Large page, first page
        (5, 1),   # Small page, second page
        (20, 1),  # Medium page, second page
        (100, 1), # Large page, second page
    ])
    def test_aircraft_pagination_parameterized(self, client: TestClient, num_results: int, page: int) -> None:
        """Test pagination with various page sizes and page numbers."""
        with client as client:
            response = client.get(f"/api/s7/aircraft?num_results={num_results}&page={page}")
            assert response.status_code == 200, f"Failed pagination with num_results={num_results}, page={page}"
            data = response.json()
            assert isinstance(data, list), "Result is not a list"
            assert len(data) <= num_results, f"Page size exceeded: {len(data)} > {num_results}"

    def test_aircraft_pagination(self, client: TestClient) -> None:
        """Test that pagination works correctly."""
        with client as client:
            # Test with different page sizes
            for num_results in [5, 20]:
                response = client.get(f"/api/s7/aircraft?num_results={num_results}&page=0")
                assert response.status_code == 200, f"Failed pagination with num_results={num_results}"
                data = response.json()
                assert len(data) <= num_results, f"Page size exceeded: {len(data)} > {num_results}"

            # Test successive pages
            page1 = client.get("/api/s7/aircraft?num_results=5&page=0").json()
            page2 = client.get("/api/s7/aircraft?num_results=5&page=1").json()

            # If both pages have data, they should be different
            if page1 and page2:
                assert page1 != page2, "Page 0 and Page 1 returned identical results"

    def test_aircraft_pagination_edge_cases(self, client: TestClient) -> None:
        """Test edge cases for pagination parameters."""
        with client as client:
            # Test zero page size
            response = client.get("/api/s7/aircraft?num_results=0&page=0")
            assert response.status_code == 200, "Failed with zero page size"

            # Test negative page (should be treated as 0)
            response = client.get("/api/s7/aircraft?num_results=10&page=-1")
            assert response.status_code == 200, "Failed with negative page number"

            # Test very large page size
            response = client.get("/api/s7/aircraft?num_results=1000&page=0")
            assert response.status_code == 200, "Failed with very large page size"

            # Test very large page number
            response = client.get("/api/s7/aircraft?num_results=10&page=1000")
            assert response.status_code == 200, "Failed with very large page number"
            assert response.json() == [], "Expected empty list for page beyond data bounds"

    def test_aircraft_no_duplicates(self, client: TestClient) -> None:
        """Test that the aircraft listing doesn't have duplicates."""
        with client as client:
            page = 0
            results = []
            while True:
                response = client.get(f"/api/s7/aircraft/?num_results=100&page={page}")
                assert response.status_code == 200, "Failed to fetch aircraft data"
                data = response.json()
                if not data:
                    break
                results.extend(a["icao"] for a in data)
                page += 1
                if page > 10:  # Safety limit to prevent infinite loops
                    break
            assert len(results) == len(set(results)), "Duplicate aircraft detected"


    def test_prepare_idempotency(self, client: TestClient) -> None:
        """Test that calling prepare multiple times works consistently."""
        with client as client:
            # Call prepare twice
            first_response = client.post("/api/s7/aircraft/prepare")
            assert first_response.status_code == 200, "First prepare call failed"

            second_response = client.post("/api/s7/aircraft/prepare")
            assert second_response.status_code == 200, "Second prepare call failed"

            # Both should return the same result
            assert first_response.text == second_response.text, "Prepare endpoint is not idempotent"

    def test_special_icao_formats(self, client: TestClient) -> None:
        """Test different ICAO format patterns."""
        with client as client:
            # Test with lowercase
            response = client.get("/api/s7/aircraft/a0f1bb/positions")
            assert response.status_code == 200, "Failed with lowercase ICAO"

            # Test with uppercase
            response = client.get("/api/s7/aircraft/A0F1BB/positions")
            assert response.status_code == 200, "Failed with uppercase ICAO"

            # Test with mixed case
            response = client.get("/api/s7/aircraft/A0f1Bb/positions")
            assert response.status_code == 200, "Failed with mixed case ICAO"

    def test_performance_batch(self, client: TestClient) -> None:
        """Test performance with a batch of requests."""
        with client as client:
            # Make multiple requests and ensure they all complete within a reasonable time
            for _ in range(5):
                start_time = pytest.importorskip("time").time()
                response = client.get("/api/s7/aircraft?num_results=100&page=0")
                end_time = pytest.importorskip("time").time()

                assert response.status_code == 200, "Batch request failed"
                assert end_time - start_time < 5, f"Request took too long: {end_time - start_time:.2f} seconds"


class TestItCanBeEvaluated:
    """
    Basic tests to ensure the endpoints work as expected for evaluation.
    """

    def test_prepare(self, client: TestClient) -> None:
        """Test the prepare endpoint."""
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"
            assert response.status_code == 200, "Failed to prepare aircraft data"

    def test_aircraft(self, client: TestClient) -> None:
        """Test the aircraft listing endpoint."""
        with client as client:
            response = client.get("/api/s7/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            # Check structure of the first item if we have data
            if r:
                for field in ["icao", "registration", "type"]:
                    assert field in r[0], f"Missing '{field}' field"


class TestSecurity:
    """Test security aspects of the API."""

    def test_sql_injection_resistance(self, client: TestClient) -> None:
        """Test that the API is resistant to SQL injection attempts."""
        with client as client:
            # Attempt SQL injection in the ICAO parameter
            injection_attempts = [
                "'; DROP TABLE aircraft_data; --",
                "' OR '1'='1",
                "'; SELECT * FROM information_schema.tables; --",
            ]

            for attempt in injection_attempts:
                response = client.get(f"/api/s7/aircraft/{attempt}/positions")
                # Should either return 404 or default data, but not expose DB info
                error_msg = f"Bad status code for injection: {response.status_code}"
                assert response.status_code in (200, 404), error_msg

                # For stats endpoint
                response = client.get(f"/api/s7/aircraft/{attempt}/stats")
                assert response.status_code in (200, 404), error_msg

    def test_invalid_inputs(self, client: TestClient) -> None:
        """Test how the API handles various invalid inputs."""
        with client as client:
            # Test with very long ICAO
            long_icao = "a" * 1000
            response = client.get(f"/api/s7/aircraft/{long_icao}/positions")
            assert response.status_code in (200, 404, 422), f"Unexpected status code: {response.status_code}"

            # Test with special characters
            special_chars = "!@#$%^&*()"
            response = client.get(f"/api/s7/aircraft/{special_chars}/positions")
            assert response.status_code in (200, 404, 422), f"Unexpected status code: {response.status_code}"

            # Test with invalid pagination parameters
            response = client.get("/api/s7/aircraft?num_results=abc&page=xyz")
            assert response.status_code in (200, 422), f"Unexpected status code: {response.status_code}"

    def test_error_handling(self, client: TestClient) -> None:
        """Test proper error handling."""
        with client as client:
            # Test nonexistent endpoint
            response = client.get("/api/s7/nonexistent")
            assert response.status_code == 404, "Expected 404 for nonexistent endpoint"

            # Test method not allowed
            response = client.delete("/api/s7/aircraft/prepare")
            assert response.status_code == 405, "Expected 405 Method Not Allowed"
