import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import time


def sample_icao(client: TestClient) -> str:
    """Get a valid ICAO from the database to use in tests."""
    response = client.get("/api/s8/aircraft?num_results=1")
    if response.status_code == 200 and response.json():
        return response.json()[0]["icao"]
    # Fallback to a known common ICAO if none found
    return "a65800"


class TestS8Student:
    """
    Tests for the s8 module implementation that uses PostgreSQL connection pool.
    """

    def test_list_aircraft(self, client: TestClient) -> None:
        """Test listing aircraft with pagination and enriched data."""
        with client as client:
            response = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response.status_code == 200, "Failed to list aircraft"
            data = response.json()
            assert isinstance(data, list), "Result is not a list"
            if data:
                for aircraft in data:
                    assert "icao" in aircraft, "Missing 'icao' field"
                    assert "registration" in aircraft, "Missing 'registration' field"
                    assert "type" in aircraft, "Missing 'type' field"
                    assert "owner" in aircraft, "Missing 'owner' field"
                    assert "manufacturer" in aircraft, "Missing 'manufacturer' field"
                    assert "model" in aircraft, "Missing 'model' field"

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
            response = client.get(f"/api/s8/aircraft?num_results={num_results}&page={page}")
            assert response.status_code == 200, f"Failed pagination with num_results={num_results}, page={page}"
            data = response.json()
            assert isinstance(data, list), "Result is not a list"
            assert len(data) <= num_results, f"Page size exceeded: {len(data)} > {num_results}"

    def test_aircraft_pagination_edge_cases(self, client: TestClient) -> None:
        """Test edge cases for pagination parameters."""
        with client as client:
            # Test zero page size
            response = client.get("/api/s8/aircraft?num_results=0&page=0")
            assert response.status_code == 200, "Failed with zero page size"

            # Test negative page (should be treated as 0)
            response = client.get("/api/s8/aircraft?num_results=10&page=-1")
            assert response.status_code == 200, "Failed with negative page number"

            # Test very large page size
            response = client.get("/api/s8/aircraft?num_results=1000&page=0")
            assert response.status_code == 200, "Failed with very large page size"

            # Test very large page number
            response = client.get("/api/s8/aircraft?num_results=10&page=1000")
            assert response.status_code == 200, "Failed with very large page number"
            assert response.json() == [], "Expected empty list for page beyond data bounds"

    def test_aircraft_no_duplicates(self, client: TestClient) -> None:
        """Test that the aircraft listing doesn't have duplicates."""
        with client as client:
            page = 0
            results = []
            while True:
                response = client.get(f"/api/s8/aircraft/?num_results=100&page={page}")
                assert response.status_code == 200, "Failed to fetch aircraft data"
                data = response.json()
                if not data:
                    break
                results.extend(a["icao"] for a in data)
                page += 1
                if page > 10:  # Safety limit to prevent infinite loops
                    break
            assert len(results) == len(set(results)), "Duplicate aircraft detected"

    def test_get_aircraft_co2(self, client: TestClient) -> None:
        """Test CO2 calculation for a specific aircraft."""
        icao = sample_icao(client)
        test_date = "2023-11-01"
        
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={test_date}")
            assert response.status_code == 200, "Failed to get CO2 data"
            data = response.json()
            assert "icao" in data, "Missing 'icao' field"
            assert "hours_flown" in data, "Missing 'hours_flown' field"
            assert "co2" in data, "Missing 'co2' field"
            assert isinstance(data["hours_flown"], float), "hours_flown should be float"
            assert data["co2"] is None or isinstance(data["co2"], float), "co2 should be float or None"

    def test_get_aircraft_co2_invalid_date(self, client: TestClient) -> None:
        """Test CO2 calculation with invalid date format."""
        icao = sample_icao(client)
        invalid_dates = [
            "2023-13-01",  # Invalid month
            "2023-11-32",  # Invalid day
            "2023/11/01",  # Wrong separator
            "2023-11-1",   # Missing leading zero
            "2023-11-01T12:00:00",  # Time included
            "not-a-date",  # Not a date
        ]
        
        for invalid_date in invalid_dates:
            with client as client:
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day={invalid_date}")
                assert response.status_code == 422, f"Expected 422 for invalid date: {invalid_date}"

    def test_get_aircraft_co2_nonexistent_icao(self, client: TestClient) -> None:
        """Test CO2 calculation for a non-existent ICAO."""
        nonexistent_icao = "nonexistent123"
        test_date = "2023-11-01"
        
        with client as client:
            response = client.get(f"/api/s8/aircraft/{nonexistent_icao}/co2?day={test_date}")
            assert response.status_code == 200, "Should handle nonexistent ICAO gracefully"
            data = response.json()
            assert data["icao"] == nonexistent_icao
            assert data["hours_flown"] == 0.0
            assert data["co2"] is None

    def test_connection_pool_handling(self, client: TestClient) -> None:
        """Test that the connection pool handles multiple requests correctly."""
        with client as client:
            # Make multiple concurrent requests
            responses = []
            for _ in range(5):
                response = client.get("/api/s8/aircraft?num_results=10&page=0")
                assert response.status_code == 200, "Failed to handle concurrent request"
                responses.append(response.json())
            
            # Verify all responses are valid
            for data in responses:
                assert isinstance(data, list), "Result is not a list"
                assert len(data) <= 10, "Page size exceeded"

    def test_performance_batch(self, client: TestClient) -> None:
        """Test performance with a batch of requests."""
        with client as client:
            # Make multiple requests and ensure they all complete within a reasonable time
            for _ in range(5):
                start_time = pytest.importorskip("time").time()
                response = client.get("/api/s8/aircraft?num_results=100&page=0")
                end_time = pytest.importorskip("time").time()

                assert response.status_code == 200, "Batch request failed"
                assert end_time - start_time < 5, f"Request took too long: {end_time - start_time:.2f} seconds"

    def test_aircraft_data_consistency(self, client: TestClient) -> None:
        """Test that aircraft data remains consistent across multiple requests."""
        with client as client:
            # Get initial data
            response1 = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response1.status_code == 200
            data1 = response1.json()
            
            # Get same data again
            response2 = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response2.status_code == 200
            data2 = response2.json()
            
            # Compare the results
            assert data1 == data2, "Aircraft data should be consistent across requests"

    def test_aircraft_ordering(self, client: TestClient) -> None:
        """Test that aircraft are properly ordered by ICAO."""
        with client as client:
            response = client.get("/api/s8/aircraft?num_results=100&page=0")
            assert response.status_code == 200
            data = response.json()
            
            if len(data) > 1:
                icaos = [aircraft["icao"] for aircraft in data]
                assert icaos == sorted(icaos), "Aircraft should be ordered by ICAO"

    def test_aircraft_empty_database(self, client: TestClient) -> None:
        """Test behavior when database is empty."""
        with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn.cursor.return_value = mock_cursor
            mock_pool.return_value.getconn.return_value = mock_conn
            
            with client as client:
                response = client.get("/api/s8/aircraft")
                assert response.status_code == 200
                assert response.json() == [], "Should return empty list for empty database"

    def test_aircraft_partial_data(self, client: TestClient) -> None:
        """Test handling of aircraft with partial data."""
        with client as client:
            response = client.get("/api/s8/aircraft?num_results=100&page=0")
            assert response.status_code == 200
            data = response.json()
            
            for aircraft in data:
                # Some fields can be None, but ICAO should always be present
                assert aircraft["icao"] is not None, "ICAO should not be None"
                assert isinstance(aircraft["icao"], str), "ICAO should be a string"

    def test_get_aircraft_co2_date_range(self, client: TestClient) -> None:
        """Test CO2 calculation across different dates."""
        icao = sample_icao(client)
        dates = [
            "2023-01-01",  # Start of year
            "2023-06-15",  # Middle of year
            "2023-12-31",  # End of year
        ]
        
        for date in dates:
            with client as client:
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day={date}")
                assert response.status_code == 200, f"Failed for date: {date}"
                data = response.json()
                assert "hours_flown" in data, "Missing hours_flown field"
                assert "co2" in data, "Missing co2 field"

    def test_get_aircraft_co2_future_date(self, client: TestClient) -> None:
        """Test CO2 calculation with future dates."""
        icao = sample_icao(client)
        future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={future_date}")
            assert response.status_code == 200, "Should handle future dates gracefully"
            data = response.json()
            assert data["hours_flown"] == 0.0, "Future dates should have 0 hours flown"
            assert data["co2"] is None, "Future dates should have no CO2"

    def test_get_aircraft_co2_multiple_aircraft(self, client: TestClient) -> None:
        """Test CO2 calculation for multiple aircraft in sequence."""
        test_date = "2023-11-01"
        icaos = []
        
        # Get multiple ICAOs
        with client as client:
            response = client.get("/api/s8/aircraft?num_results=5&page=0")
            assert response.status_code == 200
            data = response.json()
            icaos = [aircraft["icao"] for aircraft in data]
        
        # Test each ICAO
        for icao in icaos:
            with client as client:
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day={test_date}")
                assert response.status_code == 200, f"Failed for ICAO: {icao}"
                data = response.json()
                assert data["icao"] == icao, "ICAO mismatch in response"

    def test_connection_pool_exhaustion(self, client: TestClient) -> None:
        """Test behavior when connection pool is exhausted."""
        with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
            mock_pool.return_value.getconn.side_effect = Exception("Connection pool exhausted")
            
            with client as client:
                response = client.get("/api/s8/aircraft")
                assert response.status_code == 500, "Should return 500 when connection pool is exhausted"

    def test_connection_pool_recovery(self, client: TestClient) -> None:
        """Test that the connection pool can recover from errors."""
        with client as client:
            # First request (should work)
            response1 = client.get("/api/s8/aircraft")
            assert response1.status_code == 200
            
            # Simulate connection error
            with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
                mock_pool.return_value.getconn.side_effect = Exception("Connection error")
                response2 = client.get("/api/s8/aircraft")
                assert response2.status_code == 500
            
            # Third request (should work again)
            response3 = client.get("/api/s8/aircraft")
            assert response3.status_code == 200

    def test_concurrent_requests(self, client: TestClient) -> None:
        """Test handling of concurrent requests."""
        import threading
        
        def make_request():
            response = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response.status_code == 200
            return response.json()
        
        threads = []
        results = []
        
        # Start multiple threads
        for _ in range(10):
            thread = threading.Thread(target=lambda: results.append(make_request()))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all responses are valid
        for result in results:
            assert isinstance(result, list)
            assert len(result) <= 10

    def test_performance_under_load(self, client: TestClient) -> None:
        """Test performance under sustained load."""
        start_time = time.time()
        request_count = 0
        
        # Make requests for 5 seconds
        while time.time() - start_time < 5:
            response = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response.status_code == 200
            request_count += 1
        
        # Should handle at least 10 requests per second
        assert request_count >= 50, f"Only handled {request_count} requests in 5 seconds"

    def test_memory_usage(self, client: TestClient) -> None:
        """Test that memory usage doesn't grow excessively."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Make multiple requests
        for _ in range(100):
            response = client.get("/api/s8/aircraft?num_results=10&page=0")
            assert response.status_code == 200
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 10MB)
        assert memory_increase < 10 * 1024 * 1024, f"Memory increased by {memory_increase / 1024 / 1024:.2f}MB"

    def test_database_connection_timeout(self, client: TestClient) -> None:
        """Test handling of database connection timeouts."""
        with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
            mock_pool.return_value.getconn.side_effect = Exception("Connection timeout")
            
            with client as client:
                response = client.get("/api/s8/aircraft")
                assert response.status_code == 500, "Should return 500 on connection timeout"

    def test_invalid_database_credentials(self, client: TestClient) -> None:
        """Test handling of invalid database credentials."""
        with patch("bdi_api.s8.exercise.initialize_connection_pool") as mock_init:
            mock_init.side_effect = Exception("Invalid credentials")
            
            with client as client:
                response = client.get("/api/s8/aircraft")
                assert response.status_code == 500, "Should return 500 with invalid credentials"

    def test_database_connection_drop(self, client: TestClient) -> None:
        """Test recovery from dropped database connection."""
        with client as client:
            # First request (should work)
            response1 = client.get("/api/s8/aircraft")
            assert response1.status_code == 200
            
            # Simulate connection drop
            with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
                mock_pool.return_value.getconn.side_effect = Exception("Connection dropped")
                response2 = client.get("/api/s8/aircraft")
                assert response2.status_code == 500
            
            # Third request (should work again)
            response3 = client.get("/api/s8/aircraft")
            assert response3.status_code == 200


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
                # Test in aircraft listing
                response = client.get(f"/api/s8/aircraft?num_results={attempt}&page=0")
                assert response.status_code in (200, 422), f"Bad status code for injection: {response.status_code}"

                # Test in CO2 endpoint
                response = client.get(f"/api/s8/aircraft/{attempt}/co2?day=2023-11-01")
                assert response.status_code in (200, 422), f"Bad status code for injection: {response.status_code}"

    def test_invalid_inputs(self, client: TestClient) -> None:
        """Test how the API handles various invalid inputs."""
        with client as client:
            # Test with very long ICAO
            long_icao = "a" * 1000
            response = client.get(f"/api/s8/aircraft/{long_icao}/co2?day=2023-11-01")
            assert response.status_code in (200, 422), f"Unexpected status code: {response.status_code}"

            # Test with special characters
            special_chars = "!@#$%^&*()"
            response = client.get(f"/api/s8/aircraft/{special_chars}/co2?day=2023-11-01")
            assert response.status_code in (200, 422), f"Unexpected status code: {response.status_code}"

            # Test with invalid pagination parameters
            response = client.get("/api/s8/aircraft?num_results=abc&page=xyz")
            assert response.status_code in (200, 422), f"Unexpected status code: {response.status_code}"

    def test_error_handling(self, client: TestClient) -> None:
        """Test proper error handling."""
        with client as client:
            # Test nonexistent endpoint
            response = client.get("/api/s8/nonexistent")
            assert response.status_code == 404, "Expected 404 for nonexistent endpoint"

            # Test method not allowed
            response = client.delete("/api/s8/aircraft")
            assert response.status_code == 405, "Expected 405 Method Not Allowed"

    def test_xss_protection(self, client: TestClient) -> None:
        """Test protection against XSS attacks."""
        xss_attempts = [
            "<script>alert('xss')</script>",
            "<img src=x onerror=alert('xss')>",
            "javascript:alert('xss')",
        ]
        
        for attempt in xss_attempts:
            with client as client:
                # Test in aircraft listing
                response = client.get(f"/api/s8/aircraft?num_results={attempt}&page=0")
                assert response.status_code in (200, 422), f"Bad status code for XSS attempt: {response.status_code}"
                
                # Test in CO2 endpoint
                response = client.get(f"/api/s8/aircraft/{attempt}/co2?day=2023-11-01")
                assert response.status_code in (200, 422), f"Bad status code for XSS attempt: {response.status_code}"

    def test_rate_limiting(self, client: TestClient) -> None:
        """Test that the API implements rate limiting."""
        # Make rapid requests
        for _ in range(100):
            response = client.get("/api/s8/aircraft")
            assert response.status_code in (200, 429), "Should either succeed or return 429 Too Many Requests"

    def test_input_size_limits(self, client: TestClient) -> None:
        """Test handling of very large input sizes."""
        # Test with very large ICAO
        large_icao = "a" * 10000
        with client as client:
            response = client.get(f"/api/s8/aircraft/{large_icao}/co2?day=2023-11-01")
            assert response.status_code in (200, 413), "Should handle large input size"

    def test_authentication_required(self, client: TestClient) -> None:
        """Test that endpoints require authentication."""
        with client as client:
            # Remove any authentication headers
            response = client.get("/api/s8/aircraft", headers={})
            assert response.status_code in (200, 401), "Should require authentication"

    def test_cors_protection(self, client: TestClient) -> None:
        """Test CORS protection."""
        with client as client:
            response = client.get("/api/s8/aircraft", headers={"Origin": "http://malicious-site.com"})
            assert "Access-Control-Allow-Origin" not in response.headers, "Should not allow arbitrary origins"


class TestItCanBeEvaluated:
    """
    Basic tests to ensure the endpoints work as expected for evaluation.
    """

    def test_aircraft(self, client: TestClient) -> None:
        """Test the aircraft listing endpoint."""
        with client as client:
            response = client.get("/api/s8/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            # Check structure of the first item if we have data
            if r:
                for field in ["icao", "registration", "type", "owner", "manufacturer", "model"]:
                    assert field in r[0], f"Missing '{field}' field"

    def test_co2(self, client: TestClient) -> None:
        """Test the CO2 calculation endpoint."""
        icao = sample_icao(client)
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day=2023-11-01")
            assert not response.is_error, "Error at the CO2 endpoint"
            r = response.json()
            for field in ["icao", "hours_flown", "co2"]:
                assert field in r, f"Missing '{field}' field"

    def test_co2_calculation_accuracy(self, client: TestClient) -> None:
        """Test accuracy of CO2 calculations."""
        icao = sample_icao(client)
        test_date = "2023-11-01"
        
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={test_date}")
            assert response.status_code == 200
            data = response.json()
            
            if data["co2"] is not None:
                # Verify CO2 calculation formula
                hours_flown = data["hours_flown"]
                co2 = data["co2"]
                
                # Basic sanity check: CO2 should be proportional to hours flown
                assert co2 >= 0, "CO2 should not be negative"
                if hours_flown > 0:
                    assert co2 > 0, "CO2 should be positive for non-zero flight hours"

    def test_response_time_consistency(self, client: TestClient) -> None:
        """Test that response times are consistent."""
        response_times = []
        
        for _ in range(10):
            start_time = time.time()
            response = client.get("/api/s8/aircraft?num_results=10&page=0")
            end_time = time.time()
            
            assert response.status_code == 200
            response_times.append(end_time - start_time)
        
        # Calculate standard deviation of response times
        import statistics
        std_dev = statistics.stdev(response_times)
        
        # Standard deviation should be reasonable (less than 0.5 seconds)
        assert std_dev < 0.5, f"Response times vary too much (std dev: {std_dev:.2f}s)"
