from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


class TestS4:
    def test_download_and_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=1")
            assert response.status_code == 200, "Download request failed"

        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200, "Prepare request failed"

    def test_download_large_file_limit(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=100")
            assert response.status_code == 200, "Failed to download with large file_limit=100"

class TestEdgeCases:
    def test_download_file_limit_zero(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=0")
            assert response.status_code == 200, "Zero file limit should not cause failure"

    def test_prepare_no_files_in_s3(self, client: TestClient) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3
            mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

            with client as client:
                response = client.post("/api/s4/aircraft/prepare")
                assert response.status_code == 200, "Prepare should handle empty S3 gracefully"

    def test_invalid_s3_response(self, client: TestClient) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3
            mock_s3.list_objects_v2.return_value = None  # Simulate an invalid response

            with client as client:
                response = client.post("/api/s4/aircraft/prepare")
                assert response.status_code == 200, "Prepare should handle invalid S3 response"
