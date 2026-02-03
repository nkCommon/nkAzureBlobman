import os
import shutil
import unittest
from dotenv import load_dotenv
from src.nkAzureBlobber import AzureBlobContainerClient

class TestAzureBlobber(unittest.TestCase):
    def setUp(self):

        load_dotenv()
        self.tenant_id = os.getenv("blob_TenantID")
        self.client_id = os.getenv("blob_ClientID")
        self.client_secret = os.getenv("blob_ClientSecret")
        self.account_url="https://naestvedprivategpt.blob.core.windows.net"
        self.container_name="kommuneguides"

        self.client = AzureBlobContainerClient(
            account_url=self.account_url,
            container_name=self.container_name,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        
        self.txt_file_content = "Dette er en test fil og må fjernes!"
        self.txt_file_content2 = "Dette er en test 2 fil og må fjernes!"
        
        return super().setUp()

    def tearDown(self):
        return super().tearDown()
    
    def test_read_files_basics(self):
        files = self.client.list_blobs(include_properties=True)
        self.assertTrue(len(files)>0)
        for file in files:
            self.assertEqual(file.container_name, self.container_name)
            

    def test_list_containers(self):
        containers = self.client.list_container_names()
        self.assertTrue(len(containers)>0)
        for container in containers:
            print(container)
        
    def test_write_delete_files_basics(self):
        files = self.client.list_blobs()
        number_of_files = len(files)
        
        sample_blob = "hello_test.txt"

        # 1) Upload: write some content to a blob (or use upload_file for a local file)
        self.client.write(sample_blob, self.txt_file_content)
        print(f"Uploaded: {sample_blob}")

        # 2) Verify it's there
        Contents = self.client.read_text(sample_blob).strip()
        self.assertEqual(Contents, self.txt_file_content)
        self.assertTrue(self.client.exists(sample_blob))

        files = self.client.list_blobs(include_properties=True)
        print(files)
        self.assertTrue(len(files) == number_of_files+1)
        for file in files:
            if file.name == sample_blob:
                self.assertEqual(file.size, 36)
                break

        # 3) Delete the blob
        self.client.delete(sample_blob)
        print(f"Deleted: {sample_blob}")

        files = self.client.list_blobs()
        self.assertTrue(len(files) == number_of_files)

    def test_upload_delete_files_basics(self):
        files = self.client.list_blobs()
        number_of_files = len(files)
        
        sample_blob = "/Users/lakas/git/nkAzureBlobman/sample.txt"
        file_name = "sample.txt"
        self.client.upload_file(local_path=sample_blob, blob_name=file_name)

        # 2) Verify it's there
        Contents = self.client.read_text(file_name).strip()
        self.assertEqual(Contents, self.txt_file_content)
        self.assertTrue(self.client.exists(file_name))

        files = self.client.list_blobs()
        self.assertTrue(len(files) == number_of_files+1)


        sample_blob = "/Users/lakas/git/nkAzureBlobman/sample2.txt"
        self.client.upload_file(local_path=sample_blob, blob_name=file_name)
        Contents = self.client.read_text(file_name).strip()
        self.assertEqual(Contents, self.txt_file_content2)

        self.assertTrue(self.client.exists(file_name))

        files = self.client.list_blobs()
        self.assertTrue(len(files) == number_of_files+1)


        # 3) Delete the blob
        self.client.delete(file_name)
        print(f"Deleted: {file_name}")

        files = self.client.list_blobs()
        self.assertTrue(len(files) == number_of_files)


