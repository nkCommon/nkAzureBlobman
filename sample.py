import os
from dotenv import load_dotenv
from src.nkAzureBlobber import AzureBlobContainerClient

# --- Sample: upload a file, then delete it ---
if __name__ == "__main__":
    load_dotenv()
    tenant_id = os.getenv("blob_TenantID")
    client_id = os.getenv("blob_ClientID")
    client_secret = os.getenv("blob_ClientSecret")

    client = AzureBlobContainerClient(
        account_url="https://naestvedprivategpt.blob.core.windows.net",
        container_name="kommuneguides",
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    
    # print the list of blobs
    print(client.list_blobs())


    # Sample blob name (use a unique name so we don't overwrite real data)
    sample_blob = "sample/hello.txt"

    # 1) Upload: write some content to a blob (or use upload_file for a local file)
    client.write(sample_blob, "Hello from azure_blob_test.py\n")
    print(f"Uploaded: {sample_blob}")

    # Optional: upload a local file instead
    # client.upload_file("sample/myfile.pdf", "/path/to/myfile.pdf")

    # 2) Verify it's there
    print("Contents:", client.read_text(sample_blob).strip())
    print("Exists:", client.exists(sample_blob))

    # 3) Delete the blob
    client.delete(sample_blob)
    print(f"Deleted: {sample_blob}")

    # 4) Verify it's gone
    print("Exists after delete:", client.exists(sample_blob))
