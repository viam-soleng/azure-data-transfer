import asyncio
import json
import base64

from io import BytesIO
from PIL import Image
from typing import List

# Viam packages
from viam.app.viam_client import ViamClient
from viam.rpc.dial import DialOptions
from viam.proto.app.data import Filter, BinaryData, BinaryID

# Azure packages
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from azure.core.credentials import AzureNamedKeyCredential, TokenCredential, AzureSasCredential


class AzureAccount():

    azureResourceGroup: str
    azureStorageAccount: str
    azureContainerName: str

    SASToken: str

    file_system: FileSystemClient

    def __init__(self):
        self.azureResourceGroup = "<RESOURCE_GROUP>"
        self.azureStorageAccount = "<STORAGE_ACCOUNT"
        self.azureContainerName = "<CONTAINER_NAME>"

	self.SASToken = "<TOKEN>"
        print("Create data lake service client...")
        service = DataLakeServiceClient("https://{}.dfs.core.windows.net/".format(self.azureStorageAccount), credential=self.SASToken)

        print("Getting container (file system)...")
        self.file_system = service.get_file_system_client(self.azureContainerName)

    def upload_data(self, filename, data):
        print("Uploading data to azure data lake container...")

        file = self.file_system.create_file(filename)
        file.append_data(data, offset=0, length=len(data))
        file.flush_data(len(data))


class ViamCloudData():
    
    app_client : None
    api_key_id: str
    api_key: str
    outdir: str

    azure_account: AzureAccount

    def __init__(self, azure_account: AzureAccount):
        self.api_key = "<API_KEY>"
        self.api_key_id = "<API_KEY_ID>"
        self.outdir = "<OUTPut_DIRECTORY>"
        self.save_images = True

        self.azure_account = azure_account

    async def viam_connect(self) -> ViamClient:
        dial_options = DialOptions.with_api_key( 
            api_key=self.api_key,
            api_key_id=self.api_key_id
        )
        return await ViamClient.create_from_dial_options(dial_options)

    async def get_image_dataset(self, dataset_id) -> List[BinaryData]:
        print("Filtering on dataset_id: " + dataset_id)

        print("Get binary ids...")
        dataset_binary_ids = await self.get_binary_ids(dataset_id)
        print("Received " + str(len(dataset_binary_ids)) + " binary_ids")

        print("Get binary dataset...")
        dataset = await self.get_binary_data_by_ids(dataset_binary_ids)
        print("Image data size: " + str(len(dataset)))

        if self.save_images:
            print("Saving dataset...")
            await self.save_dataset(dataset)
        
        return dataset

    async def get_binary_ids(self, dataset_id) -> List[BinaryData]:
        filter_args = {}
        filter_args['dataset_id'] = dataset_id
        filter = Filter(**filter_args)
        binary_args = {'filter': filter, 'include_binary_data': False}

        dataset_binary_ids = []
        done = False
        while not done:
            binary_ids = await self.app_client.data_client.binary_data_by_filter(**binary_args)
            if len(binary_ids[0]):
                dataset_binary_ids.extend(binary_ids[0])
                binary_args['last'] = binary_ids[2]
            else:
                done = True

        return dataset_binary_ids
    
    async def get_binary_data_by_ids(self, binary_ids: List[BinaryData]) -> List[BinaryData]:
        my_ids = []
        for binary_id in binary_ids:
            my_ids.append(BinaryID(
            file_id=binary_id.metadata.id,
            organization_id=binary_id.metadata.capture_metadata.organization_id,
            location_id=binary_id.metadata.capture_metadata.location_id
        ))
        
        return await self.app_client.data_client.binary_data_by_ids(my_ids) 
    
    async def save_dataset(self, dataset: List[BinaryData]) -> None:
        i = 0
        for data in dataset:
            print(data.metadata)
            image = Image.open(BytesIO(data.binary))
            image.save(self.outdir + "/" + str(i) + ".jpg")
            i += 1
        return

    async def close(self):
        self.app_client.close()


def convert_viam_data_to_azure(data: BinaryData) -> bytes: 

    # Get image labels
    labels = []
    for tag in data.metadata.capture_metadata.tags:
        labels.append(tag)

    # Get detections
    detections = []
    for bbox in data.metadata.annotations.bboxes:
        detections.append({
            "label": bbox.label,
            "x_min": bbox.x_min_normalized,
            "y_min": bbox.y_min_normalized,
            "x_max": bbox.x_max_normalized,
            "y_max": bbox.y_max_normalized,
        })

    # Create json for upload
    blob_data = {
        "data": base64.b64encode(data.binary).decode("ascii"),
        "labels": labels,
        "detections": detections
    }

    return json.dumps(blob_data).encode("utf-8")

async def main():
    ac = AzureAccount()

    dataset_id = '666737f22889729a021efb8f'
    td = ViamCloudData(ac)
    td.app_client = await td.viam_connect()
    dataset = await td.get_image_dataset(dataset_id)

    print("--------------------------------------------")
    i = 0
    for data in dataset:
        print("File #{}/{}".format(i, len(dataset)))
        filename = "{}.json".format(data.metadata.id)
        blob_data = convert_viam_data_to_azure(data)

        ac.upload_data(filename, blob_data)
        i += 1

    await td.close()

if __name__ == "__main__":
    asyncio.run(main())
