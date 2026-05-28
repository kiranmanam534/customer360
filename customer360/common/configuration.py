import json

class Configuration:
    def __init__(self):
        self.client_id = "1cc5a001-fdfc-4320-8cc8-e224f31f2f54"
        self.client_secret = dbutils.secrets.get("km-scope", "client-secret")
        self.tenant_id = "5b4616a5-0d4e-4d5f-ba9e-49a845d5f0bf"
        self.storage_account_name = "kmstorage9490"
        self.container_name = "cusromer360"
        self.base_url = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net"

    def config_spn_credentials(self):
        spark.conf.set("fs.azure.account.auth.type", "OAuth")
        spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set("fs.azure.account.oauth2.client.id", self.client_id)
        spark.conf.set("fs.azure.account.oauth2.client.secret", self.client_secret)
        spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token")


    @staticmethod
    def get_josn_file(file_path):
        with open(file_path, 'r') as file:
            return json.load(file) 