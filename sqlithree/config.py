from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict()
    aws_access_key_id: str = Field()
    aws_secret_access_key: str = Field()
    sqlithree_bucket_name: str = Field()
    
settings = Settings()