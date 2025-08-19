from pydantic import BaseModel, Field


class ColumnMappingItem(BaseModel):
    src_col: str = Field(alias="srcCol")
    dest_col: str = Field(alias="destCol")


class Configuration(BaseModel):
    client_id: str = Field(alias="clientId")
    client_md5: str = Field(alias="#clientMd5")
    list_id: int = Field(alias="listId")
    column_mapping: list[ColumnMappingItem] = Field(alias="columnMapping", default_factory=list)
