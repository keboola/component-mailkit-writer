# Component Mailkit Writer

This component takes a table stored in Keboola and makes it an e-mail recipient list in Mailkit.

## Configuration Parameters

- `clientId` (string): Client ID as stated in Mailkit (Profil – Integrace – API ID)
- `#clientMd5` (string): Client MD5 as stated in Mailkit (Profil – Integrace – MD5 kód)
- `listId` (integer): Mailing list ID which can be found in the URL of the mailing list. The ID is the number after the `ID_user_list,` string.
- `columnMapping`: List of column mappings. Each item must have:
		- `srcCol` (string): Source column name
		- `destCol` (string): Destination column name

### Example Configuration

```json
{
	"clientId": "your-client-id",
	"#clientMd5": "your-md5-hash",
	"listId": 123456,
	"columnMapping": [
		{"srcCol": "firstName", "destCol": "first_name"},
		{"srcCol": "lastName", "destCol": "last_name"}
	]
}
```
