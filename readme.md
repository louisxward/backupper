# Backupper

Version: 0.1.0

## Environment(.env place in root)

```
ACCESS_KEY=
SECRET_KEY=
ENDPOINT=
REGION=
FORCE_PATH_STYLE=
```

## Config(.env place in root/data)

```
{
  "backups": [
    {
      "directory": "",
      "bucket": "",
      "prefix": "",
      "maxBackupCount": ,
      "sizeCheck": ,
      "isZip":
    },
}
```

## Commands

`npm start` - dev command restarts on save

`docker compose build` - prod build

`docker compose up -d` - prod deploy

`docker compose logs -f` - prod logs

## ToDo

Timer
Commands for manual
