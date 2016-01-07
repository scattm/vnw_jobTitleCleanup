Change config.cfg.dist to config.cfg

Edit config and config session on `__main__.py`, ex. `es2sqlite = ES2SQLite(configfile, 'SRC_ES')`

Import from ES:
```
python JobSuggesterCleanUp import
```

Import from tsv file and export to ES index:
- Edit filename on `__main__.py`, line  `es2sqlite.insert_from_file('JobTitle-v2_5.tsv')` 
```
python JobSuggesterCleanUp importfile
```