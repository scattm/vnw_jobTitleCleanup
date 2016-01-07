Change config.cfg.dist to config.cfg

Edit config and config session on `__main__.py`, ex. `es2sqlite = ES2SQLite(configfile, 'SRC_ES')`

Import from ES:
```
python JobSuggesterCleanUp import
```

Import from tsv file and export to ES index (update index name on config.cfg first):
```
python JobSuggesterCleanUp importfile path/to/file
```

TODO: move ES config session to params