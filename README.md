emfas
=====

This library started off as a tool to identify music running on (live)streams, but
it evolved into a toolset to work with the [Echoprint](http://echoprint.me) framework.


## emfas

The main library, used to identify music from streams.


## emfas.server

This is a python library to work with an echoprint server, which also
has a commandline tool:

```
python -m emfas.server {ingest, fastingest, split}
```

### ingest

Ingest allows you to insert youtube playlists (more to come)
into your echoprint server.

### fastingest

Fastingest is basically a replacement for the fastingest tool from
echoprint. Only that it is really fast and doesn't load all the data
into memory at once, making it suitable for really big files (like the
[moomash database dumps](http://www.mooma.sh/api.html)).

### split

A replacement for the split utility from the echoprint server, it actually
works on big files ...