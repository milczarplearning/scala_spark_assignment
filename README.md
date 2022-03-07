### Building
```shell
sbt package
```
### Running
```shell
sbt run
```

### Limitations
- I am not handling exceptions
- I simplified dataset loading 
- I should have added config file but I tried to keep everything as simple as possible
- Saving to txt file is  not proper for greater amount of data

### Others
- I didn't add any comments, I tried to use descriptive method names.
- I should have added more unit tests but the logic is so trivial I didn't.
- Tested on jdk 11, linux fedora