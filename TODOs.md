# DTN-RPyC TODOs

- [x] Implement Rhizome
- [ ] Implement MSP
    - [ ] Use C library for MSP connections
        - [ ] probably write C wrapper?
    - [x] Implement argument parser for MSP
    - [ ] Use `-p` option for MSP
- [ ] Implement transparent mode
    - [ ] MDP loopup
        - [ ]  Use C library for MDP lookup
            - [ ]  Probably write C wrapper?
- [ ] Update all call to match [technology overview](/technology.md)
- [x] Cascading simple procedures
- [x] Cascading procedures
    - [x] Cascading complex procedures
    - [x] Also, use the cleanup as described in [technology overview](/technology.md)
    - [x] Filter parameter inside cc-jobfile
    - [x] Global filters inside cc-jobfile
    - [x] Option to delete files jobfiles/archives after execution

- [x] Publishing procedures
- [x] Publishing capabilities
- [x] Optional timeout functionality
- [x] Error log
- [x] Test cases


For more information, see:

- [DTN-RPC paper](http://dl.ifip.org/db/conf/networking/networking2017/1570334581.pdf)
- [Original DTN-RPC repo](https://github.com/adur1990/DTN-RPC)
- [Technology Overview](/technology.md)
