/*
Go client library of Tkrzw-RPC

The core package of Tkrzw-RPC provides a server program which manages databases of Tkrzw.  This package provides a Python client library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.
*/
package tkrzw_rpc

// END OF FILE
