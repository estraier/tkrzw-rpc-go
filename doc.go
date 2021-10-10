/*
Go client library of Tkrzw-RPC

The core package of Tkrzw-RPC provides a server program which manages databases of Tkrzw.  This package provides a Python client library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.

The class "RemoteDBM" has a similar API to the local DBM API, which represents an associative array aka a map[[]byte][]byte in Go.  Read the homepage https://dbmx.net/tkrzw-rpc/ for details.

All identifiers are defined under the package "tkrzw_rpc", which can be imported in source files of application programs as "github.com/estraier/tkrzw-rpc-go".

 import "github.com/estraier/tkrzw-rpc-go"

An instance of the struct "RemoteDBM" is used in order to handle a database connection.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the struct "Status".  Iterator to access each record is implemented by the struct "Iterator".

The key and the value of the records are stored as byte arrays.  However, you can specify strings and other types which imlements the Stringer interface whereby the object is converted into a byte array.

If you write the above import directive, the Go module for Tkrzw-RPC is installed implicitly when you build or run your program.  Go 1.14 or later is required to use this package.



   
*/
package tkrzw_rpc

// END OF FILE
