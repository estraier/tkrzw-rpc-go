/*
Go client library of Tkrzw-RPC

The core package of Tkrzw-RPC provides a server program which manages databases of Tkrzw.  This package provides a Python client library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.

The class "RemoteDBM" has a similar API to the local DBM API, which represents an associative array aka a map[[]byte][]byte in Go.  Read the homepage https://dbmx.net/tkrzw-rpc/ for details.

All identifiers are defined under the package "tkrzw_rpc", which can be imported in source files of application programs as "github.com/estraier/tkrzw-rpc-go".

 import "github.com/estraier/tkrzw-rpc-go"

An instance of the struct "RemoteDBM" is used in order to handle a database connection.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the struct "Status".  Iterator to access each record is implemented by the struct "Iterator".

The key and the value of the records are stored as byte arrays.  However, you can specify strings and other types which imlements the Stringer interface whereby the object is converted into a byte array.

If you write the above import directive and prepare the "go.mod" file, the Go module for Tkrzw-RPC is installed implicitly when you run "go get".  Go 1.14 or later is required to use this package.

The following code is a simple example to use a database, without checking errors.  Many methods accept both byte arrays and strings.  If strings are given, they are converted implicitly into byte arrays.

 package main

 import (
   "fmt"
   "github.com/estraier/tkrzw-rpc-go"
 )

 func main() {
   // Prepares the database.
   dbm := tkrzw_rpc.NewRemoteDBM()
   dbm.Connect("127.0.0.1:1978", -1, "")

   // Sets records.
   // Keys and values are implicitly converted into bytes.
   dbm.Set("first", "hop", true)
   dbm.Set("second", "step", true)
   dbm.Set("third", "jump", true)

   // Retrieves record values as strings.
   fmt.Println(dbm.GetStrSimple("first", "*"))
   fmt.Println(dbm.GetStrSimple("second", "*"))
   fmt.Println(dbm.GetStrSimple("third", "*"))

   // Checks and deletes a record.
   if dbm.Check("first") {
     dbm.Remove("first")
   }

   // Traverses records with a range over a channel.
   for record := range dbm.EachStr() {
     fmt.Println(record.Key, record.Value)
   }

   // Closes the connection.
   dbm.Disconnect()
 }

The following code is an advanced example where a so-called long transaction is done by the compare-and-exchange (aka compare-and-swap) idiom.  The example also shows how to use the iterator to access each record.

 package main

 import (
   "fmt"
   "github.com/estraier/tkrzw-rpc-go"
 )

 func main() {
   // Prepares the database.
   // The timeout is in seconds.
   // The method OrDie causes panic if the status is not success.
   // You should write your own error handling in large scale programs.
   dbm := tkrzw_rpc.NewRemoteDBM()
   dbm.Connect("localhost:1978", 10, "").OrDie()

   // Closes the connection for sure and checks the error too.
   defer func() { dbm.Disconnect().OrDie() }()

   // Two bank accounts for Bob and Alice.
   // Numeric values are converted into strings implicitly.
   dbm.Set("Bob", 1000, false).OrDie()
   dbm.Set("Alice", 3000, false).OrDie()

   // Function to do a money transfer atomically.
   transfer := func(src_key string, dest_key string, amount int64) *tkrzw_rpc.Status {
     // Gets the old values as numbers.
     old_src_value := tkrzw_rpc.ToInt(dbm.GetStrSimple(src_key, "0"))
     old_dest_value := tkrzw_rpc.ToInt(dbm.GetStrSimple(dest_key, "0"))

     // Calculates the new values.
     new_src_value := old_src_value - amount
     new_dest_value := old_dest_value + amount
     if new_src_value < 0 {
       return tkrzw_rpc.NewStatus(tkrzw_rpc.StatusApplicationError, "insufficient value")
     }

     // Prepares the pre-condition and the post-condition of the transaction.
     old_records := []tkrzw_rpc.KeyValueStrPair{
       {src_key, tkrzw_rpc.ToString(old_src_value)},
       {dest_key, tkrzw_rpc.ToString(old_dest_value)},
     }
     new_records := []tkrzw_rpc.KeyValueStrPair{
       {src_key, tkrzw_rpc.ToString(new_src_value)},
       {dest_key, tkrzw_rpc.ToString(new_dest_value)},
     }

     // Performs the transaction atomically.
     // This fails safely if other concurrent transactions break the pre-condition.
     return dbm.CompareExchangeMultiStr(old_records, new_records)
   }

   // Tries a transaction until it succeeds
   var status *tkrzw_rpc.Status
   for num_tries := 0; num_tries < 100; num_tries++ {
     status = transfer("Alice", "Bob", 500)
     if !status.Equals(tkrzw_rpc.StatusInfeasibleError) {
       break
     }
   }
   status.OrDie()

   // Traverses records in a primitive way.
   iter := dbm.MakeIterator()
   defer iter.Destruct()
   iter.First()
   for {
     key, value, status := iter.GetStr()
     if !status.IsOK() {
       break
     }
     fmt.Println(key, value)
     iter.Next()
   }
 }
*/
package tkrzw_rpc

// END OF FILE
