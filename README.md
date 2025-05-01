# B+Tree Database Engine Documentation

## Overview

The B+Tree Database Engine is a lightweight, transactional database system designed for educational purposes. It features a B+Tree-based index structure for efficient data access, a buffer pool for memory management, and ACID-compliant transactions. The implementation is written in Go and provides a solid foundation for understanding database internals.

## Architecture

The database engine consists of several key components:

### Storage Layer

#### Buffer Pool
The buffer pool manager handles the caching of pages from disk to memory, implementing a replacement policy (likely LRU) to determine which pages to evict when the buffer is full. Key features include:

- Page fetching and pinning
- Dirty page tracking
- Page replacement algorithms
- Thread-safe operations

#### Pages
Pages are the fundamental unit of storage in the database. Each page:
- Has a unique page ID
- Contains serialized data
- Tracks its dirty status (modified but not yet written to disk)
- Can be pinned in memory to prevent eviction

#### B+Tree Index
The B+Tree implementation is used for both clustered and non-clustered indexes. The B+Tree structure:
- Maintains sorted keys for efficient range queries
- Supports page splitting when nodes become full
- Links leaf nodes for range scans
- Handles concurrent access

### Transaction Management

#### Transaction
Each transaction has:
- A unique transaction ID
- A status (active, committed, aborted)
- Read/write sets
- Locks acquired

#### Lock Manager
The lock manager implements a two-phase locking protocol:
- Supports shared (read) and exclusive (write) locks
- Manages lock acquisition and release
- Prevents deadlocks
- Coordinates concurrent access to resources

### Query Processing

#### Query Types
The engine supports basic SQL operations:
- SELECT (range scans)
- INSERT
- UPDATE
- DELETE

#### Table Management
Tables are defined with:
- Schema information (columns, types)
- Primary key
- Optional secondary indexes

## Key Features

### ACID Compliance

- **Atomicity**: Transactions are all-or-nothing operations
- **Consistency**: Data remains in a valid state after transactions
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Committed transactions survive system failures through Write-Ahead Logging

### Concurrency Control

The database uses a combination of:
- Two-phase locking for transaction isolation
- Mutex locks for critical sections
- Lock upgrading for efficient operation

### Recovery

A Write-Ahead Log (WAL) is used to ensure data durability:
- Logs operations before they're applied to data pages
- Enables crash recovery
- Records transaction begin, operations, and commit/abort

## Usage Example

```go
// Create a new database
db, err := model.NewDatabase("mydb", "mydb.dat", "mydb.wal", 4096, 20)
if err != nil {
    // Handle error
}
defer db.Close()

// Define a schema
schema := model.Schema{
    Columns: []model.Column{
        {Name: "id", Type: model.ColumnTypeInt, NotNull: true},
        {Name: "name", Type: model.ColumnTypeString, NotNull: true},
        {Name: "age", Type: model.ColumnTypeInt, NotNull: true},
    },
    PrimaryKey: "id",
}

// Create a table
_, err = db.CreateTable("users", schema, 4)
if err != nil {
    // Handle error
}

// Begin a transaction
trans, err := db.TransactionManager.Begin()
if err != nil {
    // Handle error
}

// Insert data
usersTable, _ := db.GetTable("users")
row := map[string][]byte{
    "id":    IntToBytes(1),
    "name":  []byte("Alice"),
    "age":   []byte{0, 0, 0, 30},
}
query := model.NewQuery(model.QueryTypeInsert, "", nil, nil, row, trans, usersTable)
_, err = query.Execute()

// Commit the transaction
db.TransactionManager.Commit(trans)

// Query data
querySelect := model.NewQuery(model.QueryTypeSelect, "id", IntToBytes(1), IntToBytes(10), nil, trans, usersTable)
results, err := querySelect.Execute()
```

## Implementation Details

### B+Tree Implementation

The B+Tree implementation is a critical component of the database's performance:

- Each node in the tree is stored as a page in the buffer pool
- Internal nodes store keys and pointers to child nodes
- Leaf nodes store keys and values (or record pointers)
- Split operations maintain the balanced property of the tree
- Leaf nodes are linked for efficient range scans

#### Node Structure

```go
type BNode struct {
    isLeaf   bool           // Whether this is a leaf node
    PageID   types.PageID   // ID of the page containing this node
    Parent   types.PageID   // Parent node's page ID (0 for root)
    Keys     [][]byte       // Keys stored in this node
    Values   [][]byte       // Values (only for leaf nodes)
    PageIDs  []types.PageID // Child page IDs (only for internal nodes)
    NextLeaf types.PageID   // Next leaf node (only for leaf nodes)
}
```

### Locking Strategy

The locking strategy is crucial for ensuring transaction isolation while maintaining performance:

1. **Table-Level Locking**: Locks the entire table during schema changes
2. **Page-Level Locking**: Locks individual pages during reads and writes
3. **Tree-Level Locking**: Special locks for tree structure modifications
4. **Lock Upgrading**: Shared locks can be upgraded to exclusive locks when needed
5. **Two-Phase Locking**: All locks are acquired before any are released

### Known Limitations and Challenges

1. **Root Node Changes**: During B+Tree splits that create a new root, special care must be taken to ensure atomic updates and proper locking.

2. **Concurrency Issues**: High concurrency can lead to performance issues due to lock contention.

3. **Serialization/Deserialization**: The conversion between in-memory structures and disk representations must be carefully managed to avoid data corruption.

4. **Memory Management**: The buffer pool must effectively balance between caching and eviction to maintain performance.

## Best Practices

When working with this database engine, consider these guidelines:

1. **Transaction Scope**: Keep transactions as short as possible to minimize contention.

2. **Lock Granularity**: Use the appropriate lock level (shared vs. exclusive) based on operation needs.

3. **Buffer Pool Sizing**: Tune the buffer pool size based on your workload and available memory.

4. **Indexing Strategy**: Choose appropriate columns for indexing based on query patterns.

5. **Error Handling**: Always check return errors and handle transaction aborts appropriately.

## Troubleshooting

Common issues and their solutions:

1. **Deadlocks**: If transactions are deadlocking, consider implementing deadlock detection or prevention.

2. **Data Corruption**: Ensure all transactions are properly committed or aborted, and verify serialization/deserialization code.

3. **Performance Issues**: Analyze lock contention, buffer pool hit rates, and optimize query patterns.

4. **Root Node Changes**: Special care is needed when modifying the tree structure, particularly when changing the root node. Ensure proper locking is in place.

## Future Improvements

Potential enhancements for the database engine:

1. **Query Optimizer**: Implement a cost-based optimizer for query execution.

2. **Improved Concurrency**: Implement more fine-grained locking or consider alternative concurrency control mechanisms.

3. **Compression**: Add data compression to reduce I/O and memory usage.

4. **Distributed Operation**: Extend to support distributed transactions and replication.

5. **Advanced Indexing**: Implement additional index types like hash indexes or R-trees.
