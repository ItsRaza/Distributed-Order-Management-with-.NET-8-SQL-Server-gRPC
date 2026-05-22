# RegionShard: Complete Step-by-Step Guide
## Distributed Order Management with .NET 8, SQL Server & gRPC

> **Who this is for:** You have strong .NET/C# experience but have never worked with sharding or gRPC. This guide assumes zero prior knowledge of either topic and builds up from first principles. Every concept is explained before the code is written.

---

## Table of Contents

1. [Conceptual Foundation — Read This First](#1-conceptual-foundation)
2. [What You Will Build](#2-what-you-will-build)
3. [Prerequisites & Setup](#3-prerequisites--setup)
4. [Phase 1 — Understanding gRPC Before Writing Any Code](#4-phase-1--understanding-grpc)
5. [Phase 2 — Understanding Sharding Before Writing Any Code](#5-phase-2--understanding-sharding)
6. [Phase 3 — Solution Structure & Docker Setup](#6-phase-3--solution-structure--docker-setup)
7. [Phase 4 — Proto Contracts (The gRPC "API Specification")](#7-phase-4--proto-contracts)
8. [Phase 5 — The Shard Router Library](#8-phase-5--the-shard-router-library)
9. [Phase 6 — Order Service (gRPC Server + Shard-Aware EF Core)](#9-phase-6--order-service)
10. [Phase 7 — Inventory Service (gRPC Server + Cross-Shard Reads)](#10-phase-7--inventory-service)
11. [Phase 8 — API Gateway (HTTP → gRPC Clients)](#11-phase-8--api-gateway)
12. [Phase 9 — Running & Testing the Full System](#12-phase-9--running--testing)
13. [Phase 10 — Resilience with Polly](#13-phase-10--resilience-with-polly)
14. [Common Errors & How to Fix Them](#14-common-errors--how-to-fix-them)
15. [Further Learning Roadmap](#15-further-learning-roadmap)

---

## 1. Conceptual Foundation

Before writing a single line of code, understand the two technologies independently.

### What is gRPC?

REST APIs communicate using JSON over HTTP/1.1. Every field name is sent as a string every time, the payload is human-readable text, and a client can only call one operation per connection at a time.

gRPC does three things differently:

**Binary serialization with Protocol Buffers (Protobuf).** Instead of `{"orderId": "abc-123", "region": "ME"}`, gRPC encodes messages into a compact binary format. A typical message that is 200 bytes as JSON becomes 20-40 bytes as Protobuf. This matters enormously when Order Service is calling Inventory Service thousands of times per minute inside your data centre.

**HTTP/2 transport.** HTTP/2 allows multiple requests to be in-flight simultaneously over a single connection, enables server push, and supports bidirectional streaming. This is what makes gRPC streaming possible.

**Contract-first development via `.proto` files.** Before writing any service code, you define a `.proto` file that describes your service methods and message shapes. The .NET tooling auto-generates all the boilerplate client and server code from that file. Both the client and server reference the same `.proto`, so they are always in sync. This is similar to a shared interface in C# — a compile-time contract rather than a runtime agreement.

There are four gRPC call patterns. In this project you will use the first two:

| Pattern | Description | Use Case in This Project |
|---|---|---|
| Unary | Client sends one request, server returns one response | `PlaceOrder`, `CheckInventory` |
| Server streaming | Client sends one request, server sends a stream of responses | `WatchOrderStatus` (optional bonus) |
| Client streaming | Client streams many messages, server replies once | Not used here |
| Bidirectional streaming | Both sides stream simultaneously | Not used here |

### What is Database Sharding?

A single SQL Server instance has a ceiling. At some point, no matter how much you scale the machine vertically (more RAM, faster SSD), it cannot handle the read/write throughput your application demands. Sharding solves this by splitting the data across multiple independent database instances.

Each instance is called a **shard**. A shard contains only a subset of the rows. Every shard has identical table structure, but different data.

**The shard key** is the column used to decide which shard a row belongs to. Choosing the right shard key is the most important architectural decision in a sharding design. A bad shard key creates **hot shards** — one shard gets 80% of the traffic while others sit idle.

**Shard routing** is the logic that maps a shard key value to a specific database connection string. This routing logic is what your application uses instead of a single `DbContext`. Two common strategies:

- **Hash-based routing:** `shard_index = hash(region_id) % number_of_shards`. Simple, even distribution, but hard to add new shards later.
- **Range-based routing:** "Region IDs 1–100 go to Shard 0, 101–200 go to Shard 1." Easy to add shards, but prone to hot spots if one range is busier.

In this project you will use hash-based routing with `region_id` as the shard key.

**The key tradeoff to internalize:** Once you shard, you lose cross-shard transactions. If an order is created on Shard 0 but the inventory it references lives on Shard 1, you cannot wrap both operations in a single `BEGIN TRANSACTION / COMMIT`. You must handle partial failures at the application level. This is the primary reason sharding is an advanced topic and should not be introduced prematurely.

---

## 2. What You Will Build

**System: RegionShard** — a distributed order management platform where orders are stored across SQL Server shards partitioned by region.

### Services

| Service | Role | Port |
|---|---|---|
| `ApiGateway` | Public HTTP API; clients call this | 5000 |
| `OrderService` | gRPC server; handles order creation and retrieval | 5001 |
| `InventoryService` | gRPC server; checks and reserves stock per region | 5002 |

### Shard Layout (SQL Server via Docker)

| Shard | Database Name | Port | Regions Served |
|---|---|---|---|
| Shard 0 | `RegionShardDb_0` | 1433 (instance 1) | Asia-Pacific (region_id % 3 == 0) |
| Shard 1 | `RegionShardDb_1` | 1434 (instance 2) | Middle East (region_id % 3 == 1) |
| Shard 2 | `RegionShardDb_2` | 1435 (instance 3) | Americas (region_id % 3 == 2) |

### Data Flow for "Place Order"

```
Client (Postman/curl)
    │  HTTP POST /orders
    ▼
ApiGateway (ASP.NET Core)
    │  gRPC PlaceOrder(region_id, product_id, qty)
    ▼
OrderService
    │  gRPC CheckInventory(region_id, product_id, qty)
    ▼
InventoryService
    │  Reads from correct shard via ShardRouter
    ▼
SQL Server Shard N
    │  Returns available stock
    ▼
InventoryService returns response
    │
OrderService → ShardRouter → writes Order to correct SQL Server shard
    │
Returns OrderId to ApiGateway
    │
HTTP 201 Created back to client
```

---

## 3. Prerequisites & Setup

### Software Required

- **.NET 8 SDK** — https://dotnet.microsoft.com/download/dotnet/8.0
- **Docker Desktop** — https://www.docker.com/products/docker-desktop (for running 3 SQL Server instances locally)
- **Visual Studio 2022** (Community is free) or **JetBrains Rider**
- **Postman** or **curl** for testing
- **grpcurl** (optional, for testing gRPC directly) — https://github.com/fullstorydev/grpcurl

### Verify Your Setup

Open a terminal and run:

```bash
dotnet --version
# Expected: 8.x.x

docker --version
# Expected: Docker version 24.x or later

docker compose version
# Expected: Docker Compose version v2.x
```

### NuGet Packages You Will Use

| Package | Purpose |
|---|---|
| `Grpc.AspNetCore` | Host a gRPC server in ASP.NET Core |
| `Google.Protobuf` | Protobuf serialization runtime |
| `Grpc.Net.Client` | Call gRPC services from a .NET client |
| `Grpc.Tools` | Build-time code generation from `.proto` files |
| `Microsoft.EntityFrameworkCore.SqlServer` | EF Core SQL Server driver |
| `Microsoft.EntityFrameworkCore.Tools` | EF Core CLI migrations |
| `Polly` | Retry and circuit breaker policies |

---

## 4. Phase 1 — Understanding gRPC

### Watch These First (before writing any code)

> These videos are short. Watching them before coding will save you hours of confusion.

**Essential — Watch in this order:**

1. **Tim Corey — Intro to gRPC in C#** (35 min)
   `https://www.youtube.com/watch?v=QyxCX2GYHxk`
   Covers: what gRPC is, what a .proto file looks like, running your first server/client. Highly practical.

2. **Nick Chapsas — gRPC Server & Unary Calls in .NET** (20 min)
   `https://www.youtube.com/watch?v=hp5FTB7PI9s`
   Covers: setting up an ASP.NET Core gRPC service, calling it from a .NET client. Very clean production-style code.

3. **Nick Chapsas — Server Streaming** (15 min)
   `https://www.youtube.com/watch?v=F2T6xNRoa1E`
   Covers: server streaming pattern. Useful context even if you don't implement it immediately.

4. **Microsoft Learn — gRPC Overview**
   `https://learn.microsoft.com/en-us/aspnet/core/grpc/`
   Official reference. Bookmark this and return to it as questions arise.

**Full gRPC .NET Playlist (Nick Chapsas — 5 videos, ~1.5 hrs total):**
`https://www.youtube.com/playlist?list=PLUOequmGnXxPOlhyA57ijmEyOeVmYQt32`

### Key Concepts to Confirm You Understand Before Proceeding

After watching, verify you can answer these:

- [ ] What does a `.proto` file define?
- [ ] What is the difference between a `message` and a `service` in Protobuf?
- [ ] What NuGet packages does a gRPC **server** project need vs. a gRPC **client** project?
- [ ] Where does the C# code (the `XxxBase` class and the `XxxClient` class) come from?
- [ ] What does `<Protobuf Include="..." GrpcServices="Server" />` in a `.csproj` do?

---

## 5. Phase 2 — Understanding Sharding

### Watch These First

1. **Arpit Bhayani — Database Sharding, explained visually** (15 min)
   `https://www.youtube.com/watch?v=hdxdhCpgYo8`
   The best visual explanation of consistent hashing and why naive modulo sharding has rebalancing problems.

2. **ByteByteGo — Horizontal Scaling vs Vertical Scaling** (8 min)
   `https://www.youtube.com/watch?v=xpDnVSmNFX0`
   Sets up the "why" before the "how".

3. **MSSQLTips — Database Sharding in SQL Server**
   `https://www.mssqltips.com/sqlservertip/7479/database-sharding-for-performance-and-maintenance/`
   SQL Server-specific. Read this article in full — it covers partitioned views and the trade-offs of SQL Server sharding specifically.

### Key Concepts to Confirm You Understand

- [ ] What is the difference between **partitioning** (within one database) and **sharding** (across multiple databases)?
- [ ] What is a **shard key** and why does choosing it poorly cause hot spots?
- [ ] Why can you not use a cross-shard `JOIN`?
- [ ] What does **fan-out query** mean?
- [ ] Why does adding a new shard require a resharding operation if you use `id % N`?

---

## 6. Phase 3 — Solution Structure & Docker Setup

### 6.1 Create the Solution

Open a terminal in a folder of your choice (e.g. `C:\Projects\` on Windows):

```bash
mkdir RegionShard
cd RegionShard

# Create the blank solution
dotnet new sln -n RegionShard

# Create each project
dotnet new webapi  -n ApiGateway         --no-openapi
dotnet new grpc    -n OrderService
dotnet new grpc    -n InventoryService
dotnet new classlib -n ShardRouter

# Add all projects to the solution
dotnet sln add ApiGateway/ApiGateway.csproj
dotnet sln add OrderService/OrderService.csproj
dotnet sln add InventoryService/InventoryService.csproj
dotnet sln add ShardRouter/ShardRouter.csproj
```

Your folder structure should now look like:

```
RegionShard/
├── RegionShard.sln
├── ApiGateway/
├── OrderService/
├── InventoryService/
└── ShardRouter/
```

### 6.2 Create a Shared Protos Folder

gRPC `.proto` files must be accessible to both the server project and the client project that calls it. The cleanest approach is a shared folder at the solution root that both projects reference.

```bash
mkdir Protos
```

You will put `order.proto` and `inventory.proto` here. Each service project will reference these files in its `.csproj`.

### 6.3 Docker Compose — Three SQL Server Instances

Create `docker-compose.yml` in the root `RegionShard/` folder:

```yaml
version: '3.9'

services:

  # Shard 0 — Asia-Pacific
  sqlserver-shard0:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: rs_shard0
    environment:
      SA_PASSWORD: "RegionShard@2024"
      ACCEPT_EULA: "Y"
    ports:
      - "1433:1433"
    volumes:
      - shard0_data:/var/opt/mssql

  # Shard 1 — Middle East
  sqlserver-shard1:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: rs_shard1
    environment:
      SA_PASSWORD: "RegionShard@2024"
      ACCEPT_EULA: "Y"
    ports:
      - "1434:1433"
    volumes:
      - shard1_data:/var/opt/mssql

  # Shard 2 — Americas
  sqlserver-shard2:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: rs_shard2
    environment:
      SA_PASSWORD: "RegionShard@2024"
      ACCEPT_EULA: "Y"
    ports:
      - "1435:1433"
    volumes:
      - shard2_data:/var/opt/mssql

volumes:
  shard0_data:
  shard1_data:
  shard2_data:
```

Start the databases:

```bash
docker compose up -d
```

Wait ~30 seconds for SQL Server to initialise, then verify all three are running:

```bash
docker ps
# You should see rs_shard0, rs_shard1, rs_shard2 all with status "Up"
```

> **Why three separate containers instead of one with three databases?**
> Real sharding means truly independent servers — each with its own CPU, memory, and I/O resources. Three containers on your local machine simulate this. In production these would be three separate Azure SQL instances or VMs.

### 6.4 Connection Strings

You will reference these in `appsettings.json` for both `OrderService` and `InventoryService`:

```json
{
  "ShardConnections": [
    "Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1434;Database=RegionShardDb_1;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1435;Database=RegionShardDb_2;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
  ]
}
```

---

## 7. Phase 4 — Proto Contracts

### 7.1 Understanding Protobuf Syntax

A `.proto` file has three sections:

```protobuf
syntax = "proto3";                    // Always proto3 for new projects

option csharp_namespace = "MyApp";    // Generated C# classes go in this namespace

package mypackage;                     // Prevents name collisions across services

// Message = a data structure (like a C# class with properties)
message MyRequest {
  string name = 1;     // Field number 1 — used in binary encoding, NEVER reuse old numbers
  int32  age  = 2;     // Field number 2
}

// Service = a set of RPC methods (like a C# interface)
service MyService {
  rpc DoSomething (MyRequest) returns (MyResponse);
}
```

**Field numbers are critical.** Once a `.proto` is in production, never change a field's number. It is what the binary encoding uses — changing it breaks existing clients silently.

**Scalar types mapping:**

| Protobuf Type | C# Type |
|---|---|
| `string` | `string` |
| `int32` | `int` |
| `int64` | `long` |
| `bool` | `bool` |
| `double` | `double` |
| `bytes` | `ByteString` |

### 7.2 Create `Protos/order.proto`

```protobuf
syntax = "proto3";

option csharp_namespace = "RegionShard.Contracts";

package order;

// ─── Place Order ───────────────────────────────────────────────
message PlaceOrderRequest {
  int32  region_id   = 1;   // Shard key — which region this order belongs to
  int32  product_id  = 2;
  int32  quantity    = 3;
  string customer_id = 4;
}

message PlaceOrderResponse {
  string order_id = 1;      // GUID assigned to the new order
  string status   = 2;      // "CONFIRMED" or "FAILED"
  string message  = 3;      // Human-readable detail
}

// ─── Get Order ─────────────────────────────────────────────────
message GetOrderRequest {
  string order_id  = 1;
  int32  region_id = 2;     // Needed to route to the correct shard
}

message GetOrderResponse {
  string order_id    = 1;
  int32  region_id   = 2;
  int32  product_id  = 3;
  int32  quantity    = 4;
  string customer_id = 5;
  string status      = 6;
  string created_at  = 7;
}

// ─── List Orders (for a region) ────────────────────────────────
message ListOrdersRequest {
  int32 region_id = 1;
}

message ListOrdersResponse {
  repeated GetOrderResponse orders = 1;  // "repeated" = List<T> in C#
}

// ─── Service Definition ────────────────────────────────────────
service OrderService {
  rpc PlaceOrder  (PlaceOrderRequest)  returns (PlaceOrderResponse);
  rpc GetOrder    (GetOrderRequest)    returns (GetOrderResponse);
  rpc ListOrders  (ListOrdersRequest)  returns (ListOrdersResponse);
}
```

### 7.3 Create `Protos/inventory.proto`

```protobuf
syntax = "proto3";

option csharp_namespace = "RegionShard.Contracts";

package inventory;

// ─── Check Inventory ───────────────────────────────────────────
message CheckInventoryRequest {
  int32 region_id  = 1;
  int32 product_id = 2;
  int32 quantity   = 3;   // How many units we want to reserve
}

message CheckInventoryResponse {
  bool  available       = 1;   // true if sufficient stock exists
  int32 available_stock = 2;   // how much is actually available
  string message        = 3;
}

// ─── Reserve Inventory (deduct stock) ─────────────────────────
message ReserveInventoryRequest {
  int32  region_id  = 1;
  int32  product_id = 2;
  int32  quantity   = 3;
  string order_id   = 4;   // For audit trail
}

message ReserveInventoryResponse {
  bool   success    = 1;
  string message    = 2;
}

// ─── Service Definition ────────────────────────────────────────
service InventoryService {
  rpc CheckInventory   (CheckInventoryRequest)   returns (CheckInventoryResponse);
  rpc ReserveInventory (ReserveInventoryRequest) returns (ReserveInventoryResponse);
}
```

### 7.4 Wire Proto Files Into Projects

**OrderService** acts as both a gRPC **server** (for `OrderService`) and a gRPC **client** (calling `InventoryService`).

Edit `OrderService/OrderService.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Grpc.AspNetCore" Version="2.62.0" />
  </ItemGroup>

  <ItemGroup>
    <!-- This service implements the OrderService proto (Server) -->
    <Protobuf Include="..\Protos\order.proto" GrpcServices="Server"
              ProtoRoot="..\Protos\" Link="Protos\order.proto" />

    <!-- This service calls InventoryService (Client) -->
    <Protobuf Include="..\Protos\inventory.proto" GrpcServices="Client"
              ProtoRoot="..\Protos\" Link="Protos\inventory.proto" />
  </ItemGroup>
</Project>
```

**InventoryService** is only a gRPC server.

Edit `InventoryService/InventoryService.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Grpc.AspNetCore" Version="2.62.0" />
  </ItemGroup>

  <ItemGroup>
    <Protobuf Include="..\Protos\inventory.proto" GrpcServices="Server"
              ProtoRoot="..\Protos\" Link="Protos\inventory.proto" />
  </ItemGroup>
</Project>
```

**ApiGateway** is only a gRPC client for both services.

Edit `ApiGateway/ApiGateway.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Grpc.Net.Client"  Version="2.62.0" />
    <PackageReference Include="Google.Protobuf"  Version="3.26.0" />
    <PackageReference Include="Grpc.Tools"       Version="2.62.0" PrivateAssets="All" />
  </ItemGroup>

  <ItemGroup>
    <Protobuf Include="..\Protos\order.proto"     GrpcServices="Client"
              ProtoRoot="..\Protos\" Link="Protos\order.proto" />
    <Protobuf Include="..\Protos\inventory.proto" GrpcServices="Client"
              ProtoRoot="..\Protos\" Link="Protos\inventory.proto" />
  </ItemGroup>
</Project>
```

> **How code generation works:** When you build, `Grpc.Tools` reads every `.proto` file listed under `<Protobuf>` and generates C# classes in `obj/Debug/net8.0/`. For `GrpcServices="Server"` it generates an abstract `XxxBase` class you inherit from. For `GrpcServices="Client"` it generates a concrete `XxxClient` class you instantiate. You never edit these generated files.

---

## 8. Phase 5 — The Shard Router Library

The `ShardRouter` class library contains all sharding logic. Both `OrderService` and `InventoryService` reference it.

### 8.1 Add NuGet Packages to ShardRouter

```bash
cd ShardRouter
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 8.0.0
dotnet add package Microsoft.EntityFrameworkCore.Design    --version 8.0.0
cd ..
```

### 8.2 The IShardRouter Interface

Create `ShardRouter/IShardRouter.cs`:

```csharp
namespace RegionShard.ShardRouter;

/// <summary>
/// Maps a region_id to the SQL Server connection string for that shard.
/// Both OrderService and InventoryService depend on this abstraction.
/// </summary>
public interface IShardRouter
{
    /// <summary>
    /// Returns the connection string for the shard that owns the given regionId.
    /// </summary>
    string GetConnectionString(int regionId);

    /// <summary>
    /// Returns the shard index (0, 1, or 2) for a region.
    /// Useful for logging and diagnostics.
    /// </summary>
    int GetShardIndex(int regionId);

    /// <summary>
    /// Returns all connection strings (used for fan-out queries across all shards).
    /// </summary>
    IReadOnlyList<string> GetAllConnectionStrings();
}
```

### 8.3 The HashShardRouter Implementation

Create `ShardRouter/HashShardRouter.cs`:

```csharp
namespace RegionShard.ShardRouter;

/// <summary>
/// Routes regions to shards using modulo hashing: shardIndex = regionId % shardCount.
///
/// Shard layout:
///   Shard 0 → regionId % 3 == 0  (Asia-Pacific)
///   Shard 1 → regionId % 3 == 1  (Middle East)
///   Shard 2 → regionId % 3 == 2  (Americas)
/// </summary>
public sealed class HashShardRouter : IShardRouter
{
    private readonly string[] _connectionStrings;

    public HashShardRouter(string[] connectionStrings)
    {
        if (connectionStrings == null || connectionStrings.Length == 0)
            throw new ArgumentException("At least one connection string is required.", nameof(connectionStrings));

        _connectionStrings = connectionStrings;
    }

    public string GetConnectionString(int regionId)
    {
        var index = GetShardIndex(regionId);
        return _connectionStrings[index];
    }

    public int GetShardIndex(int regionId)
    {
        // Math.Abs handles negative regionId values safely
        return Math.Abs(regionId) % _connectionStrings.Length;
    }

    public IReadOnlyList<string> GetAllConnectionStrings()
        => Array.AsReadOnly(_connectionStrings);
}
```

### 8.4 The ShardedDbContextFactory

This is the key abstraction that replaces a single `DbContext` injection. Instead of asking DI for "a DbContext", services ask for a factory and call `CreateForRegion(regionId)`.

Create `ShardRouter/ShardedDbContextFactory.cs`:

```csharp
using Microsoft.EntityFrameworkCore;

namespace RegionShard.ShardRouter;

/// <summary>
/// Creates DbContext instances connected to the correct shard for a given regionId.
/// This replaces the standard EF Core IDbContextFactory pattern for sharded databases.
/// </summary>
public sealed class ShardedDbContextFactory<TContext>
    where TContext : DbContext
{
    private readonly IShardRouter _shardRouter;
    private readonly Func<string, TContext> _contextFactory;

    /// <param name="shardRouter">The router that maps regionId to a connection string.</param>
    /// <param name="contextFactory">
    ///   A delegate that creates a TContext from a connection string.
    ///   Example: connStr => new OrderDbContext(new DbContextOptionsBuilder()
    ///                                              .UseSqlServer(connStr).Options)
    /// </param>
    public ShardedDbContextFactory(IShardRouter shardRouter, Func<string, TContext> contextFactory)
    {
        _shardRouter  = shardRouter;
        _contextFactory = contextFactory;
    }

    /// <summary>
    /// Creates a DbContext pointing at the shard that owns the given regionId.
    /// The caller is responsible for disposing the context (use with "using").
    /// </summary>
    public TContext CreateForRegion(int regionId)
    {
        var connectionString = _shardRouter.GetConnectionString(regionId);
        return _contextFactory(connectionString);
    }

    /// <summary>
    /// Creates DbContext instances for ALL shards.
    /// Use for fan-out queries where you need data from every shard.
    /// </summary>
    public IEnumerable<TContext> CreateForAllShards()
    {
        return _shardRouter.GetAllConnectionStrings()
                           .Select(cs => _contextFactory(cs));
    }
}
```

### 8.5 Add Projects as References

```bash
# OrderService uses ShardRouter
dotnet add OrderService/OrderService.csproj reference ShardRouter/ShardRouter.csproj

# InventoryService uses ShardRouter
dotnet add InventoryService/InventoryService.csproj reference ShardRouter/ShardRouter.csproj
```

---

## 9. Phase 6 — Order Service

### 9.1 EF Core Setup

Add NuGet packages:

```bash
cd OrderService
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 8.0.0
dotnet add package Microsoft.EntityFrameworkCore.Design    --version 8.0.0
cd ..
```

Create `OrderService/Data/Order.cs` (the EF Core entity):

```csharp
namespace OrderService.Data;

public class Order
{
    public Guid   OrderId    { get; set; } = Guid.NewGuid();
    public int    RegionId   { get; set; }   // Shard key
    public int    ProductId  { get; set; }
    public int    Quantity   { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Status     { get; set; } = "PENDING";   // CONFIRMED, FAILED
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
```

Create `OrderService/Data/OrderDbContext.cs`:

```csharp
using Microsoft.EntityFrameworkCore;

namespace OrderService.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) { }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(o => o.OrderId);

            entity.Property(o => o.OrderId)
                  .HasDefaultValueSql("NEWID()");

            // Index on RegionId — every query will filter by this
            entity.HasIndex(o => o.RegionId);

            entity.Property(o => o.CustomerId)
                  .HasMaxLength(100)
                  .IsRequired();

            entity.Property(o => o.Status)
                  .HasMaxLength(20)
                  .IsRequired();
        });
    }
}
```

### 9.2 Running EF Migrations Across All Three Shards

Because all three shards have identical schema, you only need one set of migrations. You will apply them to each shard separately.

Add a Design-time factory so the EF tools can create a context without a running app:

Create `OrderService/Data/OrderDbContextFactory.cs`:

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace OrderService.Data;

// This class is only used by "dotnet ef migrations add" — not at runtime
public class OrderDbContextDesignTimeFactory : IDesignTimeDbContextFactory<OrderDbContext>
{
    public OrderDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseSqlServer("Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;")
            .Options;
        return new OrderDbContext(options);
    }
}
```

Generate and apply migrations:

```bash
cd OrderService

# Generate migration files (only need to do this once)
dotnet ef migrations add InitialCreate

# Apply to Shard 0
dotnet ef database update --connection "Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"

# Apply to Shard 1
dotnet ef database update --connection "Server=localhost,1434;Database=RegionShardDb_1;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"

# Apply to Shard 2
dotnet ef database update --connection "Server=localhost,1435;Database=RegionShardDb_2;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"

cd ..
```

### 9.3 The gRPC Service Implementation

Create `OrderService/Services/OrderGrpcService.cs`:

```csharp
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using RegionShard.Contracts;    // Generated from order.proto
using RegionShard.ShardRouter;
using OrderService.Data;

namespace OrderService.Services;

/// <summary>
/// Implements the OrderService gRPC server.
/// Inherits from the auto-generated OrderService.OrderServiceBase.
/// </summary>
public class OrderGrpcService : RegionShard.Contracts.OrderService.OrderServiceBase
{
    private readonly ShardedDbContextFactory<OrderDbContext> _dbFactory;
    private readonly InventoryService.InventoryServiceClient _inventoryClient;
    private readonly ILogger<OrderGrpcService> _logger;

    public OrderGrpcService(
        ShardedDbContextFactory<OrderDbContext> dbFactory,
        InventoryService.InventoryServiceClient inventoryClient,
        ILogger<OrderGrpcService> logger)
    {
        _dbFactory       = dbFactory;
        _inventoryClient = inventoryClient;
        _logger          = logger;
    }

    // ─── PlaceOrder ──────────────────────────────────────────────────────────
    public override async Task<PlaceOrderResponse> PlaceOrder(
        PlaceOrderRequest request, ServerCallContext context)
    {
        _logger.LogInformation(
            "PlaceOrder: region={RegionId}, product={ProductId}, qty={Qty}",
            request.RegionId, request.ProductId, request.Quantity);

        // Step 1: Check inventory via gRPC call to InventoryService
        var inventoryResponse = await _inventoryClient.CheckInventoryAsync(
            new CheckInventoryRequest
            {
                RegionId  = request.RegionId,
                ProductId = request.ProductId,
                Quantity  = request.Quantity
            });

        if (!inventoryResponse.Available)
        {
            return new PlaceOrderResponse
            {
                Status  = "FAILED",
                Message = $"Insufficient stock: {inventoryResponse.AvailableStock} available, {request.Quantity} requested"
            };
        }

        // Step 2: Reserve inventory
        var reserveResponse = await _inventoryClient.ReserveInventoryAsync(
            new ReserveInventoryRequest
            {
                RegionId  = request.RegionId,
                ProductId = request.ProductId,
                Quantity  = request.Quantity,
                OrderId   = Guid.NewGuid().ToString() // Temp ID for the reserve call
            });

        if (!reserveResponse.Success)
        {
            return new PlaceOrderResponse
            {
                Status  = "FAILED",
                Message = $"Inventory reservation failed: {reserveResponse.Message}"
            };
        }

        // Step 3: Write the order to the correct shard
        var order = new Order
        {
            RegionId   = request.RegionId,
            ProductId  = request.ProductId,
            Quantity   = request.Quantity,
            CustomerId = request.CustomerId,
            Status     = "CONFIRMED"
        };

        // ShardedDbContextFactory routes to the correct SQL Server instance
        using var db = _dbFactory.CreateForRegion(request.RegionId);
        db.Orders.Add(order);
        await db.SaveChangesAsync(context.CancellationToken);

        _logger.LogInformation("Order {OrderId} created on shard for region {RegionId}",
            order.OrderId, request.RegionId);

        return new PlaceOrderResponse
        {
            OrderId = order.OrderId.ToString(),
            Status  = "CONFIRMED",
            Message = "Order placed successfully"
        };
    }

    // ─── GetOrder ────────────────────────────────────────────────────────────
    public override async Task<GetOrderResponse> GetOrder(
        GetOrderRequest request, ServerCallContext context)
    {
        // We need region_id to know which shard to query
        using var db = _dbFactory.CreateForRegion(request.RegionId);

        var order = await db.Orders
            .AsNoTracking()
            .FirstOrDefaultAsync(o => o.OrderId == Guid.Parse(request.OrderId),
                                 context.CancellationToken);

        if (order is null)
            throw new RpcException(new Status(StatusCode.NotFound,
                $"Order {request.OrderId} not found in region {request.RegionId}"));

        return MapToResponse(order);
    }

    // ─── ListOrders ──────────────────────────────────────────────────────────
    public override async Task<ListOrdersResponse> ListOrders(
        ListOrdersRequest request, ServerCallContext context)
    {
        // Single-shard query — only returns orders in the specified region
        using var db = _dbFactory.CreateForRegion(request.RegionId);

        var orders = await db.Orders
            .AsNoTracking()
            .Where(o => o.RegionId == request.RegionId)
            .OrderByDescending(o => o.CreatedAt)
            .Take(100)
            .ToListAsync(context.CancellationToken);

        var response = new ListOrdersResponse();
        response.Orders.AddRange(orders.Select(MapToResponse));
        return response;
    }

    private static GetOrderResponse MapToResponse(Order o) => new()
    {
        OrderId    = o.OrderId.ToString(),
        RegionId   = o.RegionId,
        ProductId  = o.ProductId,
        Quantity   = o.Quantity,
        CustomerId = o.CustomerId,
        Status     = o.Status,
        CreatedAt  = o.CreatedAt.ToString("o") // ISO 8601
    };
}
```

### 9.4 Wire Everything Up in Program.cs

Replace the content of `OrderService/Program.cs`:

```csharp
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Services;
using RegionShard.Contracts;
using RegionShard.ShardRouter;

var builder = WebApplication.CreateBuilder(args);

// ─── 1. gRPC Server ───────────────────────────────────────────────────────────
builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

// ─── 2. Shard Router ──────────────────────────────────────────────────────────
var connectionStrings = builder.Configuration
    .GetSection("ShardConnections")
    .Get<string[]>()!;

builder.Services.AddSingleton<IShardRouter>(
    new HashShardRouter(connectionStrings));

// ─── 3. Sharded DbContext Factory ────────────────────────────────────────────
builder.Services.AddSingleton(provider =>
{
    var router = provider.GetRequiredService<IShardRouter>();
    return new ShardedDbContextFactory<OrderDbContext>(
        router,
        connStr =>
        {
            var options = new DbContextOptionsBuilder<OrderDbContext>()
                .UseSqlServer(connStr)
                .Options;
            return new OrderDbContext(options);
        });
});

// ─── 4. gRPC Client for InventoryService ─────────────────────────────────────
builder.Services.AddGrpcClient<InventoryService.InventoryServiceClient>(options =>
{
    options.Address = new Uri(builder.Configuration["InventoryService:Url"]!);
});

var app = builder.Build();

app.MapGrpcService<OrderGrpcService>();

// Health check endpoint — useful for Docker health checks
app.MapGet("/health", () => "Order Service is running");

app.Run();
```

`OrderService/appsettings.json`:

```json
{
  "Kestrel": {
    "EndpointDefaults": {
      "Protocols": "Http2"
    }
  },
  "ShardConnections": [
    "Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1434;Database=RegionShardDb_1;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1435;Database=RegionShardDb_2;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
  ],
  "InventoryService": {
    "Url": "https://localhost:5002"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  }
}
```

---

## 10. Phase 7 — Inventory Service

### 10.1 EF Core Entity — Inventory

Add NuGet packages to InventoryService:

```bash
cd InventoryService
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 8.0.0
dotnet add package Microsoft.EntityFrameworkCore.Design    --version 8.0.0
cd ..
```

Create `InventoryService/Data/InventoryItem.cs`:

```csharp
namespace InventoryService.Data;

public class InventoryItem
{
    public int    Id        { get; set; }
    public int    RegionId  { get; set; }   // Shard key
    public int    ProductId { get; set; }
    public int    Stock     { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
```

Create `InventoryService/Data/InventoryDbContext.cs`:

```csharp
using Microsoft.EntityFrameworkCore;

namespace InventoryService.Data;

public class InventoryDbContext : DbContext
{
    public InventoryDbContext(DbContextOptions<InventoryDbContext> options) : base(options) { }

    public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InventoryItem>(entity =>
        {
            entity.HasKey(i => i.Id);

            // Composite index for the typical query pattern: WHERE RegionId=? AND ProductId=?
            entity.HasIndex(i => new { i.RegionId, i.ProductId }).IsUnique();
        });
    }
}
```

Run migrations (same approach as OrderService):

```bash
cd InventoryService
dotnet ef migrations add InitialCreate

dotnet ef database update --connection "Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
dotnet ef database update --connection "Server=localhost,1434;Database=RegionShardDb_1;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
dotnet ef database update --connection "Server=localhost,1435;Database=RegionShardDb_2;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
cd ..
```

### 10.2 Seed Data Script

Run this SQL on each shard after migrations to have some test inventory data. Connect with SQL Server Management Studio (SSMS) or Azure Data Studio to `localhost,1433`, `localhost,1434`, and `localhost,1435` with `sa` / `RegionShard@2024`.

```sql
-- Run on RegionShardDb_0 (Shard 0 — region_ids where id % 3 == 0: 3, 6, 9...)
INSERT INTO InventoryItems (RegionId, ProductId, Stock, UpdatedAt)
VALUES (3, 101, 500, GETUTCDATE()),
       (3, 102, 200, GETUTCDATE()),
       (6, 101, 150, GETUTCDATE()),
       (6, 103, 80,  GETUTCDATE());

-- Run on RegionShardDb_1 (Shard 1 — region_ids where id % 3 == 1: 1, 4, 7...)
INSERT INTO InventoryItems (RegionId, ProductId, Stock, UpdatedAt)
VALUES (1, 101, 300, GETUTCDATE()),
       (1, 102, 450, GETUTCDATE()),
       (4, 101, 100, GETUTCDATE());

-- Run on RegionShardDb_2 (Shard 2 — region_ids where id % 3 == 2: 2, 5, 8...)
INSERT INTO InventoryItems (RegionId, ProductId, Stock, UpdatedAt)
VALUES (2, 101, 700, GETUTCDATE()),
       (2, 102, 90,  GETUTCDATE()),
       (5, 103, 250, GETUTCDATE());
```

### 10.3 The gRPC Service — With Fan-Out Query

Create `InventoryService/Services/InventoryGrpcService.cs`:

```csharp
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using RegionShard.Contracts;
using RegionShard.ShardRouter;
using InventoryService.Data;

namespace InventoryService.Services;

public class InventoryGrpcService : RegionShard.Contracts.InventoryService.InventoryServiceBase
{
    private readonly ShardedDbContextFactory<InventoryDbContext> _dbFactory;
    private readonly ILogger<InventoryGrpcService> _logger;

    public InventoryGrpcService(
        ShardedDbContextFactory<InventoryDbContext> dbFactory,
        ILogger<InventoryGrpcService> logger)
    {
        _dbFactory = dbFactory;
        _logger    = logger;
    }

    // ─── CheckInventory ──────────────────────────────────────────────────────
    // Single-shard read — uses region_id to route to the correct database
    public override async Task<CheckInventoryResponse> CheckInventory(
        CheckInventoryRequest request, ServerCallContext context)
    {
        using var db = _dbFactory.CreateForRegion(request.RegionId);

        var item = await db.InventoryItems
            .AsNoTracking()
            .FirstOrDefaultAsync(
                i => i.RegionId == request.RegionId && i.ProductId == request.ProductId,
                context.CancellationToken);

        if (item is null)
        {
            return new CheckInventoryResponse
            {
                Available       = false,
                AvailableStock  = 0,
                Message         = $"No inventory record for product {request.ProductId} in region {request.RegionId}"
            };
        }

        var isAvailable = item.Stock >= request.Quantity;

        return new CheckInventoryResponse
        {
            Available      = isAvailable,
            AvailableStock = item.Stock,
            Message        = isAvailable
                ? $"Stock available: {item.Stock} units"
                : $"Insufficient stock: {item.Stock} available, {request.Quantity} requested"
        };
    }

    // ─── ReserveInventory ────────────────────────────────────────────────────
    // Deducts stock. Uses an optimistic concurrency pattern to avoid race conditions.
    public override async Task<ReserveInventoryResponse> ReserveInventory(
        ReserveInventoryRequest request, ServerCallContext context)
    {
        using var db = _dbFactory.CreateForRegion(request.RegionId);

        var item = await db.InventoryItems
            .FirstOrDefaultAsync(
                i => i.RegionId == request.RegionId && i.ProductId == request.ProductId,
                context.CancellationToken);

        if (item is null || item.Stock < request.Quantity)
        {
            return new ReserveInventoryResponse
            {
                Success = false,
                Message = "Insufficient stock at reservation time"
            };
        }

        item.Stock     -= request.Quantity;
        item.UpdatedAt  = DateTime.UtcNow;

        await db.SaveChangesAsync(context.CancellationToken);

        _logger.LogInformation(
            "Reserved {Qty} units of product {ProductId} in region {RegionId} for order {OrderId}. Remaining: {Stock}",
            request.Quantity, request.ProductId, request.RegionId, request.OrderId, item.Stock);

        return new ReserveInventoryResponse
        {
            Success = true,
            Message = $"Reserved {request.Quantity} units. Remaining stock: {item.Stock}"
        };
    }
}
```

> **IMPORTANT — Cross-Shard Query Example (Fan-Out):**
> If you ever need to query inventory across ALL regions simultaneously (e.g. "show me total stock for product 101 globally"), this is how you do it:
>
> ```csharp
> // Fan-out: query all shards in parallel, merge results in memory
> var contexts = _dbFactory.CreateForAllShards().ToList();
>
> var tasks = contexts.Select(db =>
>     db.InventoryItems
>       .AsNoTracking()
>       .Where(i => i.ProductId == productId)
>       .ToListAsync()
> ).ToList();
>
> var results = await Task.WhenAll(tasks);
> var merged  = results.SelectMany(r => r).ToList();
> int totalStock = merged.Sum(i => i.Stock);
>
> // Always dispose contexts when done
> foreach (var db in contexts) db.Dispose();
> ```
>
> This is called a **fan-out query**. Notice the pattern: run all shard queries in parallel with `Task.WhenAll`, then merge the result sets in application memory. You cannot `JOIN` across shards at the SQL level — that must happen in C#.

### 10.4 InventoryService Program.cs

```csharp
using InventoryService.Data;
using InventoryService.Services;
using Microsoft.EntityFrameworkCore;
using RegionShard.ShardRouter;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

var connectionStrings = builder.Configuration
    .GetSection("ShardConnections")
    .Get<string[]>()!;

builder.Services.AddSingleton<IShardRouter>(new HashShardRouter(connectionStrings));

builder.Services.AddSingleton(provider =>
{
    var router = provider.GetRequiredService<IShardRouter>();
    return new ShardedDbContextFactory<InventoryDbContext>(
        router,
        connStr =>
        {
            var options = new DbContextOptionsBuilder<InventoryDbContext>()
                .UseSqlServer(connStr)
                .Options;
            return new InventoryDbContext(options);
        });
});

var app = builder.Build();
app.MapGrpcService<InventoryGrpcService>();
app.MapGet("/health", () => "Inventory Service is running");
app.Run();
```

`InventoryService/appsettings.json`:

```json
{
  "Kestrel": {
    "EndpointDefaults": {
      "Protocols": "Http2"
    }
  },
  "ShardConnections": [
    "Server=localhost,1433;Database=RegionShardDb_0;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1434;Database=RegionShardDb_1;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;",
    "Server=localhost,1435;Database=RegionShardDb_2;User Id=sa;Password=RegionShard@2024;TrustServerCertificate=True;"
  ],
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  }
}
```

---

## 11. Phase 8 — API Gateway

The API Gateway exposes HTTP endpoints to the outside world and translates them into gRPC calls.

### 11.1 Controller

Create `ApiGateway/Controllers/OrdersController.cs`:

```csharp
using Grpc.Net.Client;
using Microsoft.AspNetCore.Mvc;
using RegionShard.Contracts;

namespace ApiGateway.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly OrderService.OrderServiceClient _orderClient;
    private readonly ILogger<OrdersController> _logger;

    public OrdersController(
        OrderService.OrderServiceClient orderClient,
        ILogger<OrdersController> logger)
    {
        _orderClient = orderClient;
        _logger      = logger;
    }

    // POST api/orders
    [HttpPost]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderDto dto)
    {
        var response = await _orderClient.PlaceOrderAsync(new PlaceOrderRequest
        {
            RegionId   = dto.RegionId,
            ProductId  = dto.ProductId,
            Quantity   = dto.Quantity,
            CustomerId = dto.CustomerId
        });

        if (response.Status == "CONFIRMED")
            return CreatedAtAction(nameof(GetOrder),
                new { orderId = response.OrderId, regionId = dto.RegionId },
                new { response.OrderId, response.Status, response.Message });

        return BadRequest(new { response.Status, response.Message });
    }

    // GET api/orders/{orderId}?regionId=1
    [HttpGet("{orderId}")]
    public async Task<IActionResult> GetOrder(string orderId, [FromQuery] int regionId)
    {
        try
        {
            var response = await _orderClient.GetOrderAsync(new GetOrderRequest
            {
                OrderId  = orderId,
                RegionId = regionId
            });
            return Ok(response);
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
        {
            return NotFound(new { message = ex.Status.Detail });
        }
    }

    // GET api/orders?regionId=1
    [HttpGet]
    public async Task<IActionResult> ListOrders([FromQuery] int regionId)
    {
        var response = await _orderClient.ListOrdersAsync(new ListOrdersRequest
        {
            RegionId = regionId
        });
        return Ok(response.Orders);
    }
}

// ─── DTOs ────────────────────────────────────────────────────────────────────
public record PlaceOrderDto(int RegionId, int ProductId, int Quantity, string CustomerId);
```

### 11.2 ApiGateway Program.cs

```csharp
using RegionShard.Contracts;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

// Register the gRPC client for OrderService
builder.Services.AddGrpcClient<OrderService.OrderServiceClient>(options =>
{
    options.Address = new Uri(builder.Configuration["OrderService:Url"]!);
});

var app = builder.Build();
app.MapControllers();
app.Run();
```

`ApiGateway/appsettings.json`:

```json
{
  "OrderService": {
    "Url": "https://localhost:5001"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  },
  "AllowedHosts": "*"
}
```

---

## 12. Phase 9 — Running & Testing the Full System

### 12.1 Set Launch Ports

Set explicit URLs in each service so they don't conflict.

`OrderService/Properties/launchSettings.json` (update the `https` profile):
```json
"https": {
  "commandName": "Project",
  "applicationUrl": "https://localhost:5001;http://localhost:5011",
  "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
}
```

`InventoryService/Properties/launchSettings.json`:
```json
"https": {
  "commandName": "Project",
  "applicationUrl": "https://localhost:5002;http://localhost:5012",
  "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
}
```

`ApiGateway/Properties/launchSettings.json`:
```json
"https": {
  "commandName": "Project",
  "applicationUrl": "https://localhost:5000;http://localhost:5010",
  "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
}
```

### 12.2 Start All Services

Open **three separate terminal windows** and run:

```bash
# Terminal 1
cd InventoryService && dotnet run

# Terminal 2
cd OrderService && dotnet run

# Terminal 3
cd ApiGateway && dotnet run
```

Wait until all three show `Now listening on: https://localhost:500X`.

### 12.3 Test With curl

**Place an order (Region 1 → Shard 1):**

```bash
curl -X POST https://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"regionId": 1, "productId": 101, "quantity": 5, "customerId": "CUST-001"}' \
  -k
```

Expected response:
```json
{
  "orderId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "CONFIRMED",
  "message": "Order placed successfully"
}
```

**Get the order back (must pass regionId so the router knows which shard):**

```bash
curl "https://localhost:5000/api/orders/3fa85f64-5717-4562-b3fc-2c963f66afa6?regionId=1" -k
```

**Test shard routing — place orders in different regions and verify they land on different SQL Server instances:**

```bash
# Region 3 → Shard 0 (3 % 3 == 0) — should write to localhost,1433
curl -X POST https://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"regionId": 3, "productId": 101, "quantity": 2, "customerId": "CUST-002"}' -k

# Region 2 → Shard 2 (2 % 3 == 2) — should write to localhost,1435
curl -X POST https://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"regionId": 2, "productId": 102, "quantity": 1, "customerId": "CUST-003"}' -k
```

After running these, connect to each SQL Server instance and run `SELECT * FROM Orders` to confirm the rows are on different shards.

**Test insufficient stock:**

```bash
# Try to order more than available (only 300 stock for region 1, product 101)
curl -X POST https://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"regionId": 1, "productId": 101, "quantity": 9999, "customerId": "CUST-999"}' -k
```

Expected: HTTP 400 with `"status": "FAILED"`.

### 12.4 Verify Shard Routing in SSMS

Connect SSMS to all three instances and run on each:

```sql
-- On RegionShardDb_0 (localhost,1433): should contain region_id = 3, 6, 9...
SELECT OrderId, RegionId, ProductId, Status, CreatedAt FROM Orders ORDER BY CreatedAt DESC;

-- On RegionShardDb_1 (localhost,1434): should contain region_id = 1, 4, 7...
SELECT OrderId, RegionId, ProductId, Status, CreatedAt FROM Orders ORDER BY CreatedAt DESC;

-- On RegionShardDb_2 (localhost,1435): should contain region_id = 2, 5, 8...
SELECT OrderId, RegionId, ProductId, Status, CreatedAt FROM Orders ORDER BY CreatedAt DESC;
```

This visual confirmation is the most satisfying moment of the project — you can see the data physically distributed across independent databases.

---

## 13. Phase 10 — Resilience with Polly

### 13.1 Why This Matters

In production, a shard can become temporarily unavailable — network blip, SQL Server restart, overload. Without resilience, a single shard failure cascades to 503 errors for all users in that region. Polly adds retry logic and a circuit breaker.

Add Polly to OrderService and InventoryService:

```bash
dotnet add OrderService/OrderService.csproj package Microsoft.Extensions.Http.Polly --version 8.0.0
dotnet add InventoryService/InventoryService.csproj package Microsoft.Extensions.Http.Polly --version 8.0.0
```

### 13.2 Retry Policy on the ShardedDbContextFactory

Wrap the `SaveChangesAsync` call with a Polly retry policy for transient SQL errors:

```csharp
using Polly;
using Polly.Retry;

// Add this as a helper in ShardRouter or directly in the service
var retryPolicy = Policy
    .Handle<Microsoft.Data.SqlClient.SqlException>(ex =>
        ex.Number == 1205 ||   // Deadlock
        ex.Number == -2 ||     // Timeout
        ex.Number == 233)      // Connection closed
    .WaitAndRetryAsync(
        retryCount: 3,
        sleepDurationProvider: attempt => TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt)),
        onRetry: (exception, timespan, attempt, context) =>
        {
            Console.WriteLine($"Retry {attempt} after {timespan.TotalMs}ms due to: {exception.Message}");
        });

// Usage
await retryPolicy.ExecuteAsync(async () =>
{
    using var db = _dbFactory.CreateForRegion(request.RegionId);
    db.Orders.Add(order);
    await db.SaveChangesAsync();
});
```

### 13.3 Circuit Breaker on gRPC Client

In `OrderService/Program.cs`, configure the gRPC client with a circuit breaker so that if `InventoryService` fails repeatedly, calls fail fast instead of waiting for timeouts:

```csharp
builder.Services
    .AddGrpcClient<InventoryService.InventoryServiceClient>(options =>
    {
        options.Address = new Uri(builder.Configuration["InventoryService:Url"]!);
    })
    .AddTransientHttpErrorPolicy(policy =>
        policy.WaitAndRetryAsync(3, retryAttempt =>
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))))
    .AddTransientHttpErrorPolicy(policy =>
        policy.CircuitBreakerAsync(
            handledEventsAllowedBeforeBreaking: 5,
            durationOfBreak: TimeSpan.FromSeconds(30)));
```

---

## 14. Common Errors & How to Fix Them

### Error: `RpcException: Status(StatusCode="Unavailable")`

**Cause:** The gRPC server is not running, or the URL in `appsettings.json` is wrong.
**Fix:** Confirm all three services are running. Check that `InventoryService:Url` in OrderService's config matches the port InventoryService is actually listening on.

### Error: `Cannot use HTTP/2 on plaintext (http://) in .NET`

**Cause:** gRPC requires HTTP/2. In development, .NET enforces TLS by default.
**Fix 1 (recommended):** Use `https://` URLs and trust the dev certificate (`dotnet dev-certs https --trust`).
**Fix 2 (for local only):** Add `AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);` at the top of `Program.cs` and use `http://` URLs.

### Error: `SqlException: Cannot open database "RegionShardDb_0"`

**Cause:** Migrations have not been applied, or the Docker containers are not running.
**Fix:** Run `docker compose up -d` and wait 30 seconds. Then re-run `dotnet ef database update` for each shard.

### Error: Generated gRPC types not found (compilation errors after editing .proto)

**Cause:** The build cache is stale.
**Fix:** Run `dotnet build --no-incremental` or delete the `obj/` folder and rebuild.

### Error: `Grpc.Core.RpcException: NotFound` on `GetOrder`

**Cause:** You queried the wrong shard — you passed the wrong `regionId` when fetching.
**Remember:** In a sharded system, the region_id is not optional metadata — it is the routing key. You must always pass it when fetching by ID, because the order only exists on one specific shard.

### Error: EF Core trying to run migrations on the wrong database

**Cause:** The design-time factory always uses Shard 0. This is intentional.
**Fix:** Generate migrations once, then apply to each shard separately using `--connection` as shown in the guide.

---

## 15. Further Learning Roadmap

### Immediate Next Steps (After This Project Works)

1. **Add OpenTelemetry tracing** — see how a single `PlaceOrder` HTTP request creates a trace span in the gateway, a child span in OrderService, and a grandchild span in InventoryService.
   - Package: `OpenTelemetry.Extensions.Hosting`, `OpenTelemetry.Instrumentation.GrpcNetClient`
   - Tutorial: https://learn.microsoft.com/en-us/dotnet/core/diagnostics/distributed-tracing-instrumentation-walkthroughs

2. **Add the Outbox Pattern** — currently, if the order is written to the DB but the gRPC call to Inventory fails halfway, you have an inconsistency. The outbox pattern solves this.
   - Article: https://www.milanjovanovic.tech/blog/outbox-pattern-for-reliable-microservices-messaging

3. **Replace modulo routing with consistent hashing** — modulo (`%`) means adding a 4th shard requires resharding ~75% of your data. Consistent hashing reduces this to ~25%.
   - Video: https://www.youtube.com/watch?v=hdxdhCpgYo8 (Arpit Bhayani)

### Deeper gRPC Learning

- **gRPC streaming in depth** — Nick Chapsas full playlist: https://www.youtube.com/playlist?list=PLUOequmGnXxPOlhyA57ijmEyOeVmYQt32
- **Official Microsoft gRPC documentation**: https://learn.microsoft.com/en-us/aspnet/core/grpc/
- **gRPC vs REST benchmark and decision guide**: https://learn.microsoft.com/en-us/aspnet/core/grpc/comparison

### Deeper Sharding Learning

- **Sharding Pattern (Azure Architecture Center)**: https://learn.microsoft.com/en-us/azure/architecture/patterns/sharding
- **The Complete Guide to Database Sharding in .NET**: https://developersvoice.com/blog/database/dotnet-database-sharding-guide-sql-postgres-mongo/
- **SQL Server Sharding article (MSSQLTips)**: https://www.mssqltips.com/sqlservertip/7479/database-sharding-for-performance-and-maintenance/

### Production Topics to Study After This

| Topic | Why It Matters |
|---|---|
| Global distributed IDs (Snowflake IDs) | Auto-increment IDs don't work across shards — each shard would generate ID 1, 2, 3... independently |
| Cross-shard saga pattern | Distributed transactions without 2PC |
| Read replicas per shard | Read-heavy workloads |
| Shard rebalancing | What happens when you need a 4th shard |
| Azure SQL Elastic Pools | Managed sharding in the cloud |

---

> **A final note:** The most common mistake when learning sharding is over-applying it. Sharding adds significant operational complexity. A single well-tuned SQL Server instance with proper indexing handles hundreds of millions of rows and thousands of requests per second. Only introduce sharding when you have concrete, measured evidence that a single instance is the bottleneck. This project gives you the skill — use it judiciously.
